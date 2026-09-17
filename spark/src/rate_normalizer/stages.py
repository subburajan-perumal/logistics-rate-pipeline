"""Transform stages (docs/PLAN.md §10.2). Every stage is a pure function
DataFrame -> DataFrame built from built-in functions and joins — no Python
UDFs, no RDDs (D-26). Stages only *annotate* rows; `rules.validate` decides
which rows are rejected, so every drop has a name."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .io import Reference

DATE_FORMATS = ["yyyy-MM-dd", "dd/MM/yyyy", "dd-MMM-yyyy", "yyyy/MM/dd"]

# Matches European thousands/decimal style: 1.085,50 / 120,00
_EU_NUMBER = r"^-?\d{1,3}(\.\d{3})*(,\d+)?$|^-?\d+,\d+$"


def _parse_price(col: str):
    """Coalesce Go's typed price with a Spark re-parse of the raw string, so
    a source whose recipe hint was wrong still yields a number when the text
    is unambiguous."""
    raw = F.trim(F.col(col))
    eu = F.regexp_replace(F.regexp_replace(raw, r"\.", ""), ",", ".")
    us = F.regexp_replace(F.regexp_replace(raw, ",", ""), r"^\$", "")
    reparsed = F.when(raw.rlike(_EU_NUMBER), eu).otherwise(us).try_cast("double")
    return F.coalesce(F.col("price"), reparsed)


def _parse_date(col: str):
    raw = F.trim(F.col(col))
    candidates = [F.try_to_timestamp(raw, F.lit(fmt)).cast("date") for fmt in DATE_FORMATS]
    return F.coalesce(*candidates)


def parse_types(df: DataFrame) -> DataFrame:
    """Stage 1 — typed price and dates, whitespace/case-normalized keys."""
    return (
        df.withColumn("price_parsed", _parse_price("price_raw"))
        .withColumn("valid_from", _parse_date("valid_from_raw"))
        .withColumn("valid_to", _parse_date("valid_to_raw"))
        .withColumn("origin_key", F.upper(F.trim("origin_raw")))
        .withColumn("destination_key", F.upper(F.trim("destination_raw")))
        .withColumn("container_key", F.upper(F.trim("container_type_raw")))
        .withColumn("currency_key", F.upper(F.trim("currency")))
    )


def normalize_ports(df: DataFrame, ref: Reference) -> DataFrame:
    """Stage 2 — LOCODE / city / alias → LOCODE via a broadcast join
    (the lookup is 60 rows; a shuffle join would be the wrong plan)."""
    o = F.broadcast(
        ref.ports.withColumnRenamed("locode", "origin_locode").withColumnRenamed("key", "origin_key")
    )
    d = F.broadcast(
        ref.ports.withColumnRenamed("locode", "destination_locode").withColumnRenamed(
            "key", "destination_key"
        )
    )
    return df.join(o, "origin_key", "left").join(d, "destination_key", "left")


def normalize_container(df: DataFrame, ref: Reference) -> DataFrame:
    """Stage 3 — equipment aliases (20GP, 40HQ, 40', …) → canonical type."""
    c = F.broadcast(ref.containers.withColumnRenamed("alias", "container_key"))
    return df.join(c, "container_key", "left")


def normalize_units(df: DataFrame, ref: Reference) -> DataFrame:
    """Stage 4 — per-TEU quotes become per-container: 20' × 1, 40' × 2.
    A per-TEU row whose type has no TEU factor is flagged (unit_unsupported)."""
    t = F.broadcast(ref.teu)
    df = df.join(t, "container_type", "left")
    factor = F.when(F.col("unit") == "per_teu", F.col("teu")).otherwise(F.lit(1))
    return df.withColumn("unit_factor", factor).withColumn(
        "base_price", F.round(F.col("price_parsed") * F.col("unit_factor"), 2)
    )


def normalize_currency(df: DataFrame, ref: Reference) -> DataFrame:
    """Stage 5 — static FX table (D-29) → USD; surcharges summed with
    `aggregate` over the array (no UDF)."""
    fx = F.broadcast(ref.fx.withColumnRenamed("currency", "currency_key"))
    df = df.join(fx, "currency_key", "left")
    surcharge_sum = F.aggregate(
        F.coalesce(F.col("surcharges"), F.array()),
        F.lit(0.0),
        lambda acc, s: acc + F.coalesce(s["amount"], F.lit(0.0)),
    )
    return (
        df.withColumn("price_usd", F.round(F.col("base_price") * F.col("usd_per_unit"), 2))
        .withColumn("surcharges_usd", F.round(surcharge_sum * F.col("usd_per_unit"), 2))
        .withColumn("all_in_usd", F.round(F.col("price_usd") + F.col("surcharges_usd"), 2))
    )


DEDUP_KEY = ["carrier", "origin_locode", "destination_locode", "container_type", "valid_from"]


def dedup(df: DataFrame, ref: Reference) -> DataFrame:
    """Stage 7 — one row per lane/type/validity: highest source priority,
    then most recently fetched, then record_hash as a total tie-break so
    the result is deterministic (D-30). Returns the survivors plus a
    `dedup_removed` count via `.count()` at the caller."""
    p = F.broadcast(ref.priority)
    df = df.join(p, "source_id", "left").withColumn("priority", F.coalesce(F.col("priority"), F.lit(999)))
    w = Window.partitionBy(*DEDUP_KEY).orderBy(
        F.col("priority").asc(), F.col("fetched_at").desc(), F.col("record_hash").asc()
    )
    return df.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn")


def finalize(df: DataFrame) -> DataFrame:
    """Stage 8 — output columns (§10.4)."""
    return (
        df.withColumn("lane_id", F.concat_ws("-", "origin_locode", "destination_locode"))
        .withColumn("base_currency", F.col("currency_key"))
        .withColumn("unit_source", F.col("unit"))
        .withColumn("normalized_at", F.current_timestamp())
    )
