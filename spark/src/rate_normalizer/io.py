"""Input/output for the normalizer: manifest gate, raw read with explicit
schema, reference tables, and Parquet/Delta writers (D-14, D-27, D-31).

Only the DataFrame API is used (D-26) so the same code runs on local Spark
and on Databricks serverless (Spark Connect)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import schema

MANIFEST = "_MANIFEST.json"


class IncompleteRunError(RuntimeError):
    """The run directory has no manifest: it was cancelled or is still being written."""


@dataclass(frozen=True)
class Manifest:
    run_id: str
    started_at: str
    finished_at: str
    workers: int
    as_of: str
    sources_ok: int
    sources_failed: int
    records: int
    raw: dict


def read_manifest(run_dir: str) -> Manifest:
    """Fail fast when the run is incomplete — Spark must never normalize a
    partial landing (D-14). Plain file IO works for local paths and for
    Databricks Volumes (FUSE-mounted)."""
    p = Path(run_dir) / MANIFEST
    if not p.exists():
        raise IncompleteRunError(f"{run_dir} has no {MANIFEST}; refusing to normalize an incomplete run")
    raw = json.loads(p.read_text(encoding="utf-8"))
    return Manifest(
        run_id=raw["run_id"],
        started_at=raw["started_at"],
        finished_at=raw["finished_at"],
        workers=raw["workers"],
        as_of=raw.get("as_of", ""),
        sources_ok=raw["totals"]["sources_ok"],
        sources_failed=raw["totals"]["sources_failed"],
        records=raw["totals"]["records"],
        raw=raw,
    )


def read_raw(spark: SparkSession, run_dir: str) -> DataFrame:
    """Read every part of every source under the run directory."""
    # Absolute POSIX glob: Hadoop resolves relative Windows paths against the wrong root.
    pattern = Path(run_dir).resolve().as_posix() + "/source=*/part-*.jsonl.gz"
    return (
        spark.read.schema(schema.RAW)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", schema.CORRUPT_COL)
        .json(pattern)
    )


@dataclass(frozen=True)
class Reference:
    ports: DataFrame  # key (upper alias) → locode
    containers: DataFrame  # alias → container_type
    fx: DataFrame  # currency → usd_per_unit
    teu: DataFrame  # container_type → teu factor
    priority: DataFrame  # source_id → priority
    rules: list[str]  # enabled rule names in order


def _csv(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.option("header", True).csv(path)


def load_reference(spark: SparkSession, config_dir: str) -> Reference:
    """Load the committed lookup tables. `ports.csv` is exploded so that the
    LOCODE, the city and every alias all resolve to the LOCODE."""
    cfg = Path(config_dir)
    ports = _csv(spark, str(cfg / "ports.csv"))
    keys = ports.select(
        F.col("locode"),
        F.explode(
            F.array_union(
                F.array(F.col("locode"), F.col("city")),
                F.split(F.coalesce(F.col("aliases"), F.lit("")), r"\|"),
            )
        ).alias("key"),
    ).where(F.col("key") != "")
    port_keys = keys.select(F.upper(F.trim("key")).alias("key"), "locode").dropDuplicates(["key"])

    containers = _csv(spark, str(cfg / "container_aliases.csv")).select(
        F.upper(F.trim("alias")).alias("alias"), "container_type"
    )
    fx = _csv(spark, str(cfg / "fx_rates.csv")).select(
        F.upper("currency").alias("currency"), F.col("usd_per_unit").cast("double").alias("usd_per_unit")
    )
    teu = _csv(spark, str(cfg / "teu_factors.csv")).select(
        "container_type", F.col("teu").cast("int").alias("teu")
    )
    priority = _csv(spark, str(cfg / "source_priority.csv")).select(
        "source_id", F.col("priority").cast("int").alias("priority")
    )
    rules = load_rule_names(str(cfg / "rules.yaml"))
    return Reference(port_keys, containers, fx, teu, priority, rules)


def load_rule_names(path: str) -> list[str]:
    import yaml

    doc = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    return [r["name"] for r in doc["rules"] if r.get("enabled", True)]


def write_parquet(df: DataFrame, path: str, partition_by: str | None = None) -> None:
    w = df.write.mode("overwrite")
    if partition_by:
        w = w.partitionBy(partition_by)
    w.parquet(path)


def write_delta_merge(spark: SparkSession, df: DataFrame, table: str, keys: list[str]) -> None:
    """Idempotent Delta write: MERGE on the natural key so re-running a run
    is a no-op (D-31). Used on Databricks; locally `--format parquet` is the
    default because Delta needs the connector."""
    df.createOrReplaceTempView("_incoming")
    cols = df.columns
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} USING DELTA AS SELECT * FROM _incoming WHERE 1 = 0")
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    update = ", ".join(f"t.{c} = s.{c}" for c in cols)
    insert_cols = ", ".join(cols)
    insert_vals = ", ".join(f"s.{c}" for c in cols)
    spark.sql(
        f"MERGE INTO {table} t USING _incoming s ON {on} "
        f"WHEN MATCHED THEN UPDATE SET {update} "
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
    )


def write_json(obj: dict, path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(obj, indent=2, sort_keys=True), encoding="utf-8")
