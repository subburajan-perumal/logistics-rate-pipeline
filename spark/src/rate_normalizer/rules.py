"""Named reject rules (docs/PLAN.md §10.3, D-28). Each rule is a function
returning a boolean Column that is TRUE when the row must be rejected. The
order and the enabled set come from `config/rules.yaml`; a row carries
only its first matching reason, so reject counts sum to the reject total.

Adding a rule = one function here + one line in rules.yaml."""

from __future__ import annotations

from collections.abc import Callable

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .schema import CORRUPT_COL

Rule = Callable[[str], Column]  # as_of -> condition


def corrupt_json(as_of: str) -> Column:
    return F.col(CORRUPT_COL).isNotNull()


def bad_price(as_of: str) -> Column:
    return F.col("price_parsed").isNull() | (F.col("price_parsed") <= 0)


def unknown_port(as_of: str) -> Column:
    return F.col("origin_locode").isNull() | F.col("destination_locode").isNull()


def same_port(as_of: str) -> Column:
    return F.col("origin_locode") == F.col("destination_locode")


def unknown_container(as_of: str) -> Column:
    return F.col("container_type").isNull()


def unit_unsupported(as_of: str) -> Column:
    return (F.col("unit") == "per_teu") & F.col("teu").isNull()


def unknown_currency(as_of: str) -> Column:
    return F.col("usd_per_unit").isNull()


def bad_dates(as_of: str) -> Column:
    return F.col("valid_from").isNull() | F.col("valid_to").isNull()


def dates_reversed(as_of: str) -> Column:
    return F.col("valid_from") > F.col("valid_to")


def expired_as_of(as_of: str) -> Column:
    d = F.to_date(F.lit(as_of))
    return (F.col("valid_to") < d) | (F.col("valid_from") > d)


REGISTRY: dict[str, Rule] = {
    "corrupt_json": corrupt_json,
    "bad_price": bad_price,
    "unknown_port": unknown_port,
    "same_port": same_port,
    "unknown_container": unknown_container,
    "unit_unsupported": unit_unsupported,
    "unknown_currency": unknown_currency,
    "bad_dates": bad_dates,
    "dates_reversed": dates_reversed,
    "expired_as_of": expired_as_of,
}


def reason_column(rule_names: list[str], as_of: str) -> Column:
    """First-match CASE over the enabled rules in configured order."""
    unknown = [n for n in rule_names if n not in REGISTRY]
    if unknown:
        raise ValueError(f"rules.yaml names unknown rule(s): {unknown}")
    expr: Column | None = None
    for name in rule_names:
        cond = REGISTRY[name](as_of)
        expr = F.when(cond, F.lit(name)) if expr is None else expr.when(cond, F.lit(name))
    if expr is None:
        return F.lit(None).cast("string")
    return expr.otherwise(F.lit(None))


def validate(df: DataFrame, rule_names: list[str], as_of: str) -> tuple[DataFrame, DataFrame]:
    """Stage 6 — split into (accepted, rejected-with-reason)."""
    annotated = df.withColumn("reject_reason", reason_column(rule_names, as_of))
    accepted = annotated.where(F.col("reject_reason").isNull()).drop("reject_reason")
    rejected = annotated.where(F.col("reject_reason").isNotNull())
    return accepted, rejected
