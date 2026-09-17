"""The end-to-end normalization job: manifest gate → read → stages → rules
→ dedup → outputs + run_metrics.json (docs/PLAN.md §10)."""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import io, rules, schema, stages


@dataclass
class Result:
    run_id: str
    rows_in: int
    rows_out: int
    rejects_by_reason: dict[str, int]
    dedup_removed: int
    sources_failed: int
    stage_durations_ms: dict[str, int] = field(default_factory=dict)
    spark_version: str = ""
    as_of: str = ""

    def as_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "rows_in": self.rows_in,
            "rows_out": self.rows_out,
            "rejects_total": sum(self.rejects_by_reason.values()),
            "rejects_by_reason": dict(sorted(self.rejects_by_reason.items())),
            "dedup_removed": self.dedup_removed,
            "sources_failed": self.sources_failed,
            "stage_durations_ms": self.stage_durations_ms,
            "spark_version": self.spark_version,
            "as_of": self.as_of,
        }


class Timer:
    def __init__(self) -> None:
        self.laps: dict[str, int] = {}
        self._t = time.perf_counter()

    def lap(self, name: str) -> None:
        now = time.perf_counter()
        self.laps[name] = int((now - self._t) * 1000)
        self._t = now


def transform(raw: DataFrame, ref: io.Reference, as_of: str) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Pure pipeline: returns (rate_matrix, rejects, deduped_candidates_before_dedup)
    so callers can count dedup_removed without recomputing."""
    df = stages.parse_types(raw)
    df = stages.normalize_ports(df, ref)
    df = stages.normalize_container(df, ref)
    df = stages.normalize_units(df, ref)
    df = stages.normalize_currency(df, ref)
    accepted, rejected = rules.validate(df, ref.rules, as_of)
    deduped = stages.dedup(accepted, ref)
    matrix = stages.finalize(deduped).select(*schema.RATE_MATRIX_COLUMNS)
    rej = rejected.select(*schema.REJECT_COLUMNS)
    return matrix, rej, accepted


def run(
    spark: SparkSession,
    input_dir: str,
    output_dir: str,
    config_dir: str,
    as_of: str,
    fmt: str = "parquet",
    delta_schema: str = "workspace.rates",
) -> Result:
    timer = Timer()
    manifest = io.read_manifest(input_dir)
    if not as_of:
        as_of = manifest.as_of
    ref = io.load_reference(spark, config_dir)
    raw = io.read_raw(spark, input_dir).cache()
    rows_in = raw.count()
    timer.lap("read")

    matrix, rej, accepted = transform(raw, ref, as_of)
    accepted = accepted.cache()
    accepted_n = accepted.count()
    timer.lap("transform")

    reject_counts = {
        r["reject_reason"]: r["n"]
        for r in rej.groupBy("reject_reason").agg(F.count("*").alias("n")).collect()
    }
    timer.lap("validate")

    matrix = matrix.cache()
    rows_out = matrix.count()
    dedup_removed = accepted_n - rows_out
    timer.lap("dedup")

    if fmt == "delta":
        io.write_delta_merge(spark, matrix, f"{delta_schema}.rate_matrix", ["run_id", "record_hash"])
        io.write_delta_merge(
            spark, rej, f"{delta_schema}.rejects", ["run_id", "record_hash", "reject_reason"]
        )
    io.write_parquet(matrix.coalesce(1), os.path.join(output_dir, "rate_matrix"), partition_by="carrier")
    io.write_parquet(rej.coalesce(1), os.path.join(output_dir, "rejects"))
    timer.lap("write")

    result = Result(
        run_id=manifest.run_id,
        rows_in=rows_in,
        rows_out=rows_out,
        rejects_by_reason=reject_counts,
        dedup_removed=dedup_removed,
        sources_failed=manifest.sources_failed,
        stage_durations_ms=timer.laps,
        spark_version=spark.version,
        as_of=as_of,
    )
    io.write_json(result.as_dict(), os.path.join(output_dir, "run_metrics.json"))
    raw.unpersist()
    accepted.unpersist()
    matrix.unpersist()
    return result
