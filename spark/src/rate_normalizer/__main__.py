"""CLI: python -m rate_normalizer --input RUN_DIR --output OUT_DIR [--config DIR] [--as-of DATE]
[--format parquet|delta] [--master local[2]]"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from . import job, session


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="rate-normalizer", description=__doc__)
    p.add_argument("--input", required=True, help="run directory containing _MANIFEST.json and source=*/")
    p.add_argument(
        "--output", required=True, help="output directory (rate_matrix/, rejects/, run_metrics.json)"
    )
    p.add_argument(
        "--config", default=str(Path(__file__).resolve().parents[2] / "config"), help="reference tables"
    )
    p.add_argument("--as-of", default="", help="as-of date for expired_as_of; defaults to the manifest's")
    p.add_argument("--format", choices=["parquet", "delta"], default="parquet")
    p.add_argument("--delta-schema", default="workspace.rates")
    p.add_argument("--master", default=None, help="e.g. local[2]; omit on Databricks")
    args = p.parse_args(argv)

    spark = session.build(args.master)
    try:
        result = job.run(
            spark, args.input, args.output, args.config, args.as_of, args.format, args.delta_schema
        )
    finally:
        if args.master:
            spark.stop()
    print(json.dumps(result.as_dict(), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
