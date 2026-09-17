"""SparkSession construction. On Databricks the existing session is reused;
locally we pin the driver to localhost (Windows hostnames with underscores
are invalid Spark URLs) and keep memory small (docs/PLAN.md D-27)."""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession


def build(master: str | None = None, app_name: str = "rate-normalizer") -> SparkSession:
    b = SparkSession.builder.appName(app_name)
    if master:
        # Python workers (only needed for createDataFrame from local rows in
        # tests) must run the same interpreter; Windows has no `python3`.
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
        b = (
            b.master(master)
            .config("spark.driver.memory", "1g")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.driver.host", "localhost")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.ui.enabled", "false")
        )
    return b.getOrCreate()
