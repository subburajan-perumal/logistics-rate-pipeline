"""Explicit schemas for the raw-record contract (docs/PLAN.md §7) and the
normalized outputs (§10.4). Reading with an explicit schema is what makes
`PERMISSIVE` mode route malformed lines into `_corrupt` instead of failing
the job or silently inferring the wrong types."""

from pyspark.sql import types as T

CORRUPT_COL = "_corrupt"

SURCHARGE = T.StructType(
    [
        T.StructField("name", T.StringType()),
        T.StructField("amount_raw", T.StringType()),
        T.StructField("amount", T.DoubleType()),
    ]
)

RAW = T.StructType(
    [
        T.StructField("schema_version", T.IntegerType()),
        T.StructField("run_id", T.StringType()),
        T.StructField("source_id", T.StringType()),
        T.StructField("carrier", T.StringType()),
        T.StructField("fetched_at", T.TimestampType()),
        T.StructField("page", T.IntegerType()),
        T.StructField("row_index", T.IntegerType()),
        T.StructField("origin_raw", T.StringType()),
        T.StructField("destination_raw", T.StringType()),
        T.StructField("container_type_raw", T.StringType()),
        T.StructField("price_raw", T.StringType()),
        T.StructField("price", T.DoubleType()),
        T.StructField("currency", T.StringType()),
        T.StructField("unit", T.StringType()),
        T.StructField("valid_from_raw", T.StringType()),
        T.StructField("valid_to_raw", T.StringType()),
        T.StructField("surcharges", T.ArrayType(SURCHARGE)),
        T.StructField("source_ref", T.StringType()),
        T.StructField("record_hash", T.StringType()),
        T.StructField(CORRUPT_COL, T.StringType()),
    ]
)

# Column order of the normalized rate matrix.
RATE_MATRIX_COLUMNS = [
    "run_id",
    "carrier",
    "lane_id",
    "origin_locode",
    "destination_locode",
    "container_type",
    "price_usd",
    "base_price",
    "base_currency",
    "surcharges_usd",
    "all_in_usd",
    "unit_source",
    "valid_from",
    "valid_to",
    "source_id",
    "fetched_at",
    "record_hash",
    "normalized_at",
]

REJECT_COLUMNS = [
    "run_id",
    "source_id",
    "carrier",
    "reject_reason",
    "origin_raw",
    "destination_raw",
    "container_type_raw",
    "price_raw",
    "currency",
    "unit",
    "valid_from_raw",
    "valid_to_raw",
    "record_hash",
    "page",
    "row_index",
]
