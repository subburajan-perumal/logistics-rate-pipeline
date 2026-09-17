from datetime import date

from pyspark.sql import Row
from pyspark.sql import functions as F

from rate_normalizer import schema, stages


def raw_df(spark, rows):
    base = {
        "schema_version": 1,
        "run_id": "t",
        "source_id": "meridian",
        "carrier": "MERIDIAN",
        "fetched_at": None,
        "page": 0,
        "row_index": 0,
        "origin_raw": "INMAA",
        "destination_raw": "NLRTM",
        "container_type_raw": "40HC",
        "price_raw": "2310",
        "price": 2310.0,
        "currency": "USD",
        "unit": "per_container",
        "valid_from_raw": "2026-07-01",
        "valid_to_raw": "2026-12-31",
        "surcharges": [],
        "source_ref": "",
        "record_hash": "h",
        schema.CORRUPT_COL: None,
    }
    data = []
    for i, r in enumerate(rows):
        d = dict(base)
        d.update(r)
        d["row_index"] = i
        if "record_hash" not in r:
            d["record_hash"] = f"h{i}"
        data.append(Row(**d))
    return spark.createDataFrame(data, schema=schema.RAW)


def test_parse_types_prices_and_dates(spark):
    df = raw_df(
        spark,
        [
            {
                "price_raw": "1.085,50",
                "price": None,
                "valid_from_raw": "01/07/2026",
                "valid_to_raw": "31/12/2026",
            },
            {"price_raw": "2,310.00", "price": None},
            {"price_raw": "n/a", "price": None, "valid_from_raw": "not a date"},
            {"price_raw": "12", "price": 99.0},  # Go's typed price wins when present
            {"valid_from_raw": "01-Jul-2026", "valid_to_raw": "2026/12/31"},
        ],
    )
    out = stages.parse_types(df).orderBy("row_index").collect()
    assert out[0]["price_parsed"] == 1085.5 and out[0]["valid_from"] == date(2026, 7, 1)
    assert out[1]["price_parsed"] == 2310.0
    assert out[2]["price_parsed"] is None and out[2]["valid_from"] is None
    assert out[3]["price_parsed"] == 99.0
    assert out[4]["valid_from"] == date(2026, 7, 1) and out[4]["valid_to"] == date(2026, 12, 31)
    assert out[0]["origin_key"] == "INMAA"


def test_normalize_ports_aliases_cities_and_case(spark, ref):
    df = stages.parse_types(
        raw_df(
            spark,
            [
                {"origin_raw": " jnpt ", "destination_raw": "Rotterdam"},
                {"origin_raw": "MADRAS", "destination_raw": "Abu Dhabi"},
                {"origin_raw": "Springfield", "destination_raw": "NLRTM"},
            ],
        )
    )
    out = stages.normalize_ports(df, ref).orderBy("row_index").collect()
    assert (out[0]["origin_locode"], out[0]["destination_locode"]) == ("INNSA", "NLRTM")
    assert (out[1]["origin_locode"], out[1]["destination_locode"]) == ("INMAA", "AEKLF")
    assert out[2]["origin_locode"] is None and out[2]["destination_locode"] == "NLRTM"


def test_normalize_container_and_units(spark, ref):
    df = stages.parse_types(
        raw_df(
            spark,
            [
                {"container_type_raw": "40HQ", "price_raw": "100", "price": 100.0},
                {"container_type_raw": "40", "price_raw": "100", "price": 100.0, "unit": "per_teu"},
                {"container_type_raw": "20", "price_raw": "100", "price": 100.0, "unit": "per_teu"},
                {"container_type_raw": "45HC", "price_raw": "100", "price": 100.0},
            ],
        )
    )
    df = stages.normalize_container(df, ref)
    out = stages.normalize_units(df, ref).orderBy("row_index").collect()
    assert out[0]["container_type"] == "40HC" and out[0]["base_price"] == 100.0
    assert out[1]["container_type"] == "40DRY" and out[1]["base_price"] == 200.0  # per TEU x 2
    assert out[2]["container_type"] == "20DRY" and out[2]["base_price"] == 100.0
    assert out[3]["container_type"] is None


def test_normalize_currency_and_surcharges(spark, ref):
    df = stages.parse_types(
        raw_df(
            spark,
            [
                {
                    "currency": "eur",
                    "price_raw": "1000",
                    "price": 1000.0,
                    "surcharges": [
                        Row(name="BAF", amount_raw="120,00", amount=120.0),
                        Row(name="X", amount_raw="", amount=None),
                    ],
                },
                {"currency": "EURO", "price_raw": "1000", "price": 1000.0},
                {"currency": "INR", "price_raw": "139125", "price": 139125.0},
            ],
        )
    )
    df = stages.normalize_container(df, ref)
    df = stages.normalize_units(df, ref)
    out = stages.normalize_currency(df, ref).orderBy("row_index").collect()
    assert (
        out[0]["price_usd"] == 1080.0 and out[0]["surcharges_usd"] == 129.6 and out[0]["all_in_usd"] == 1209.6
    )
    assert out[1]["usd_per_unit"] is None
    assert out[2]["price_usd"] == 1669.5


def _keyed(spark, ref, rows, ts="2026-09-01 00:00:00"):
    df = stages.parse_types(raw_df(spark, rows))
    df = stages.normalize_ports(df, ref)
    df = stages.normalize_container(df, ref)
    return df.withColumn("fetched_at", F.to_timestamp(F.lit(ts)))


def test_dedup_priority_then_freshness_then_hash(spark, ref):
    df = _keyed(
        spark,
        ref,
        [
            {"source_id": "fathom", "record_hash": "a"},
            {"source_id": "meridian", "record_hash": "b"},  # highest source priority wins
            {"source_id": "fathom", "record_hash": "c"},
        ],
    )
    out = stages.dedup(df, ref).collect()
    assert len(out) == 1 and out[0]["record_hash"] == "b"

    df2 = _keyed(
        spark, ref, [{"source_id": "fathom", "record_hash": "z"}, {"source_id": "fathom", "record_hash": "y"}]
    )
    assert stages.dedup(df2, ref).collect()[0]["record_hash"] == "y"  # equal: lowest hash

    df3 = df2.withColumn(
        "fetched_at",
        F.when(F.col("record_hash") == "z", F.to_timestamp(F.lit("2026-09-02 00:00:00"))).otherwise(
            F.col("fetched_at")
        ),
    )
    assert stages.dedup(df3, ref).collect()[0]["record_hash"] == "z"  # fresher wins
