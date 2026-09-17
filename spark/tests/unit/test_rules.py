import gzip
import json

import pytest

from rate_normalizer import io, job, rules, schema, stages
from tests.unit.test_stages import raw_df

AS_OF = "2026-09-01"


def annotate(spark, ref, rows):
    df = stages.parse_types(raw_df(spark, rows))
    df = stages.normalize_ports(df, ref)
    df = stages.normalize_container(df, ref)
    df = stages.normalize_units(df, ref)
    return stages.normalize_currency(df, ref)


@pytest.mark.parametrize(
    "row,reason",
    [
        ({}, None),
        ({"price_raw": "", "price": None}, "bad_price"),
        ({"price_raw": "-120", "price": -120.0}, "bad_price"),
        ({"origin_raw": "Springfield"}, "unknown_port"),
        ({"origin_raw": "INMAA", "destination_raw": "Chennai"}, "same_port"),
        ({"container_type_raw": "45HC"}, "unknown_container"),
        ({"container_type_raw": "40HC", "unit": "per_teu"}, None),  # 40HC has a TEU factor
        ({"currency": "EURO"}, "unknown_currency"),
        ({"valid_from_raw": "soon"}, "bad_dates"),
        ({"valid_from_raw": "2026-12-31", "valid_to_raw": "2026-07-01"}, "dates_reversed"),
        ({"valid_from_raw": "2026-04-01", "valid_to_raw": "2026-06-30"}, "expired_as_of"),
        ({"valid_from_raw": "2026-10-01", "valid_to_raw": "2026-12-31"}, "expired_as_of"),  # not yet valid
    ],
)
def test_each_rule_pass_and_fail(spark, ref, row, reason):
    accepted, rejected = rules.validate(annotate(spark, ref, [row]), ref.rules, AS_OF)
    if reason is None:
        assert accepted.count() == 1 and rejected.count() == 0
    else:
        assert accepted.count() == 0
        assert rejected.collect()[0]["reject_reason"] == reason


def test_first_reason_only_and_order(spark, ref):
    rows = [{"price_raw": "", "price": None, "origin_raw": "Nowhere"}]
    _, rejected = rules.validate(annotate(spark, ref, rows), ref.rules, AS_OF)
    assert rejected.collect()[0]["reject_reason"] == "bad_price"  # first in rules.yaml
    _, rejected = rules.validate(annotate(spark, ref, rows), ["unknown_port", "bad_price"], AS_OF)
    assert rejected.collect()[0]["reject_reason"] == "unknown_port"  # config order decides


def test_disabled_rule_lets_rows_through(spark, ref):
    names = [n for n in ref.rules if n != "expired_as_of"]
    rows = [{"valid_from_raw": "2026-04-01", "valid_to_raw": "2026-06-30"}]
    accepted, _ = rules.validate(annotate(spark, ref, rows), names, AS_OF)
    assert accepted.count() == 1


def test_unknown_rule_name_raises():
    with pytest.raises(ValueError):
        rules.reason_column(["nope"], AS_OF)


def test_corrupt_json_line_is_rejected_not_fatal(spark, ref, tmp_path):
    run = tmp_path / "runs" / "run_id=c"
    (run / "source=x").mkdir(parents=True)
    good = {
        "schema_version": 1,
        "run_id": "c",
        "source_id": "meridian",
        "carrier": "MERIDIAN",
        "fetched_at": "2026-09-01T00:00:00Z",
        "page": 0,
        "row_index": 0,
        "origin_raw": "INMAA",
        "destination_raw": "NLRTM",
        "container_type_raw": "40HC",
        "price_raw": "1",
        "price": 1.0,
        "currency": "USD",
        "unit": "per_container",
        "valid_from_raw": "2026-07-01",
        "valid_to_raw": "2026-12-31",
        "surcharges": [],
        "source_ref": "",
        "record_hash": "h",
    }
    with gzip.open(run / "source=x" / "part-000.jsonl.gz", "wt", encoding="utf-8") as f:
        f.write(json.dumps(good) + "\n")
        f.write("{this is not json\n")
    manifest = {
        "run_id": "c",
        "started_at": "",
        "finished_at": "",
        "workers": 1,
        "as_of": AS_OF,
        "totals": {"records": 2, "sources_ok": 1, "sources_failed": 0},
    }
    (run / "_MANIFEST.json").write_text(json.dumps(manifest))
    raw = io.read_raw(spark, str(run))
    matrix, rej, _ = job.transform(raw, ref, AS_OF)
    assert matrix.count() == 1
    assert [r["reject_reason"] for r in rej.collect()] == ["corrupt_json"]
    assert set(matrix.columns) == set(schema.RATE_MATRIX_COLUMNS)
