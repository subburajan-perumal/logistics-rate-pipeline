"""The committed fixture (mocksources dump) must normalize to exactly the
counts in expected_counts.json — the contract between layer 1 and layer 3."""

import json

import pytest

from rate_normalizer import io, job
from tests.conftest import CONFIG, FIXTURE_RUN, ROOT

EXPECTED = json.loads((ROOT / "tests" / "fixtures" / "expected_counts.json").read_text())


@pytest.fixture(scope="module")
def result(spark, tmp_path_factory):
    out = tmp_path_factory.mktemp("out")
    return job.run(spark, str(FIXTURE_RUN), str(out), str(CONFIG), "2026-09-01"), out


def test_counts_match_expected(result):
    res, _ = result
    got = res.as_dict()
    for k in ["rows_in", "rows_out", "rejects_total", "rejects_by_reason", "dedup_removed", "sources_failed"]:
        assert got[k] == EXPECTED[k], k


def test_outputs_written_and_keyed(result, spark):
    _, out = result
    m = spark.read.parquet(str(out / "rate_matrix"))
    by_carrier = {r["carrier"]: r["count"] for r in m.groupBy("carrier").count().collect()}
    assert by_carrier == EXPECTED["rows_out_by_carrier"]
    key = ["carrier", "origin_locode", "destination_locode", "container_type", "valid_from"]
    assert m.dropDuplicates(key).count() == m.count()
    assert m.where("price_usd <= 0 or price_usd is null").count() == 0
    assert (out / "run_metrics.json").exists()
    rej = spark.read.parquet(str(out / "rejects"))
    assert rej.count() == EXPECTED["rejects_total"]


def test_missing_manifest_refused(spark, tmp_path):
    with pytest.raises(io.IncompleteRunError):
        job.run(spark, str(tmp_path), str(tmp_path / "out"), str(CONFIG), "2026-09-01")
