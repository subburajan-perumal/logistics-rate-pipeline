import os
from pathlib import Path

import pytest

from rate_normalizer import io, session

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "config"
FIXTURE_RUN = ROOT / "tests" / "fixtures" / "raw" / "runs" / "run_id=fixture"


@pytest.fixture(scope="session")
def spark():
    # Windows needs HADOOP_HOME (winutils) for local writes; CI (Linux) does not.
    if os.name == "nt" and not os.environ.get("HADOOP_HOME") and Path("D:/tools/hadoop").exists():
        os.environ["HADOOP_HOME"] = "D:/tools/hadoop"
    s = session.build("local[2]", app_name="rate-normalizer-tests")
    yield s
    s.stop()


@pytest.fixture(scope="session")
def ref(spark):
    return io.load_reference(spark, str(CONFIG))
