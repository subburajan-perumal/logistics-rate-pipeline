"""D-26: the job must stay Spark-Connect compatible — DataFrame API only.
An AST walk fails on any attribute access that needs a JVM-bound session."""

import ast
from pathlib import Path

BANNED_ATTRS = {
    "rdd",
    "sparkContext",
    "_jvm",
    "_jsc",
    "_sc",
    "mapPartitions",
    "foreachPartition",
    "toLocalIterator",
    "udf",
    "pandas_udf",
}
SRC = Path(__file__).resolve().parents[2] / "src" / "rate_normalizer"


def test_no_jvm_or_rdd_or_udf_usage():
    offenders = []
    for path in SRC.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and node.attr in BANNED_ATTRS:
                offenders.append(f"{path.name}:{node.lineno} .{node.attr}")
            if isinstance(node, ast.Name) and node.id in {"udf", "pandas_udf"}:
                offenders.append(f"{path.name}:{node.lineno} {node.id}")
    assert not offenders, offenders
