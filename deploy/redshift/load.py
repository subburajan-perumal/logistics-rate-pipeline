"""Idempotent Redshift Serverless load via the Data API (docs/PLAN.md §11, D-33).

    python deploy/redshift/load.py --run-id <id> --s3 s3://<bucket>/normalized/run_id=<id>

One transaction per run: DELETE the run's rows, COPY the Parquet files, record
the load in rates.load_runs, COMMIT. Loading the same run twice leaves the row
count unchanged (the idempotency test in Phase 9). No VPC connectivity is
needed — the Data API is an HTTPS call — and no passwords: the workgroup
default IAM role reads S3, the caller's IAM identity runs the statements.

Only stdlib + boto3 (already pulled in by the AWS CLI / awscli venv).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time

import boto3


def wait(client, stmt_id: str, timeout: int = 900) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        d = client.describe_statement(Id=stmt_id)
        if d["Status"] in ("FINISHED", "FAILED", "ABORTED"):
            return d
        time.sleep(2)
    raise TimeoutError(stmt_id)


def run_sqls(client, workgroup: str, database: str, sqls: list[str], name: str) -> dict:
    """batch-execute-statement runs the list in one transaction."""
    resp = client.batch_execute_statement(WorkgroupName=workgroup, Database=database, Sqls=sqls, StatementName=name)
    d = wait(client, resp["Id"])
    if d["Status"] != "FINISHED":
        raise RuntimeError(f"{name}: {d['Status']}: {d.get('Error')}")
    return d


def scalar(client, workgroup: str, database: str, sql: str):
    resp = client.execute_statement(WorkgroupName=workgroup, Database=database, Sql=sql)
    d = wait(client, resp["Id"])
    if d["Status"] != "FINISHED":
        raise RuntimeError(d.get("Error"))
    rows = client.get_statement_result(Id=resp["Id"])["Records"]
    return next(iter(rows[0][0].values())) if rows else None


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--run-id", required=True)
    p.add_argument("--s3", required=True, help="s3://bucket/normalized/run_id=<id> (contains rate_matrix/ and rejects/)")
    p.add_argument("--workgroup", default=os.environ.get("REDSHIFT_WORKGROUP", "rates-wg"))
    p.add_argument("--database", default=os.environ.get("REDSHIFT_DATABASE", "rates"))
    p.add_argument("--iam-role", default=os.environ.get("REDSHIFT_IAM_ROLE", ""), help="empty = workgroup default role")
    p.add_argument("--region", default=os.environ.get("AWS_REGION", "us-east-1"))
    p.add_argument("--ddl", default=os.path.join(os.path.dirname(__file__), "ddl.sql"))
    args = p.parse_args()

    client = boto3.client("redshift-data", region_name=args.region)
    role = f"IAM_ROLE '{args.iam_role}'" if args.iam_role else "IAM_ROLE default"
    prefix = args.s3.rstrip("/")
    started = time.time()

    ddl = [s.strip() for s in open(args.ddl, encoding="utf-8").read().split(";") if s.strip() and not s.strip().startswith("--")]
    run_sqls(client, args.workgroup, args.database, ddl, "ddl")

    before = scalar(client, args.workgroup, args.database, f"SELECT COUNT(*) FROM rates.rate_matrix WHERE run_id = '{args.run_id}'")

    sqls = [
        f"DELETE FROM rates.rate_matrix WHERE run_id = '{args.run_id}'",
        f"DELETE FROM rates.rejects WHERE run_id = '{args.run_id}'",
        f"DELETE FROM rates.load_runs WHERE run_id = '{args.run_id}'",
        f"COPY rates.rate_matrix FROM '{prefix}/rate_matrix/' {role} FORMAT AS PARQUET SERIALIZETOJSON",
        f"COPY rates.rejects FROM '{prefix}/rejects/' {role} FORMAT AS PARQUET",
        (
            "INSERT INTO rates.load_runs (run_id, rows_loaded, rejects, status, source_uri) "
            f"SELECT '{args.run_id}', (SELECT COUNT(*) FROM rates.rate_matrix WHERE run_id = '{args.run_id}'), "
            f"(SELECT COUNT(*) FROM rates.rejects WHERE run_id = '{args.run_id}'), 'ok', '{prefix}'"
        ),
    ]
    run_sqls(client, args.workgroup, args.database, sqls, f"load-{args.run_id}")

    after = scalar(client, args.workgroup, args.database, f"SELECT COUNT(*) FROM rates.rate_matrix WHERE run_id = '{args.run_id}'")
    out = {
        "run_id": args.run_id,
        "rows_before": before,
        "rows_after": after,
        "idempotent": before in (0, after),
        "load_seconds": round(time.time() - started, 1),
        "loaded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
