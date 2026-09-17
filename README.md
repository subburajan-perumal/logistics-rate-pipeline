# logistics-rate-pipeline

Eight synthetic ocean-freight rate feeds → a concurrent **Go** ingestion
service → atomic runs in S3 (or a PVC) → a **PySpark** normalization job →
a rate matrix in **Redshift** / Delta, with the Go service packaged as a
**Helm** chart for kind and EKS. Everything in the feeds is invented
(`docs/sources.md`); the point is the engineering, not the data.

Built to a frozen spec — `docs/PLAN.md` — with an append-only Decision Log.

## Numbers (committed under `bench/results/`)

| what | result | evidence |
|---|---|---|
| Sources ingested per run, in parallel | **8** (JSON/CSV/gzip, page + cursor pagination, API-key auth, injected 500/429) | `docs/sources.md` |
| Records landed per run | **20 810** raw rows, 8/8 sources ok, delphine recovered from 429 + 500 with retries | `spark/tests/fixtures/raw/runs/run_id=fixture/_MANIFEST.json` |
| Worker pool vs. sequential | **6.85 s → 3.77 s (1.82×)** — capped by the slowest source (see below) | `bench/results/20260917T070639Z-pool.md` |
| Data races / goroutine leaks | `go test -race ./...` clean; `goleak` in every pool test; live `/debug/pprof/goroutineleak` endpoint | `internal/pool/pool_test.go`, `internal/server/server_test.go` |
| Cancelled run | no manifest, **0** promoted parts (unit test + in-cluster e2e script) | `TestCancelMidRunWritesNoManifestAndNoParts`, `tests/e2e/shutdown.sh` |
| pprof-driven optimization (CSV parse, 20k rows) | **−64 % allocs/op**, −49 % B/op, ≈ −15 % ns/op | `bench/results/20260917T071149Z-parse.md` |
| Normalization | 20 810 → **2 294** rows; **66** rejects across 7 named reasons; **18 450** duplicates removed; 12 broadcast joins, 0 sort-merge joins | `bench/results/20260917T070639Z-normalize.json`, `docs/plan-explain.txt` |
| Tests | Go: 8 packages, 50+ cases · Spark: 25 cases incl. every rule pass/fail and an exact-count fixture test · Helm: lint + kubeconform + `helm test` | `.github/workflows/ci.yml` |

**On the 1.82×.** The pool cannot beat the critical path: `aurora` serves two
sequential cursor pages at ~1.9 s each, so no worker count finishes under
≈ 3.8 s, while the other seven feeds sum to ≈ 2.9 s. The plan predicted
≥ 4× from a latency budget the feed spec never had; the number is committed
as measured and the reasoning is in `docs/sources.md` (D-40). The fix would
be page-level parallelism, which cursor pagination forbids by contract.

## Architecture

```
recipes/*.json ──ConfigMap──►  ingestd (Deployment)                    mocksources (Deployment)
                               POST /runs ─► Run{ctx}                   8 seeded feeds; latency and
                                 fan-out  errgroup.SetLimit(workers) ◄── failure injection
                                 per source  fetch ─► parse ─► write   (bounded channels)
                                 fan-in   SourceReport ─► _MANIFEST.json (written last)
                               /healthz /readyz /metrics /debug/pprof/goroutineleak
                                        │ sink: fs (PVC) │ s3 (Pod Identity on EKS)
                                        ▼
            runs/run_id=<id>/source=<sid>/part-NNN.jsonl.gz … + _MANIFEST.json
                                        │
                     rate_normalizer (PySpark 4.2, DataFrame API only)
                     parse → ports → containers → units → currency → 10 named rules → window dedup
                                        │
            rate_matrix/ (Parquet, partition=carrier) · rejects/ · run_metrics.json · Delta on Databricks
                                        │
                     deploy/redshift/load.py — BEGIN; DELETE run; COPY … PARQUET; INSERT load_runs; COMMIT
```

`docs/architecture.md` explains each boundary. The one-line rules: ingestion
is faithful and never drops a row; a run is atomic (manifest last); every
dropped row has a named reason; sources, rules, sinks and worker counts are
configuration.

## What each layer proves

**Go.** `internal/pool` is a bounded fan-out over sources with a
fetch→parse→write pipeline inside each one; `context` cancellation with
per-source deadlines; retry with jittered backoff and `Retry-After`
(`internal/source`, tested with a fake clock); atomic promotion on two sinks
(`internal/sink`, S3 tested against an in-process fake); graceful SIGTERM
drain (`internal/shutdown`, `server.Drain`); `slog` + Prometheus + pprof.
`go test -race`, `goleak`, and a profile-driven optimization with
before/after numbers.

**Kubernetes.** `deploy/helm/rate-pipeline`: ServiceAccount, ConfigMap
(checksum-annotated → rollout on recipe change), PVC, Deployment (probes,
requests/limits, `terminationGracePeriodSeconds`, non-root read-only
container), Service, Ingress, `helm test`; the same chart runs on kind
(`values-kind.yaml`) and EKS (`values-eks.yaml`, S3 via Pod Identity —
no keys in the cluster). `deploy/terraform/eks` provisions a
no-NAT VPC + EKS + one `t3.small` for a time-boxed run.

**Data engineering.** `spark/src/rate_normalizer`: explicit schema with
`PERMISSIVE` corrupt-record capture, multi-format date/number parsing,
broadcast lookups, per-TEU unit and static-FX currency conversion, ten
config-ordered reject rules, `row_number()` dedup with a total tie-break,
Parquet partitioned by carrier, run metrics; Delta `MERGE` on Databricks
serverless via a bundle; an idempotent Redshift `COPY` loader with reasoned
`DISTKEY`/`SORTKEY` (`deploy/redshift`).

## Run it

### Layers 1 + 3 on a laptop (no Docker, no cloud) — what has been run

```bash
# Go 1.27+, Java 17, Python 3.10+
make fixture            # regenerates spark/tests/fixtures from the mock feeds (20 810 records)
make test test-race     # Go suite (race detector needs a C toolchain on Windows: winget install BrechtSanders.WinLibs.POSIX.UCRT)
make mock-up && make run-local && make bench   # real latencies → data/raw and bench/results
make spark-venv spark-test                     # 25 Spark tests
make spark-local RUN_ID=<id from data/raw/runs>
```

Windows note: PySpark local writes need `winutils`; set
`HADOOP_HOME=D:\tools\hadoop` with `bin/winutils.exe` + `bin/hadoop.dll`
(Hadoop 3.3.6 builds work with the bundled 3.5 client). The session builder
pins `spark.driver.host=localhost` because hostnames with underscores are
invalid Spark URLs.

### Layer 2 on kind — needs Docker

```bash
scripts/bootstrap-wsl.sh     # or bootstrap-macos.sh
cp .env.example .env.kind    # EVENTIDE_API_KEY is enough for the fs sink
make kind-up deploy-kind     # one-node cluster, ingress-nginx, chart
make run-kind                # POST /runs through http://ingest.localtest.me
make test-kind               # helm test + mid-run pod delete e2e
make kind-down
```

### Cloud (time-boxed, documented, not yet executed)

- EKS: `docs/runbook-eks.md` (≈ $0.50 for a 3-hour window; `terraform destroy` the same day).
- Databricks Free Edition: `deploy/databricks/README.md`.
- Redshift Serverless: `deploy/redshift/create.sh <bucket>` → `load.py` → `destroy.sh` (4 RPU floor, $1.50/h while active).

## Design notes (short)

- **Why ingestion stays faithful (D-11).** Go maps and types; Spark decides.
  A wrong rule is a re-run over history, not a re-crawl.
- **Why atomic runs (D-14).** A manifest written last turns "did the pod
  die mid-run?" into a file-existence check, and makes the S3→Spark handoff
  idempotent.
- **Why a Deployment, not a CronJob (D-17).** Probes, rolling updates, a
  Service and a live pprof endpoint are the Kubernetes surface worth
  demonstrating; a ticker (`INGESTD_SCHEDULE`) covers the cron case.
- **Why no UDFs (D-26).** Python UDFs serialize every row across the JVM
  boundary and are unavailable on serverless; every step here is built-in
  functions and joins — enforced by `tests/unit/test_purity.py`.
- **Why static FX (D-29).** Deterministic outputs; the table says its as-of.
- **Why `DISTKEY(lane_id)` (D-34).** Lane-level joins/aggregations
  co-locate; `SORTKEY(carrier, valid_from)` matches the dominant filter.
- **What is synthetic (D-07).** All of it. `mocksources` is in the repo.

## Layout

```
cmd/ingestd, cmd/mocksources      Go binaries
internal/{recipe,source,parse,record,sink,pool,server,metrics,shutdown,mock}
recipes/                           8 recipes (the only feed→ingestd contract)
spark/                             rate_normalizer package, config tables, tests, fixture
deploy/{kind,helm,terraform/eks,databricks,redshift}
bench/results/                     committed numbers (never overwritten)
docs/                              PLAN.md (frozen spec), architecture, sources, runbooks, Spark plan
tests/e2e/                         in-cluster shutdown test
```

MIT. Built as a portfolio project; see `docs/PLAN.md` §1 for the claims it backs.
