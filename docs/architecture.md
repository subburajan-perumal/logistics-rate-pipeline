# Architecture

Three layers, three contracts, one repo. The spec is `docs/PLAN.md`; this
page is the map of what was built and why each boundary sits where it does.

```
recipes/*.json ──ConfigMap──►  ingestd (Deployment)                     mocksources (Deployment)
                               POST /runs ─► Run{ctx}                    8 seeded synthetic feeds
                                 fan-out: errgroup.SetLimit(workers)  ◄── HTTP: JSON/CSV/gzip, pagination,
                                 per source: fetch ─► parse ─► write      auth, latency, injected 500/429
                                 fan-in: SourceReport ─► _MANIFEST.json
                               /healthz /readyz /metrics /debug/pprof/goroutineleak
                                        │ Sink: fs (PVC) │ s3 (keys on kind, Pod Identity on EKS)
                                        ▼
        runs/run_id=<id>/source=<sid>/part-NNN.jsonl.gz … + _MANIFEST.json (written last)
                                        │
                     rate_normalizer (PySpark, DataFrame API only)
                     parse → ports → containers → units → currency → rules → dedup
                                        │
        rate_matrix/ (Parquet, partition=carrier) · rejects/ · run_metrics.json · Delta on Databricks
                                        │
                     deploy/redshift/load.py: BEGIN; DELETE run; COPY PARQUET; INSERT load_runs; COMMIT
```

## Contracts

| Contract | Producer → consumer | Defined in |
|---|---|---|
| Recipe | repo → `ingestd` | `internal/recipe` (validated at startup; readiness stays false on any invalid file) |
| Feed shape | `mocksources` → `ingestd` via the recipe | `internal/mock`, `docs/sources.md` |
| Raw record + manifest | `ingestd` → Spark | `internal/record`, `spark/src/rate_normalizer/schema.py` |
| Normalized tables + metrics | Spark → Redshift / Delta | `schema.py` (`RATE_MATRIX_COLUMNS`), `deploy/redshift/ddl.sql` |
| Chart values | operator → cluster | `deploy/helm/rate-pipeline/values*.yaml` |

## Layer 1 — `ingestd`

**Concurrency (D-12).** Two levels, each with a reason. Across sources:
`errgroup.SetLimit(workers)` bounds fan-out because sources are
latency-bound and independent. Inside a source: three goroutines — fetch,
parse, write — joined by bounded channels (`PageBuffer=2` pages,
`BatchBuffer=4` × `BatchSize=500` records) so parsing overlaps network
I/O and a slow sink applies backpressure instead of growing memory.
Paginated feeds put the next page/cursor in the body, so the fetcher waits
for the parser's `next` before requesting more — the parser and writer keep
running meanwhile. Reports fan in through a channel sized to the source
count; `Run` blocks on it, then writes the manifest.

**Cancellation.** One run context (cancelled by SIGTERM or `DELETE
/runs/{id}`), one `WithTimeout` child per source. A source that finishes
after the run was cancelled is aborted, not promoted, so a cancelled run
never has promoted parts *or* a manifest. `goleak` guards every pool test;
the live `/debug/pprof/goroutineleak` profile (Go 1.27) shows the same
thing on a running pod.

**Retry (D-13).** Exponential backoff with full jitter, capped; honours
`Retry-After` (seconds or HTTP date); retries 5xx/429/network only; a
`Clock` interface makes the schedule unit-testable without sleeping.

**Atomic runs (D-14).** Parts are written under a temp name/prefix and
promoted on `Close` (rename on fs, `CopyObject`+`DeleteObject` on S3); the
manifest is written last. Downstream reads only runs with a manifest
(`rate_normalizer` raises `IncompleteRunError` otherwise).

**Light typing only (D-11).** Go maps fields and parses numbers per the
recipe hint; it never drops a row. All semantic decisions are Spark's, so
any landed run can be re-normalized with new rules.

**Observability.** `slog` JSON with `run_id`/`source_id` on every line;
Prometheus counters and histograms (`internal/metrics`); pprof mux.
`/readyz` = recipes valid ∧ sink reachable, and flips false while draining
so the Service stops routing before the pod exits.

## Layer 2 — Kubernetes

Objects (D-22): ServiceAccount, ConfigMap (recipes, checksum-annotated so a
recipe change rolls the Deployment), PVC (fs sink only), Deployment with
liveness/readiness probes, requests/limits, `terminationGracePeriodSeconds:
60` (the app drains within 45 s), non-root read-only-rootfs container;
Service; Ingress; plus the `mocksources` Deployment/Service. Secrets are
created out of band and mounted with `envFrom … optional: true`; on EKS the
S3 keys are absent and Pod Identity supplies credentials to the SDK's
default chain — the chart is identical in both places.

`helm test` starts a run through the Service and asserts `records > 0`.
`tests/e2e/shutdown.sh` deletes the pod mid-run and asserts the interrupted
run left no manifest and no promoted parts on the PVC.

## Layer 3 — `rate_normalizer`

DataFrame API only, enforced by an AST test (D-26) so the same wheel runs on
local Spark 4.2 and Databricks serverless (Spark Connect). Every lookup is a
broadcast join (60-row port table, aliases, FX, TEU factors, source
priority) — the committed plan in `docs/plan-explain.txt` shows 12 broadcast
hash joins and zero sort-merge joins; the only shuffle is the window for
dedup. Ten named reject rules run in `config/rules.yaml` order and a row
carries only its first reason, so reject counts add up. Dedup is
`row_number()` over the lane key ordered by source priority, freshness, then
`record_hash` — a total order, hence deterministic (D-30).

## Redshift (D-34)

`rate_matrix` is `DISTKEY(lane_id)` because lane-level joins and
aggregations are the access pattern (co-located), `COMPOUND SORTKEY(carrier,
valid_from)` because that is the dominant filter; `rejects` is `EVEN`;
`load_runs` is tiny and `ALL`. The loader runs one transaction per run
(delete → COPY → audit insert) so a re-load is a no-op — checked by loading
the same run twice.

## What is deliberately not here

Streaming, HPA, NetworkPolicy, service mesh, GitOps, tracing, dashboards,
Airflow/Lakeflow, Spectrum, and anything LLM (that is the sibling project).
See `docs/PLAN.md` §4.
