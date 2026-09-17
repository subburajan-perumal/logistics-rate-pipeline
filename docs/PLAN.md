# Logistics Rate Pipeline — Concurrent Go Ingestion, Kubernetes and PySpark — Detailed Build Plan

**Status: FROZEN on 2026-09-17.** This is the plan the code is built to.
Nothing in §1–§22 changes after this date. The only sections that grow
are the **Decision Log (§3)** — append-only, one row per deviation with a
reason — the **phase checkboxes (§18)**, and the **Session Log (Appendix
A)**. If the code and this document disagree, the code is wrong until a
Decision Log row says otherwise.

Written after an audit of the Windows machine this will be built on
(hardware, disk, WSL, installed toolchain, AWS CLI state), the vault's
three skill roadmaps and interview-prep notes this project must serve,
and the current documentation for every external dependency (Go, kind,
Helm, PySpark, Databricks Free Edition, Redshift Serverless, EKS, AWS
Free Tier, MinIO/LocalStack status). Sources are in Appendix B. The
short version of this plan lives in the second-brain vault at
`2 Upskilling/Skills Roadmap/Project 2 - Concurrent Go Ingestion, Kubernetes and PySpark Pipeline - Build Plan.md`;
that note is the checklist, this file is the spec. Its sibling,
Project 1, follows the same convention at
`D:\projects\logistics-rate-rag\docs\PLAN.md`.

---

## Table of contents

1. What this project must prove
2. Verified starting point — 2026-09-17 audit
3. Decision Log
4. Scope and non-goals
5. Architecture
6. Synthetic carrier sources and recipes
7. Raw record contract — what the Go service lands
8. Layer 1 — Go ingestion service
9. Layer 2 — Container, Kubernetes, Helm
10. Layer 3 — PySpark normalization job
11. Redshift load
12. Databricks deployment
13. Benchmarks and metrics
14. Configuration and secrets
15. Repository layout
16. Toolchain and pins
17. Testing and CI
18. Phases with acceptance checks
19. Risks
20. Cost budget
21. Publishing — README, LinkedIn, Career Profile
22. Definition of done
- Appendix A — Session log
- Appendix B — Sources checked on 2026-09-17
- Appendix C — Machine bootstrap, exact commands

---

## 1. What this project must prove

Three resume/interview claims, one public repo, three tailored bullets.
The vault's interview Q&A notes (Go Production Depth, Kubernetes
Interview Prep, PySpark/Databricks/Redshift Q&A) list what a loop asks;
every topic below that carries a `★` is something an interviewer can
point at in this repo instead of hearing "I've read about it".

| Claim | Currently backed by | After this project |
|---|---|---|
| **Go — production depth / concurrency** — "built a concurrent ingestion service in Go" | genuine but proprietary Freightify Go code | bounded worker pool with fan-out/fan-in ★, `context` cancellation and per-source deadlines ★, retry with backoff and `Retry-After`, graceful shutdown under SIGTERM ★, `slog` ★, Prometheus metrics, `pprof` with a measured optimization ★, goroutine-leak proof (`goleak` in tests, Go 1.27 `goroutineleak` profile live), `-race` clean, `testing/synctest` for time-dependent tests, a measured N× speedup of pool vs. sequential |
| **Kubernetes / Helm / EKS** — "ran a real service on Kubernetes" | one Qwiklabs lab (2026-09-13) | Deployment, Service, Ingress, ConfigMap (recipes), Secret, PVC, ServiceAccount, requests/limits ★, liveness/readiness probes ★, `terminationGracePeriodSeconds` honoured by the app ★, rollout on config change, packaged as a Helm 4 chart with `helm lint`/`kubeconform`/`helm test`, run on `kind` **and** on EKS provisioned by Terraform with Pod Identity for S3 |
| **PySpark / Databricks / Redshift** — "built and ran a PySpark normalization job feeding a warehouse" | nothing | DataFrame-only job (Spark Connect compatible) with explicit schema, multi-format date/number parsing, broadcast lookups ★, window-function dedup ★, named reject rules with a quarantine table, run metrics, Parquet output; run locally on Spark 4.2, on Databricks Free Edition as a serverless job via Declarative Automation Bundles writing Delta ★, and loaded into Redshift Serverless with an idempotent `COPY` ★ from Parquet with reasoned DISTKEY/SORTKEY ★ |

Target resume bullets (numbers filled in at Phase 11, never earlier):

> **Go / backend.** Built a Go ingestion service that pulls **8**
> recipe-driven carrier rate feeds concurrently through a bounded worker
> pool with context-based cancellation and graceful shutdown — **N×**
> faster than sequential on the same feeds, **0** goroutine leaks under
> `-race`, **M** records/run landed atomically in S3.

> **Kubernetes / DevOps.** Containerized it (~**K** MB distroless image)
> and ran it on kind and on Terraform-provisioned EKS as a Helm chart with
> probes, resource limits, ConfigMap-driven recipes and Pod Identity for
> S3 — a rolling restart mid-run loses **0** records.

> **Data engineering.** Wrote the PySpark job that normalizes those raw
> rows into a single rate matrix (**R** rows/run, **D %** duplicates
> removed, **Q %** quarantined with a named reason), ran it on Databricks
> serverless via bundles into Delta, and loaded it into Redshift
> Serverless with an idempotent `COPY` — **L** minutes from source pull to
> queryable row.

The demo is the committed benchmark and run-metric files, not a dashboard.

## 2. Verified starting point — 2026-09-17 audit

Everything below was checked directly on the Windows machine or in
current vendor documentation, not assumed. Each finding has a
consequence that is baked into the rest of this plan.

| # | Finding | Evidence | Consequence |
|---|---|---|---|
| A1 | **8 GB RAM** (7.89 GB physical), 6 cores / 12 threads, **547 MB free** at audit time with the usual desktop apps open. | `wmic` on 2026-09-17 | Memory is the binding constraint. WSL2 capped at 5 GB (§Appendix C); kind runs single-node; Spark local runs `local[2]` with a 1 GB driver; **kind and Spark never run at the same time** except the Phase 6 end-to-end run, and `kind delete cluster` ends every Kubernetes session. Browser closed during benchmarks. |
| A2 | **C: is 95 % full (20 GB free); D: has 109 GB free.** | `df` | Everything new lives on D:: the WSL distro (`--location D:\wsl\ubuntu`), its swap file, Docker's image store (inside the distro), the repo. Nothing is installed to C: beyond winget stubs. |
| A3 | WSL **2.6.3** is installed with **no distribution**; a hypervisor is present; `wsl --install --location` is supported. Ubuntu 22.04 / 24.04 / 26.04 LTS available online. | `wsl --version`, `wsl -l -v`, `wsl --help`, `systeminfo` | Ubuntu **24.04 LTS** (Docker's apt repo, OpenJDK 17 and every tool below are known-good on it; 26.04 is five months old) installed to D:. All Project 2 tooling runs **inside WSL**; Windows-side Python is not used for this project. |
| A4 | Not installed: Docker, Go, kind, kubectl, Helm, Java, Terraform, `gh`, `make`, `jq`, `golangci-lint`. | `command -v` sweep | Phase 0 is a real phase (2 sessions), scripted in Appendix C so it is reproducible on the Mac. Docker Desktop is **not** used — Docker Engine inside the WSL distro is lighter, keeps its data on D:, and needs no licence consideration. |
| A5 | Windows Python 3.12.10 with only `pandas` of the relevant packages; no Java on Windows. | `python --version`, `pip list` | PySpark on native Windows would need Java + `winutils`/`hadoop.dll`; avoided entirely by running Spark in WSL (A3). |
| A6 | AWS CLI 2.32.9, region `us-east-1`, stored access key returns **`InvalidClientTokenId`**. Account age/plan unknown from the machine. | `aws sts get-caller-identity` | The stored key is dead (rotated or deleted). Phase 0 logs into the console, deletes the old key, creates a new key for a **least-privilege IAM user** (`rates-dev`), sets an **AWS Budget alarm at $5**, and records whether the account is a legacy account or a post-July-2025 Free/Paid-plan account (A8). Nothing cloud-side starts until that row is in Appendix A. |
| A7 | Redshift Serverless in us-east-1: **$0.375 per RPU-hour**, base capacity adjustable **4–1024 RPU**, per-second billing with a **60-second minimum**, managed storage $0.024/GB-month; **$300 credit for 90 days** for accounts that have never used Redshift Serverless. | AWS pricing page | Workgroup pinned at **base = max = 4 RPU** → hard cap $1.50/hour; no charge when idle; sessions ≤ 2 h; namespace + workgroup deleted at the end of Phase 9. |
| A8 | AWS Free Tier since 2025-07-15 is a credits model ($100 at sign-up + up to $100 for onboarding tasks, 6 months, Free plan **or** Paid plan). Service trials such as Redshift Serverless's $300 are **Paid-plan only**. Legacy (pre-2025-07-15) accounts keep the old 12-month/always-free model but their 12-month S3 allowance has long expired. | AWS Free Tier pages | Cost budget (§20) assumes **no credits**: everything is designed to cost < $10 total. If the account turns out to be a Paid-plan account eligible for the Redshift trial, that is upside, not a dependency. |
| A9 | EKS control plane **$0.10/hour** on standard support (first 14 months of a Kubernetes minor), **$0.60/hour** on extended support. Auto Mode adds ~12 % on EC2. | EKS pricing | EKS is a **time-boxed 3-hour window**, one managed node group of one `t3.small`, a Kubernetes minor that is in standard support at the time of Phase 7, no Auto Mode, `terraform destroy` before the session ends. |
| A10 | **Databricks Community Edition is retired**; the replacement, **Free Edition**, is serverless-only: 5 concurrent job tasks, one 2X-Small SQL warehouse, no Scala/R, Unity Catalog preconfigured (`workspace` catalog), outbound internet restricted until LinkedIn verification, non-commercial only. Serverless environment **version 6** (2026-09-03) — Python 3.12.3, Spark Connect client API. Community threads confirm the `spark._jsc.hadoopConfiguration()` S3-key trick **does not work** on serverless; whether a Free Edition user can create a UC **storage credential / external location** for their own S3 bucket is **not documented either way**. Declarative Automation Bundles (formerly Asset Bundles) deploy to Free Edition with `serverless: true`. | Databricks docs + community threads | The PySpark job is written **DataFrame-API only** (no RDD, no `sparkContext`, no `_jvm`) and unit-tested for that (§17). Input on Databricks is a **UC Volume** populated from S3 by a small sync step; if Phase 8 finds external locations work on Free Edition, the S3 path is used instead and logged. Output is **Delta** on Databricks, Parquet locally. |
| A11 | **PySpark 4.2.0** is current: Python ≥ 3.10, **Java 17+**; extras `pyspark[connect]`, `pyspark-client` (pure-Python Spark Connect client). Spark 4.x requires Java 17/21. | spark.apache.org install page | Local Spark = `pyspark==4.2.0` + OpenJDK 17 headless in WSL. CI = `actions/setup-java` 17 + pip. No `s3a://` in the local path (would need a Hadoop-AWS jar pinned to the bundled Hadoop version) — local input is an `aws s3 sync` mirror or the `fs` sink output; `s3a` is an optional, documented flag (§10.1). |
| A12 | **Go 1.27** (released 2026-08-19): generic methods; `encoding/json` backed by **v2** by default (`encoding/json/v2`, `jsontext`); **`goroutineleak` pprof profile GA** (`/debug/pprof/goroutineleak`); `testing/synctest.Sleep`; `net/http/httptest.NewTestServer` for synctest; `go test` runs the `stdversion` vet check; macOS 13+ required. | go.dev release notes | `go 1.27` in `go.mod`; recipes decoded with `encoding/json/v2`; leak profile exposed and checked in the Phase 3 acceptance; `synctest` used for retry/backoff timing tests. |
| A13 | **kind v0.33.0** defaults to Kubernetes **1.36.1** (`kindest/node:v1.36.1`), also ships images for 1.37.0 / 1.35.8; v1beta4 kubeadm config for ≥ 1.36. | kind releases | kind v0.33.0 with the default node image; the Kubernetes minor used on EKS is chosen at Phase 7 from the standard-support list and logged. |
| A14 | **Helm 4** (Nov 2025) is current (4.2.x, Aug 2026); Helm 3's last minor was 3.22.0 (2026-09-10), security fixes end 2027-02-10. | helm.sh blog | Helm **4** only; the chart uses `apiVersion: v2` (still valid) and nothing Helm-3-specific. |
| A15 | **MinIO's open-source edition is archived** (repo archived 2026-02-12; no binaries/images since Oct 2025). **LocalStack Community was discontinued 2026-03-23** (single auth-token image, non-commercial tier requires registration). | itsfoss / linuxiac / LocalStack pricing pages | **No local S3 emulator in the runtime path.** S3 sink tests use the in-process Go fake `gofakes3` (`johannesboyne/gofakes3`, MIT); kind-only demos use the `fs` sink to a PVC; everything else uses real S3 (pennies). MiniStack is noted as an emulator fallback but not a dependency. |
| A16 | Two dev machines: this Windows laptop (WSL) and a Mac. Project 1 already set the precedent for `.gitattributes` LF and a `docs/PLAN.md` spec. | vault | `.gitattributes` `* text=auto eol=lf`; repo cloned into the WSL filesystem, not `/mnt/d` (§D-05); Appendix C has a macOS column. |
| A17 | Project 1 occupies the daily 09:00–11:00 build block until roughly the second week of October 2026 (its plan: ~14 sessions from 2026-09-17). | Project 1 plan §18 | Project 2 **Phase 0 (installs, waiting on downloads) may run in evenings before that**; coding phases start after Project 1 Phase 8 is ticked. Target dates in §18 assume a **2026-10-12** coding start. |
| A18 | The vault's three interview Q&A notes enumerate the topics loops ask: worker pool, fan-in/fan-out, context, `select`, `sync`, pprof, GC/escape analysis, benchmarks (Go); probes, requests/limits, Services, Ingress, Helm, Jobs/CronJobs, troubleshooting (Kubernetes); lazy evaluation, shuffles, partitioning, broadcast joins, skew, window functions, UDF cost, Delta, Unity Catalog, dist/sort keys, `COPY`, idempotency (Data). | vault notes | Design choices in §8–§11 deliberately touch each of these so every answer can cite this repo (the `★` marks in §1). Nothing is added *only* to tick a topic — each has a functional reason stated where it appears. |

## 3. Decision Log

Append-only. Rows D-01…D-38 were made while writing this plan; D-39
onwards are deviations discovered during the build, each with its reason.

| ID | Decision | Why |
|---|---|---|
| D-01 | Repo name `logistics-rate-pipeline` under `subburajan-perumal`, MIT, **private until Phase 11**, then public. Local path `~/projects/logistics-rate-pipeline` inside WSL; `D:\projects\logistics-rate-pipeline` (this file's home) is the Windows-side mirror clone used only for reading/editing docs. | Sibling naming to `logistics-rate-rag`; private-first gives a second secret scan before exposure (Project 1 D-22). |
| D-02 | One repo, three layers, **root `go.mod`**, Python package under `spark/`, deployment under `deploy/`. Not three repos. | The whole point (vault Project 2 note) is one linkable artifact; a monorepo also lets one CI run prove all three layers on each push. |
| D-03 | All development inside **WSL2 Ubuntu 24.04 installed to `D:\wsl\ubuntu`**, with **Docker Engine in the distro** (not Docker Desktop), `.wslconfig` `memory=5GB`, `swap=8GB` on `D:\wsl\swap.vhdx`. | A1–A4. Keeps C: untouched, keeps memory bounded and visible, one Linux toolchain identical to CI and to the Mac. |
| D-04 | Ubuntu 24.04 LTS, not 26.04. | A3 — every tool's install docs target 24.04; nothing here needs a newer kernel/userland. |
| D-05 | The working clone lives in the **WSL filesystem** (`~/projects/…`), not on `/mnt/d`. The Windows mirror clone is a plain second checkout that only ever pulls. | 9P/virtiofs I/O on `/mnt/d` makes `go build`, `docker build` contexts and file watchers slow and mangles permissions; the repo is the source of truth on GitHub anyway. |
| D-06 | **Go 1.27**, `encoding/json/v2` for recipes and JSONL output, standard library for HTTP/CSV/gzip/slog/pprof; third-party limited to `golang.org/x/sync` (`errgroup`), `github.com/aws/aws-sdk-go-v2` (S3 only), `github.com/prometheus/client_golang`, and test-only `go.uber.org/goleak` + `github.com/johannesboyne/gofakes3`. | A12, A15. Small dependency surface is itself a talking point; every third-party package has a one-line justification in the README. |
| D-07 | Sources are **synthetic HTTP feeds served by `mocksources`**, a second Go binary in the same repo, deployed alongside `ingestd` in the cluster. **No real carrier endpoints, no scraping, no Freightify recipes.** | Reproducible latency/error injection is what makes the concurrency numbers honest and re-runnable by a reviewer; a README notice states everything is invented. |
| D-08 | Eight sources with deliberately different shapes (§6): pagination, CSV vs. JSON, city names vs. LOCODEs, per-TEU pricing, thousands separators, `dd/mm/yyyy` dates, API-key auth, a slow source, a flaky source, one large gzip CSV. | Each quirk exists to exercise one specific piece of Go (retry, streaming parse, auth from Secret) or Spark (parsing, unit/currency conversion, dedup) — nothing decorative. |
| D-09 | `mocksources` is **deterministic**: every response derives from `(source_id, seed=20260917, page)` and a fixed `--as-of` date; latency and failure injection are driven by per-source config flags, **not** randomness at request time (a seeded PRNG keyed by request sequence gives the same "random" 500s on every run). | Benchmarks must be repeatable to the record and the failure. |
| D-10 | A **recipe** is a JSON file validated against `recipes/schema.json` at startup; adding a source = adding a file to the ConfigMap, no code change. Field mapping is a flat `columns`/`paths` map, not a templating language. | Mirrors the recipe-driven onboarding already on the resume; keeps `mocksources` and `ingestd` decoupled (the recipe is the only contract). |
| D-11 | Go does **light** normalization only (field mapping, typing, trimming, `record_hash`); Spark does **all** semantic normalization (ports, units, currency, dedup, rejects). | Clean layer boundary that interviews for both roles can probe: ingestion stays faithful and replayable; set-based rules live where they are cheap to re-run over history. |
| D-12 | Concurrency model: **fan-out over sources bounded by `--workers`** (`errgroup.SetLimit`), and **inside each source a three-stage pipeline** fetch → parse → sink connected by bounded channels, so parsing overlaps network I/O; **fan-in** of per-source `SourceReport`s into one `RunReport` through a results channel. One `context` per run (cancelled on SIGTERM / `DELETE /runs/{id}`), one child context with `WithTimeout` per source. | This is the vault's worker-pool + fan-in/fan-out drill made real, with a reason for each layer of concurrency: sources are latency-bound (pool), pages are CPU+I/O mixed (pipeline). |
| D-13 | Retry policy per recipe: `max_attempts` (default 4), exponential backoff with full jitter from `base_backoff` (default 250 ms) capped at `max_backoff` (default 5 s), honours `Retry-After` on 429/503, retries only idempotent GETs on 5xx/429/network errors, never on 4xx other than 429. Jitter comes from a seeded source in tests. | Standard, explainable, and `synctest`-testable. |
| D-14 | Sinks behind one interface: `Sink.Open(run) → Writer; Writer.Write(rec); Writer.Close() → parts`, implementations `fs` (local dir / PVC) and `s3` (aws-sdk-go-v2 `manager.Uploader`). Files are `runs/run_id=<id>/source=<id>/part-<n>.jsonl.gz`, written to a temp key/name and **renamed/promoted only on `Close`**; the run's `_MANIFEST.json` is written **last** and only if every source finished or was explicitly marked failed. Spark reads only runs with a manifest. | Atomic-run semantics are what make "a restart mid-run loses 0 records" testable, and what make the S3→Spark handoff idempotent. |
| D-15 | Output format from Go is **JSONL + gzip**, not Parquet. | No Parquet writer in the standard library; a third-party Parquet dependency buys nothing here — Spark reads JSONL with an explicit schema, and the row volume (~21 k/run) is trivial. |
| D-16 | Graceful shutdown: on SIGTERM `ingestd` stops accepting new runs, cancels the run context, waits up to `shutdown_grace` (default 45 s, < the pod's `terminationGracePeriodSeconds: 60`) for writers to close/abort, then exits 0. An interrupted run has **no manifest** and its temp parts are left for a `gc` subcommand. | Ties the Go concurrency claim to the Kubernetes claim: the same test is run in-process (`go test`) and in-cluster (`kubectl delete pod` mid-run). |
| D-17 | `ingestd` is a long-running **Deployment** exposing `POST /runs`, `GET /runs/{id}`, `DELETE /runs/{id}`, `/healthz`, `/readyz`, `/metrics`, `/debug/pprof/*` (including `goroutineleak`), plus an optional built-in ticker (`--schedule 15m`). A Kubernetes **CronJob** is *not* the primary shape. | A Deployment exercises probes, rolling updates, Service and Ingress — the objects the Kubernetes prep note says get asked — and a live pprof endpoint on a running pod is the demo. The vault note already chose Deployment + Service + Ingress. |
| D-18 | Observability: `slog` JSON handler with `run_id`/`source_id` attributes on every line; Prometheus counters/histograms listed in §8.6; no tracing. | Structured logs and metrics are what an SRE loop asks about; OpenTelemetry tracing would be scope creep. |
| D-19 | Benchmark = `ingestd run --workers {1,2,4,8,16}` against the same `mocksources` config, 5 repetitions each, wall-clock + per-source durations + peak RSS, written to `bench/results/<UTC ts>-pool.json` + `.md`, committed. Reported speedup is `median(workers=1) / median(workers=8)`. | Numbers, not adjectives (vault Project 2 note). Committed results are the evidence, as in Project 1. |
| D-20 | pprof deliverable = one **CPU + alloc profile of the parse stage** on the large gzip CSV source, one optimization driven by it (expected: avoiding per-field string allocation in the CSV → record mapping), with `go test -bench` before/after committed to `bench/results/<ts>-parse.md`. Target is "measured and documented", not a promised %. | An interviewer asks "tell me about a time you profiled Go" — this is that time, with artifacts. |
| D-21 | Container: multi-stage `Dockerfile`, `CGO_ENABLED=0`, `-trimpath -ldflags="-s -w"`, final stage `gcr.io/distroless/static-debian12:nonroot`, `readOnlyRootFilesystem`, `runAsNonRoot`, `/tmp` as `emptyDir`. Both binaries from one Dockerfile via `--target`. Images published to **GHCR** by CI (`ghcr.io/subburajan-perumal/logistics-rate-pipeline/{ingestd,mocksources}:<sha>` and `:latest`), loaded into kind with `kind load docker-image` locally. | Small, non-root, reproducible; GHCR is free for public repos and avoids an ECR bill/cleanup. |
| D-22 | Kubernetes objects (namespace `rates`): `ingestd` Deployment (1 replica, `requests cpu=50m mem=64Mi`, `limits cpu=500m mem=256Mi`, `livenessProbe /healthz`, `readinessProbe /readyz`, `terminationGracePeriodSeconds 60`, ServiceAccount), ConfigMap `recipes` mounted at `/etc/recipes` with a checksum annotation on the pod template, Secret `ingestd-secrets` (S3 keys on kind, source API key), Service ClusterIP, Ingress (`ingress-nginx`, host `ingest.localtest.me` on kind), PVC `raw-data` (kind `standard` StorageClass) for the `fs` sink; `mocksources` Deployment + Service. **No HPA, no NetworkPolicy, no PDB.** | Exactly the objects the vault note named plus the two (SA, PVC) the design needs; the three exclusions are listed as non-goals so they are not "forgotten". |
| D-23 | Helm **4** chart `deploy/helm/rate-pipeline` (`apiVersion: v2`), `values.yaml` + `values-kind.yaml` + `values-eks.yaml`; recipes loaded with `.Files.Glob "recipes/*.json"`; secrets **never** in values — created out-of-band (`kubectl create secret … --from-env-file`) or, on EKS, replaced by Pod Identity. `helm test` hook POSTs a run and asserts `records > 0`. | A chart with a working `helm test` is the "understands it" tier of the Kubernetes prep note. |
| D-24 | kind cluster: **one node** (control-plane schedules workloads), `ingress-nginx` via its kind manifest, port mappings 80/443, cluster name `rates`. Created and **deleted every session** by `make kind-up` / `make kind-down`. | A1. A multi-node kind cluster on 8 GB competes with Spark for memory. |
| D-25 | EKS via **Terraform** in `deploy/terraform/eks`: VPC with 2 public subnets and **no NAT gateway**, EKS cluster on a standard-support minor, one managed node group `t3.small` × 1 (on-demand — spot interruption would confound the mid-run test), **EKS Pod Identity** addon + an IAM role scoped to the one S3 bucket, `ingress-nginx` via Helm (provisions one NLB). `terraform apply` → deploy chart → run → collect → `terraform destroy` **inside one 3-hour window**. | A9. Terraform is an existing skill on the resume (Cloudxtreme JD names EKS+Terraform); IRSA/Pod Identity for S3 is the "no keys in the cluster" story. |
| D-26 | Spark job = installable Python package `rate_normalizer` (`spark/pyproject.toml`), entry point `python -m rate_normalizer --input … --output … --run-id …`, **DataFrame API only**; a unit test walks the package with `ast` and fails on `.rdd`, `sparkContext`, `_jvm`, `_jsc`, `mapPartitions`, `foreachPartition`, Python UDF decorators. | A10 — serverless/Spark Connect compatibility by construction, and the "why not a UDF" answer from the prep note becomes a lint rule. |
| D-27 | Local Spark: `pyspark==4.2.0`, OpenJDK 17 headless, `local[2]`, `spark.driver.memory=1g`, `spark.sql.shuffle.partitions=8`. Input is a local directory (`fs` sink output or `aws s3 sync` mirror). `s3a://` supported behind `--packages` as an optional documented path, not used in tests or CI. | A1, A11. |
| D-28 | Normalization rules (§10.3) are **named** and each produces a reject reason string; the rule list and its parameters (port table, FX table, unit factors, source priority) are committed YAML/CSV under `spark/config/`; adding a rule = one function + one config line. | Same config-over-code and named-rule discipline as Project 1's gates; the quarantine table is the deterministic counterpart to Project 1's rejections. |
| D-29 | FX rates are a **static committed table** (`fx_rates.csv`, `as_of 2026-09-01`, USD base, EUR/INR/GBP/SGD/AED), not a live API. | Deterministic outputs and no third-party API; the README says so. |
| D-30 | Dedup key `(carrier, origin_locode, destination_locode, container_type, valid_from)`; winner by `row_number()` over `ORDER BY source_priority ASC, fetched_at DESC, record_hash`. | Window-function dedup with a stated tie-break is the interview answer; `record_hash` as the last key makes it total, hence deterministic. |
| D-31 | Outputs: `rate_matrix/` Parquet partitioned by `carrier`, `rejects/` Parquet with `reason`, `run_metrics.json` (rows in/out/rejected by reason, dedup removed, durations). On Databricks the same two tables are written as **Delta** (`workspace.rates.rate_matrix`, `workspace.rates.rejects`) with `MERGE` on `run_id` for idempotent re-runs, and the Parquet copy is also written to the Volume for the Redshift path. | Parquet is what `COPY … FORMAT AS PARQUET` wants; Delta on Databricks exercises the cert-relevant surface (`MERGE`, `DESCRIBE HISTORY`). |
| D-32 | Databricks deployment via **Declarative Automation Bundles** (`deploy/databricks/databricks.yml`): a wheel artifact + a serverless job (`python_wheel_task`, `environment` with the wheel as a dependency, environment version 6); documented fallback if wheel tasks are refused on Free Edition: a notebook task that `%pip install`s the wheel from the workspace path. Input `/Volumes/workspace/rates/raw/run_id=<id>/` populated by `databricks fs cp -r` from the local mirror. | A10. |
| D-33 | Redshift Serverless: namespace `rates-ns`, workgroup `rates-wg`, base **4 RPU = max 4 RPU**, not publicly accessible, all access via the **Redshift Data API** from `deploy/redshift/load.py` (boto3). IAM role `rates-redshift-s3` with read on the bucket attached as the namespace default role. Loader runs `BEGIN; DELETE … WHERE run_id = :id; COPY … FORMAT AS PARQUET; INSERT INTO load_runs …; COMMIT` → idempotent per `run_id`. | A7. Data API needs no VPC path from a laptop; the transaction is the "how do you make a load idempotent" answer. |
| D-34 | Redshift DDL: `rates.rate_matrix` `DISTKEY(lane_id)` (`lane_id = origin_locode || '-' || destination_locode`) `COMPOUND SORTKEY(carrier, valid_from)`; `rates.rejects` `DISTSTYLE EVEN`; `rates.load_runs` `DISTSTYLE ALL`. The reasoning (lane-level joins/aggregations co-locate; carrier+validity is the dominant filter; tiny audit table replicated) is in `docs/architecture.md`. | The prep note's dist/sort-key question, answered with a choice that can be defended and a table small enough that `ALL` for the audit table is obviously right. |
| D-35 | End-to-end latency metric = `load_runs.loaded_at − manifest.started_at`, for a run that went Go (kind or EKS) → S3 → local Spark → Redshift, with each stage's duration recorded in `bench/results/<ts>-e2e.json`. | One number for the resume bullet with its breakdown committed. |
| D-36 | CI (GitHub Actions, `ubuntu-latest`): `go` job (`go vet`, `golangci-lint` v2, `go test -race ./...`), `spark` job (`ruff`, `pytest` with Java 17), `helm` job (`helm lint`, `helm template | kubeconform -strict`), `docker` job (build both images; push to GHCR on `main` only), `kind-e2e` job (`helm/kind-action`, deploy chart, `helm test`) — the last one runs on `main` and on a `ci:e2e` label to keep PR runs short. No cloud credentials in CI, ever. | Green CI without secrets (Project 1 D-21), plus a real in-cluster test that costs nothing on GitHub's runners. |
| D-37 | Machine bootstrap is a committed script `scripts/bootstrap-wsl.sh` (Ubuntu) and `scripts/bootstrap-macos.sh` (Homebrew), both idempotent, plus `Makefile` targets for every recurring action (`kind-up`, `kind-down`, `deploy-kind`, `bench`, `spark-local`, `redshift-load`, …). | A4, A16 — the Mac must reach the same state from the README in one command. |
| D-38 | Line endings LF everywhere (`.gitattributes`: `* text=auto eol=lf`; `*.png binary`). | A16; Project 1 D-23. |
| D-39 | **Supersedes §6.2 row 8 and the totals.** `fathom` serves **500** lanes (10 origins × 50 destinations, the whole port table) × 3 types = 1 500 distinct rows + 50 same-port rows + 18 450 duplicates = 20 000. Per-run expectations become **2 294** normalized rows (not 6 744) and **18 450** duplicates removed (not 14 000); rejects stay 66. | The plan's "2 000 lanes" was impossible with a 60-port table (500 pairs). Fixed at build time; the fixture and `spark/tests/fixtures/expected_counts.json` freeze the corrected numbers. |
| D-40 | **`pool_speedup` is 1.82×, not ≥ 4×** (medians: 1 worker 6.85 s; 2/4/8/16 workers 3.77 s). Committed as measured; `mocksources` latencies are **not** retuned. | The plan's §13 estimate ("sequential ≈ 25–35 s") mis-added the feed spec's own latencies: `aurora` is ~3.8 s of sequential cursor pages and the other seven sum to ~2.9 s, so the ceiling is ~1.8× (Amdahl). The §19 risk row anticipated exactly this. The README states the cause and the fix that the cursor contract forbids. |
| D-41 | **Built and tested on the Windows host toolchain first** (Go 1.27.0 via winget, Temurin JDK 17.0.20, PySpark 4.2.0 in a venv, `winutils`/`hadoop.dll` 3.3.6 at `D:\tools\hadoop`, WinLibs gcc 16.1 for `-race`) instead of waiting for the WSL distro (D-03). D-03 still stands for Docker/kind; layers 1 and 3 do not need it. | The user asked for a working project the same day; installing WSL + Docker on this 8 GB / 95 %-full-C: machine is Phase 0/4 work with its own risks. Windows needed two portable fixes now in code: `spark.driver.host=localhost` (hostnames with `_` are invalid Spark URLs) and `PYSPARK_PYTHON=sys.executable` (no `python3` on Windows). |
| D-42 | `delphine`'s failure schedule is keyed by the per-source request sequence — `n%3==0` → 500, `n%5==2` → 429 with `Retry-After: 2` — instead of a one-shot 429. | A one-shot 429 fired only on the first run of a server lifetime, so benchmark repetitions saw different behaviour. The sequence-keyed schedule is deterministic per server lifetime and every run exercises both retry paths. |
| D-43 | Recipe validation is hand-written Go (`internal/recipe.Validate`, every error names its field); `recipes/schema.json` documents the same constraints and is not loaded at runtime. | No JSON Schema dependency (D-06's small-surface rule); the validator has a test per constraint. |
| D-44 | JSON feeds are parsed with the `encoding/json` token API (which Go 1.27 backs with the v2 implementation) rather than importing `encoding/json/v2` directly. | Same performance benefit, stable API, `DisallowUnknownFields`/`UseNumber` behave as documented. Revisit if `jsontext` streaming is ever needed for nested `records_path`. |
| D-45 | `explode` is `{"path": "<object>"}` only: the object's keys become `container_type_raw`, its values `price_raw`. `40HC` has a TEU factor of 2 (plan listed only 20DRY/40DRY under `per_teu`). | Simpler recipe surface; the only nested feed (`aurora`) is exactly this shape. The 40HC factor is a harmless generalisation covered by a test. |
| D-46 | `ingestd` logs (`slog` JSON) go to **stderr**; stdout carries the machine-readable manifest/benchmark JSON in `run`/`bench` modes. Parts rotate every 10 000 records (`fathom` lands as two parts). | Pipes stay composable (`ingestd run … \| jq`); Kubernetes collects both streams. |
| D-47 | Fixture generation (`mocksources dump`) uses a fixed clock (`2026-09-01T00:00:00Z`), run id `fixture` and a fixed port `127.0.0.1:18081`, so `spark/tests/fixtures/raw/` is byte-stable across machines. | `record_hash` already excludes `fetched_at`/`run_id`/`source_ref`, but the committed files should diff cleanly too. |
| D-48 | Windows local Spark writes require Hadoop's `winutils.exe` + `hadoop.dll` (`HADOOP_HOME`). The 3.3.6 community build works with PySpark 4.2's bundled Hadoop 3.5 client for local-filesystem writes. Linux/macOS/CI need nothing. | Verified 2026-09-17: reads worked without it, `write.parquet` failed with `HADOOP_HOME … unset` until it was set. |
| D-49 | Showcase surfaces (Phase 11.5, added 2026-09-17): a static results page on GitHub Pages (`docs/index.html`, regenerated from `bench/results/` by `scripts/render_results.py`, never hand-typed) and a pandas/pyarrow Streamlit explorer (`demo/app.py`) over one committed run's output in `demo/data/`. Neither touches the pipeline. No always-on Kubernetes demo. | A recruiter clicks a link for 30 seconds; a batch pipeline's showcase is evidence of the run, not a UI. Both surfaces are $0 with no card (vault note *Free Hosting Tiers for Portfolio Demos (2026)*). Spark does not fit the free Streamlit dyno, so the explorer reads Parquet with pyarrow. |
| D-50 | `bodyclose` is excluded for `internal/source/fetch_test.go` only (`.golangci.yml`). Every other lint finding is fixed in code, not silenced. | That file feeds canned `*http.Response` fakes through a scripted `RoundTripper`; the Fetcher under test owns and closes them, which the linter cannot see. The repo went public before CI had ever run (no `gh` at build time), so the first public CI run was red on lint — fixed the same day. |

## 4. Scope and non-goals

**In scope**

- A Go service that ingests eight synthetic carrier rate feeds
  concurrently, described by recipes, landing raw JSONL runs atomically
  in S3 (or a local/PVC directory), observable and profiled, shut down
  gracefully.
- The same service containerized and run on kind and on EKS from one
  Helm chart, with the Kubernetes objects listed in D-22.
- A PySpark job that turns those runs into one normalized rate matrix
  plus a quarantine table and run metrics, run locally, on Databricks
  Free Edition (Delta), and loaded into Redshift Serverless.
- Committed benchmark and metric files that back three resume bullets,
  and a README a recruiter skims in 60 seconds and an engineer runs
  locally (kind path) in 15 minutes.

**Out of scope — do not drift**

- Real carrier endpoints, scraping, any Freightify recipe/schema/data.
- Streaming (Kafka/Kinesis/Structured Streaming) — batch runs only.
- HPA, NetworkPolicy, PDB, service mesh, GitOps/ArgoCD, cert-manager,
  external-dns, multi-cluster, Auto Mode, Karpenter.
- Airflow/Dagster/Lakeflow pipelines; Delta Live Tables; Databricks SQL
  dashboards; Redshift Spectrum; Snowflake.
- A web UI. A dashboard. Tracing. Alerting rules.
- LLM anything — that is Project 1's job.
- If tempted, add a line to the vault roadmap notes instead of code.

## 5. Architecture

```
                         ┌──────────────── Kubernetes (kind │ EKS), namespace rates ────────────────┐
                         │                                                                          │
   recipes/*.json ──ConfigMap──►  ingestd (Deployment, 1 replica)                                   │
   Secret / Pod Identity ──────►  ┌─────────────────────────────────────────────────────────┐       │
                         │        │ POST /runs ─► Run{ctx, cancel}                          │       │
   mocksources ◄──HTTP───┤        │   fan-out: errgroup.SetLimit(workers)                   │       │
   (Deployment+Service)  │        │   per source: ctx.WithTimeout ─► fetch ─► parse ─► sink │       │
   8 feeds, seeded,      │        │              (bounded channels; retry/backoff on fetch) │       │
   latency + failure     │        │   fan-in: SourceReport ──► RunReport ──► _MANIFEST.json │       │
   injection             │        │ /healthz /readyz /metrics /debug/pprof/goroutineleak    │       │
                         │        └───────────────────────┬─────────────────────────────────┘       │
                         │   Service ◄── Ingress (ingress-nginx)                                    │
                         └────────────────────────────────┼──────────────────────────────────────────┘
                                                          │ Sink: s3 │ fs (PVC)
                                                          ▼
                    s3://<bucket>/runs/run_id=<id>/source=<sid>/part-000.jsonl.gz … + _MANIFEST.json
                                                          │
                              ┌───────────────────────────┼────────────────────────────┐
                              ▼                           ▼                            ▼
                    local Spark 4.2 (WSL)      Databricks Free Edition            aws s3 sync
                    python -m rate_normalizer  serverless job via bundle          (mirror for local)
                              │                (Volume input, Delta output)
                              ▼
                    out/run_id=<id>/rate_matrix/ (Parquet, partition=carrier)
                                  /rejects/ (Parquet, reason)
                                  /run_metrics.json
                              │  aws s3 cp → s3://<bucket>/normalized/run_id=<id>/
                              ▼
                    Redshift Serverless (4 RPU): deploy/redshift/load.py
                       BEGIN; DELETE run_id; COPY … PARQUET; INSERT load_runs; COMMIT
```

**Layer contracts** (the only things layers know about each other):

| Contract | Producer → consumer | Where it is defined |
|---|---|---|
| Recipe | repo → `ingestd` | `recipes/schema.json` (§6.3) |
| Feed shape | `mocksources` → `ingestd` (via recipe) | §6.2 |
| Raw record + manifest | `ingestd` → Spark | §7 |
| Normalized tables + run metrics | Spark → Redshift loader / Delta | §10.4 |
| Chart values | operator → cluster | `deploy/helm/rate-pipeline/values*.yaml` |

**Design rules**

- Ingestion is faithful; normalization is set-based and re-runnable
  (D-11). Any run in S3 can be re-normalized at any time.
- A run is atomic: it either has a manifest and is complete, or it has
  no manifest and is ignored downstream (D-14).
- Every rule that drops or changes a row has a name that appears in the
  output (D-28). No silent coercion.
- Sources, rules, thresholds, sinks, worker counts, cluster targets are
  configuration.

## 6. Synthetic carrier sources and recipes

### 6.1 Entities

- **Carriers** (invented; no real carrier names or abbreviations): `MERIDIAN`,
  `HALCYON`, `AURORA`, `BOREALIS`, `CORVUS`, `DELPHINE`, `EVENTIDE`,
  `FATHOM`. The first two reuse Project 1's invented names on purpose
  (one fictional universe across both repos; different data).
- **Ports** (`spark/config/ports.csv`, 40 rows): LOCODE, city, country,
  aliases (e.g. `Nhava Sheva`/`JNPT`/`INNSA`). Origins are Indian ports
  (`INMAA`, `INNSA`, `INMUN`, `INCOK`, `INVTZ`, `INKRI`, `INHZA`,
  `INPAV`); destinations span Europe, Gulf, SE Asia, US East/West.
- **Container types**: canonical `20DRY`, `40DRY`, `40HC`; sources also
  emit aliases `20GP`, `40GP`, `40HQ`, `20'`, `40'HC` (mapped by Spark,
  §10.3) and one unknown `45HC` (→ reject `unknown_container`).
- **Currencies**: `USD`, `EUR`, `INR`, `GBP`, `SGD`, `AED`; one source
  emits `EURO` once (→ reject `unknown_currency`).
- **Validity**: all sources' rates are valid `2026-07-01 → 2026-12-31`
  except the trap rows described per source. `--as-of 2026-09-01` in
  every committed run.

### 6.2 The eight feeds (served by `mocksources`)

| # | source_id | carrier | transport | shape | rows | quirk it exists for | latency / failure injection |
|---|---|---|---|---|---|---|---|
| 1 | `meridian` | MERIDIAN | `GET /meridian/rates?page=N` | JSON `{items:[…], next_page}` , 50/page, 4 pages | 60 lanes × 3 = 180 | baseline paginated JSON, LOCODEs, USD, ISO dates | 150 ms/page |
| 2 | `halcyon` | HALCYON | `GET /halcyon/tariff.csv` | CSV, one row per lane×type | 40 × 3 = 120 | EUR, BAF as separate column, `1.085,00`-style decimals, `dd/mm/yyyy` dates | 300 ms |
| 3 | `aurora` | AURORA | `GET /aurora/v2/lanes?cursor=…` | JSON nested `{lanes:[{origin,destination,containers:[{type,price}]}]}`, cursor pagination, 25 lanes/page | 50 × 3 = 150 | nested JSON flattening, INR, **slow**: 1.5–2.5 s per page (seeded) | slow |
| 4 | `borealis` | BOREALIS | `GET /borealis/rates.json` | JSON array | 30 × 3 = 90 | **city names** not LOCODEs (`"Chennai"`, `"Rotterdam"`, `"JNPT"`), GBP | 200 ms |
| 5 | `corvus` | CORVUS | `GET /corvus/export.csv` | CSV | 45 × 2 = 90 | **per-TEU pricing** (`unit=per_teu`, only `20` and `40` columns) → Spark multiplies 40' by 2 and derives `40HC` = `40DRY` + configured uplift is **not** done (no invention; only `20DRY`/`40DRY` produced) | 200 ms |
| 6 | `delphine` | DELPHINE | `GET /delphine/rates?page=N` | JSON, 3 pages | 35 × 3 = 105 | **flaky**: seeded 30 % of requests return 500, plus one 429 with `Retry-After: 2` on the second page — exercises retry/backoff | 100 ms + failures |
| 7 | `eventide` | EVENTIDE | `GET /eventide/rates` with `X-Api-Key` | JSON | 25 × 3 = 75 | **auth header from a Secret**; 401 without it; SGD/AED mixed per lane | 100 ms |
| 8 | `fathom` | FATHOM | `GET /fathom/bulk.csv.gz` | gzip CSV, 20 000 rows | 2 000 lanes × 3 = 6 000 distinct + 14 000 **duplicates** (older `fetched_at` variants of the same lanes) | streaming gzip parse, memory bound (peak RSS measured), and the dedup ratio for Spark | 800 ms, 3 MB gz |

Trap rows (deterministic, documented in `docs/sources.md`; each is **included** in the row counts above, not added to them):

- `meridian`: 6 lanes duplicated with `valid_to 2026-06-30` (superseded) —
  Spark's `expired_as_of` rule rejects them.
- `halcyon`: 2 rows with `valid_from > valid_to` → `dates_reversed`.
- `borealis`: 3 rows with city `"Springfield"` → `unknown_port`.
- `corvus`: 1 row with price `-120` → `bad_price`; 1 with `""` → `bad_price`.
- `delphine`: 2 rows `container_type: "45HC"` → `unknown_container`.
- `eventide`: 1 row `currency: "EURO"` → `unknown_currency`.
- `fathom`: 14 000 exact/near duplicates → dedup, not rejects; 50 rows with
  `origin == destination` → `same_port`.

Expected totals per run (asserted by a Spark test against the committed
fixture and by `mocksources --dump`): **20 810** raw rows in, **6 744**
normalized rows out, **14 000** dedup-removed, **66** rejects across 7
reasons. (Exact counts are produced by the generator and frozen into
`spark/tests/fixtures/expected_counts.json` in Phase 1.)

### 6.3 Recipe schema (`recipes/schema.json`, JSON Schema 2020-12)

```json
{
  "source_id": "halcyon",
  "carrier": "HALCYON",
  "url": "http://mocksources:8081/halcyon/tariff.csv",
  "format": "csv",                       // csv | json
  "compression": "none",                 // none | gzip
  "auth": {"type": "none"},              // none | api_key {header, secret_env}
  "pagination": {"type": "none"},        // none | page {param, start, until_empty} | cursor {path, param}
  "records_path": "",                    // json: dotted path to the array ("items", "lanes[].containers")
  "columns": {                           // target field  ← source column / dotted path
    "origin_raw": "Origin", "destination_raw": "Destination",
    "container_type_raw": "Equipment", "price_raw": "Rate",
    "currency": "Currency", "unit": "per_container",
    "valid_from_raw": "ValidFrom", "valid_to_raw": "ValidTo",
    "surcharges": [{"name": "BAF", "column": "BAF"}]
  },
  "hints": {"decimal_style": "eu", "date_layout": "02/01/2006"},
  "timeout": "20s",
  "retry": {"max_attempts": 4, "base_backoff": "250ms", "max_backoff": "5s"},
  "rate_limit_rps": 10
}
```

Validation at startup fails the process (readiness never turns true) on
any invalid recipe — misconfiguration is visible in `kubectl describe`,
not discovered on the first run.

## 7. Raw record contract — what the Go service lands

`runs/run_id=<YYYYMMDDTHHMMSSZ>-<6 hex>/source=<source_id>/part-<NNN>.jsonl.gz`,
one JSON object per line, `schema_version: 1`:

| Field | Type | Notes |
|---|---|---|
| `schema_version` | int | `1` |
| `run_id`, `source_id`, `carrier` | string | |
| `fetched_at` | RFC 3339 UTC | time the page was received |
| `page` | int | 0 for single-document sources |
| `row_index` | int | index within the page/document |
| `origin_raw`, `destination_raw` | string | as received (LOCODE or city or alias) |
| `container_type_raw` | string | as received |
| `price_raw` | string | as received, untrimmed (`"1.085,00"`) |
| `price` | number \| null | Go's best-effort parse using `hints.decimal_style`; null if unparseable — **Spark decides**, Go never drops |
| `currency` | string | as received, upper-cased |
| `unit` | string | `per_container` \| `per_teu` from the recipe |
| `valid_from_raw`, `valid_to_raw` | string | as received |
| `surcharges` | array of `{name, amount_raw, amount}` | may be empty |
| `source_ref` | string | URL + page |
| `record_hash` | string | sha256 of the canonical raw fields (excluding `fetched_at`, `run_id`) — the dedup tie-break |

`_MANIFEST.json` (written last):

```json
{"run_id": "…", "started_at": "…", "finished_at": "…", "workers": 8,
 "sources": {"meridian": {"status": "ok", "records": 180, "pages": 4, "attempts": 4, "duration_ms": 640, "parts": ["part-000.jsonl.gz"]},
             "delphine": {"status": "ok", "records": 105, "pages": 3, "attempts": 7, "retries": 4, "duration_ms": 3210, "parts": ["…"]}},
 "totals": {"records": 20810, "sources_ok": 8, "sources_failed": 0},
 "ingestd_version": "<git sha>", "recipes_sha256": "…"}
```

A source that exhausts retries is `status: failed` with `error`; the run
still gets a manifest (partial runs are valid runs — Spark records
`sources_failed` in `run_metrics.json`). A run cancelled by shutdown gets
**no** manifest.

## 8. Layer 1 — Go ingestion service

### 8.1 Packages

```
cmd/ingestd/          main: flags/env → config; wires server + runner; signal handling
cmd/mocksources/      main: the eight feeds, seeded generator, latency/failure injection, --dump
internal/recipe/      Recipe types, JSON Schema validation (json/v2), loading from a directory
internal/source/      Fetcher (HTTP client, auth, pagination, retry/backoff, rate limit)
internal/parse/       csv.go, json.go: page bytes → []RawRecord (streaming; gzip aware)
internal/record/      RawRecord, Manifest, hashing, canonical JSON encoding
internal/sink/        Sink/Writer interfaces; fs/ and s3/ implementations; temp→final promotion
internal/pool/        Run, Runner: fan-out (errgroup), per-source pipeline, fan-in, cancellation
internal/server/      HTTP API, healthz/readyz, metrics registration, pprof mux
internal/metrics/     Prometheus collectors
internal/shutdown/    signal → cancel → grace wait
```

### 8.2 The run pipeline (`internal/pool`)

```go
func (r *Runner) Run(ctx context.Context, recipes []recipe.Recipe, opts Options) (*record.Manifest, error) {
    ctx, cancel := context.WithCancel(ctx); defer cancel()
    g, gctx := errgroup.WithContext(ctx)
    g.SetLimit(opts.Workers)                                  // fan-out bound
    reports := make(chan SourceReport, len(recipes))          // fan-in
    for _, rc := range recipes {
        g.Go(func() error {
            sctx, scancel := context.WithTimeout(gctx, rc.Timeout); defer scancel()
            reports <- r.runSource(sctx, rc, opts)            // never returns error: failures are data
            return nil
        })
    }
    go func() { _ = g.Wait(); close(reports) }()
    manifest := collect(reports)                              // blocks until closed
    if ctx.Err() != nil { return nil, ctx.Err() }             // cancelled → no manifest written
    return manifest, r.sink.WriteManifest(ctx, manifest)
}
```

`runSource` is the per-source three-stage pipeline: `fetch` goroutine
(pagination loop, retry, emits `page` on a `chan []byte` of capacity 2)
→ `parse` goroutine (emits `[]RawRecord` batches on a chan of capacity 4)
→ `write` (the calling goroutine, owns the sink `Writer`, closes it on
success, aborts on error/cancel). Every stage selects on `ctx.Done()`.
Backpressure is the channel capacity; nothing is unbounded.

Rules the tests enforce: no goroutine outlives `Run` (goleak); a cancelled
run leaves no manifest and no promoted parts; a source failure never
fails the run; `workers=1` and `workers=8` produce byte-identical record
sets (order-independent comparison by `record_hash`).

### 8.3 Fetcher (`internal/source`)

- One `http.Client` per run with `Timeout` unset (deadlines come from
  context), `Transport` with `MaxIdleConnsPerHost = workers`.
- Retry per D-13; `Retry-After` parsed as seconds or HTTP-date; backoff
  sleeps via a `Clock` interface so `synctest` can drive it.
- Token-bucket rate limit per source (`rate_limit_rps`), implemented with
  `time.Ticker` behind the same `Clock`.
- Auth `api_key`: header value read from the environment variable named
  in the recipe (`secret_env`), never from the recipe file.
- Response bodies are streamed into the parser (`io.Reader`), never
  fully buffered, so `fathom` (3 MB gz → ~40 MB text) stays under the
  pod's 256 Mi limit with room; peak RSS is a benchmark output.

### 8.4 Parsers (`internal/parse`)

- CSV: `encoding/csv` with `ReuseRecord = true`; header → column index
  resolved once per document; per-row mapping into `RawRecord` using the
  recipe's `columns`. The pprof optimization target (D-20) lives here.
- JSON: `encoding/json/v2` `jsontext.Decoder` streaming through the
  `records_path`, so a 4-page response is decoded item by item.
- Number parsing honours `hints.decimal_style` (`us` `1,085.00` / `eu`
  `1.085,00` / `plain`); unparseable → `price: null`, never an error.

### 8.5 Sinks (`internal/sink`)

- `fs`: writes `…/part-NNN.jsonl.gz.tmp`, `fsync`, `rename` on `Close`;
  manifest written via temp + rename.
- `s3`: `manager.Uploader` to key `…/part-NNN.jsonl.gz` under a
  `_tmp/` prefix, then `CopyObject` + `DeleteObject` to the final key on
  `Close` (S3 has no rename; the copy is the promotion). Region/bucket
  from config; credentials from the default chain (env on kind, Pod
  Identity on EKS). Tested against `gofakes3` in-process.
- Both implement `Abort()` for cancellation: temp objects deleted; a
  `gc` subcommand removes stale `_tmp/` older than 24 h.

### 8.6 Observability (`internal/metrics`, `internal/server`)

Prometheus (`/metrics`):

| Name | Type | Labels |
|---|---|---|
| `ingest_runs_total` | counter | `status` (ok, partial, cancelled) |
| `ingest_records_total` | counter | `source` |
| `ingest_source_duration_seconds` | histogram | `source`, `status` |
| `ingest_http_requests_total` | counter | `source`, `code` |
| `ingest_retries_total` | counter | `source`, `reason` (5xx, 429, network) |
| `ingest_inflight_sources` | gauge | — |
| `ingest_run_duration_seconds` | histogram | `workers` |

`slog` JSON to stdout; every line inside a run carries `run_id` and,
inside a source, `source_id`; levels: `INFO` per source start/finish,
`WARN` per retry, `ERROR` per source failure, `DEBUG` per page (off by
default). `/debug/pprof/` full mux including `goroutineleak`
(Go 1.27); `/healthz` = process alive; `/readyz` = recipes valid **and**
sink reachable (`HeadBucket` / directory writable) — flips false during
shutdown so the Service stops routing before the pod dies.

### 8.7 CLI

```
ingestd serve  --recipes /etc/recipes --sink s3 --bucket … [--schedule 15m] [--workers 8] [--listen :8080]
ingestd run    --recipes ./recipes --sink fs --out ./data/raw --workers 8 [--as-of 2026-09-01]   # one run, exit
ingestd bench  --recipes ./recipes --sink fs --out /tmp/bench --workers 1,2,4,8,16 --reps 5 --json bench/results/<ts>-pool.json
ingestd gc     --sink s3 --bucket … --older-than 24h
mocksources serve --listen :8081 --seed 20260917 --as-of 2026-09-01 [--latency-scale 1.0] [--no-failures]
mocksources dump --out spark/tests/fixtures/raw/   # writes the exact run a healthy ingest would land (for Spark tests)
```

Flags mirror env vars (`INGESTD_WORKERS` …) for the container.

## 9. Layer 2 — Container, Kubernetes, Helm

### 9.1 Images (D-21)

`Dockerfile` with targets `ingestd` and `mocksources`; `docker build
--target ingestd -t ingestd:dev .`; expected size **< 20 MB** each
(recorded in `bench/results/<ts>-image.md`). Labels
`org.opencontainers.image.source/revision`. `.dockerignore` excludes
`data/`, `bench/`, `spark/`, `deploy/`.

### 9.2 kind (D-24)

`deploy/kind/cluster.yaml` (one node, `extraPortMappings` 80/443,
`kubeadmConfigPatches` for the ingress-ready label), `make kind-up`
(create cluster, install ingress-nginx, wait for the controller), `make
deploy-kind` (`kind load docker-image` both images, `kubectl create
secret … --from-env-file .env.kind`, `helm upgrade --install rates
deploy/helm/rate-pipeline -n rates --create-namespace -f
values-kind.yaml`), `make run-kind` (`curl -X POST
http://ingest.localtest.me/runs`), `make kind-down`.

`values-kind.yaml`: `sink: fs` with the PVC, `mocksources.enabled: true`,
`ingestd.image.pullPolicy: Never`, resources per D-22.

### 9.3 Helm chart (D-23)

```
deploy/helm/rate-pipeline/
├── Chart.yaml                 apiVersion v2, appVersion = git sha at release
├── values.yaml                documented defaults
├── values-kind.yaml / values-eks.yaml
├── recipes/*.json             ← symlinked/copied from /recipes at build (make helm-sync)
└── templates/
    ├── _helpers.tpl, namespace-less (namespace comes from --namespace)
    ├── serviceaccount.yaml    annotations for Pod Identity are not needed (association is on the AWS side)
    ├── configmap-recipes.yaml .Files.Glob "recipes/*.json"
    ├── deployment-ingestd.yaml  checksum/recipes annotation, probes, resources, envFrom secret, volumes
    ├── service-ingestd.yaml, ingress.yaml (className nginx)
    ├── pvc.yaml               only when .Values.sink == "fs"
    ├── deployment-mocksources.yaml, service-mocksources.yaml   when .Values.mocksources.enabled
    ├── tests/test-run.yaml    helm test: curlimages/curl POSTs /runs, polls GET, asserts records > 0
    └── NOTES.txt
```

Quality gates: `helm lint`, `helm template … | kubeconform -strict
-kubernetes-version <kind default>`, `helm test rates -n rates` green.

### 9.4 EKS (D-25)

`deploy/terraform/eks/` — `versions.tf` (pinned providers at Phase 7,
logged), `vpc.tf` (2 public subnets, IGW, no NAT), `eks.tf` (cluster,
managed node group `t3.small` × 1, addons `vpc-cni`, `coredns`,
`kube-proxy`, `eks-pod-identity-agent`), `iam.tf` (role
`rates-ingestd` with `s3:PutObject/GetObject/DeleteObject/ListBucket`
on the one bucket, Pod Identity association to `rates/ingestd`),
`outputs.tf` (cluster name, kubeconfig command). `values-eks.yaml`:
`sink: s3`, `mocksources.enabled: true`, images from GHCR, ingress via
ingress-nginx NLB, no Secret for AWS (Pod Identity), only the
`eventide` API key Secret. The Phase 7 runbook is `docs/runbook-eks.md`
with a **start/end timestamp table** and the AWS bill for the day.

### 9.5 In-cluster tests that back the claims

| Test | How | Expected |
|---|---|---|
| Probes | `kubectl rollout status`; break a recipe in the ConfigMap → new pod never Ready, old pod keeps serving | readiness gates rollouts |
| Config rollout | edit a recipe → `helm upgrade` → checksum annotation changes → rolling restart | one restart, no manual `kubectl rollout restart` |
| Graceful shutdown | `POST /runs` then `kubectl delete pod` mid-run | pod exits within 60 s, `_tmp/` cleaned, no manifest, next run clean; logged with timestamps |
| Resource limits | `kubectl top pod` during a run | < 128 Mi peak, recorded |
| Ingress | `curl http://ingest.localtest.me/healthz` from the host | 200 |
| helm test | `helm test rates` | passes |

## 10. Layer 3 — PySpark normalization job

### 10.1 Entry point and I/O

```
python -m rate_normalizer --input data/raw/runs/run_id=<id> --output data/out/run_id=<id> \
       --config spark/config --as-of 2026-09-01 [--format parquet|delta] [--master local[2]]
```

Reads `_MANIFEST.json` first (fails fast if absent — D-14), then
`source=*/part-*.jsonl.gz` with the **explicit schema** from §7
(`mode=PERMISSIVE`, `columnNameOfCorruptRecord=_corrupt`; corrupt lines
become `reject(reason="corrupt_json")`). `--input` may be `file://`,
`/Volumes/...` on Databricks, or `s3a://` when `--packages` is supplied
(optional, D-27).

### 10.2 Transform stages (each a pure function `DataFrame → DataFrame`)

1. `parse_types` — `price`: coalesce Go's `price` with a Spark re-parse
   of `price_raw` per `hints` (strip separators, EU decimal); dates:
   `coalesce(to_date(raw, 'yyyy-MM-dd'), to_date(raw, 'dd/MM/yyyy'),
   to_date(raw, 'dd-MMM-yyyy'))`.
2. `normalize_ports` — **broadcast join** `ports.csv` on upper-trimmed
   alias → `origin_locode`, `destination_locode`; miss → `unknown_port`.
3. `normalize_container` — alias map (`20GP→20DRY`, `40HQ→40HC`, …)
   from `container_aliases.csv`; miss → `unknown_container`.
4. `normalize_units` — `per_teu`: `20DRY ×1`, `40DRY ×2`; anything else
   under `per_teu` → `unit_unsupported`.
5. `normalize_currency` — broadcast join `fx_rates.csv` (D-29) →
   `price_usd = round(price × fx, 2)`, plus `surcharges_usd` (sum of
   parsed surcharge amounts × fx); miss → `unknown_currency`.
6. `validate` — named rules (§10.3) produce a `reject_reason` column;
   rows with a reason go to `rejects`, others continue.
7. `dedup` — window per D-30; `dedup_removed` counted.
8. `finalize` — select the output columns (§10.4), add `lane_id`,
   `run_id`, `normalized_at`.

No Python UDFs (D-26); every step is built-in functions and joins, and
the README's "why no UDFs" paragraph cites the prep note's serialization
cost argument with the actual plan (`df.explain()`) committed once in
`docs/plan-explain.txt`.

### 10.3 Reject rules (`spark/config/rules.yaml`)

| Rule | Condition | Reason |
|---|---|---|
| `corrupt_json` | `_corrupt` not null | — |
| `bad_price` | `price` null or `≤ 0` | |
| `unknown_port` | either LOCODE lookup missed | |
| `same_port` | `origin_locode == destination_locode` | |
| `unknown_container` | alias lookup missed | |
| `unit_unsupported` | see stage 4 | |
| `unknown_currency` | FX lookup missed | |
| `bad_dates` | either date failed to parse | |
| `dates_reversed` | `valid_from > valid_to` | |
| `expired_as_of` | `valid_to < as_of` **or** `valid_from > as_of` | the superseded-tariff trap |

Rule order is the table order; a row carries only its **first** reason
(so counts sum to the reject total). Each rule has a pass and a fail
test on a hand-built fixture, and the full fixture (`mocksources dump`)
must produce exactly `expected_counts.json`.

### 10.4 Output columns (`rate_matrix`)

`run_id, carrier, lane_id, origin_locode, destination_locode,
container_type, price_usd, base_price, base_currency, surcharges_usd,
all_in_usd, unit_source, valid_from, valid_to, source_id, fetched_at,
record_hash, normalized_at` — Parquet, `partitionBy("carrier")`,
`coalesce(1)` per partition (tiny data; one file per carrier keeps the
`COPY` listing short). `rejects`: the raw columns + `reject_reason`.
`run_metrics.json`: `rows_in, rows_out, rejects_by_reason, dedup_removed,
sources_failed, stage_durations_ms, spark_version, as_of`.

### 10.5 Databricks specifics (D-31, D-32)

`--format delta` writes `workspace.rates.rate_matrix` / `rejects` with
`MERGE INTO … ON t.run_id = s.run_id AND t.record_hash = s.record_hash`
(re-running a run is a no-op), then `DESCRIBE HISTORY` output is saved
to `bench/results/<ts>-delta-history.md`. The bundle target `dev` binds
the job to environment version 6 and passes `--input
/Volumes/workspace/rates/raw/run_id=<id>`.

## 11. Redshift load

`deploy/redshift/` — `ddl.sql` (D-34), `create.sh` (namespace,
workgroup at 4/4 RPU, default IAM role, `aws redshift-serverless
create-…`), `load.py` (Data API; polls `describe-statement`; prints the
transaction and the row count), `verify.sql` (row count per carrier,
one lane lookup, `SELECT * FROM load_runs`), `destroy.sh`. A second
`load.py` invocation for the same `run_id` must leave the row count
unchanged (idempotency test, recorded). Output (`bench/results/<ts>-e2e.json`)
holds each stage's timestamp for D-35.

## 12. Databricks deployment

`deploy/databricks/databricks.yml` (bundle name `rate-pipeline`, target
`dev`, `workspace.host` from env), `resources/jobs.yml` (serverless
`python_wheel_task`, `environment_key: default` with
`dependencies: ["./dist/*.whl"]`, `environment_version: "6"`),
`artifacts` building the wheel from `spark/`. Commands: `databricks
bundle validate`, `deploy`, `run rate_normalizer_job --params …`.
Raw input arrives via `make db-sync RUN_ID=…` (`aws s3 sync` to a local
mirror, then `databricks fs cp -r … dbfs:/Volumes/workspace/rates/raw/`).
Phase 8 additionally **tries** `CREATE STORAGE CREDENTIAL` / `CREATE
EXTERNAL LOCATION` for the bucket once; the outcome (works / refused on
Free Edition) is a Decision Log row either way.

## 13. Benchmarks and metrics

All files under `bench/results/` are committed, timestamped, never
overwritten; `bench/results/LATEST.md` is a regenerated view.

| Metric | Definition | Target / expectation |
|---|---|---|
| `sources_parallel` | sources in one run | 8 |
| `pool_speedup` | `median wall(workers=1) / median wall(workers=8)`, 5 reps each, `mocksources` default latency, browser closed | **≥ 4×** (latency-bound: sequential ≈ Σ ≈ 25–35 s; parallel ≈ max ≈ 8 s) |
| `records_per_run` | `manifest.totals.records` | 20 810 |
| `peak_rss_mb` | `/proc/self/status` VmHWM sampled by `ingestd bench` | reported; < 128 Mi in-cluster |
| `goroutine_leaks` | goleak in every pool test; `/debug/pprof/goroutineleak` after a run in kind | **0** |
| `race_detector` | `go test -race ./...` | clean |
| `shutdown_records_lost` | kind mid-run delete: promoted parts without manifest | **0** (and no manifest) |
| `image_size_mb` | `docker images` | < 20 |
| `parse_bench` | `go test -bench BenchmarkParseCSV -benchmem` before/after the pprof optimization | reported (ns/op, allocs/op) |
| `rows_out`, `dedup_removed`, `reject_rate` | from `run_metrics.json` | 6 744 / 14 000 / ≈ 0.3 % of rows in (66 rows), every reject named |
| `spark_stage_durations` | `run_metrics.json` | reported |
| `e2e_latency` | D-35 | **< 5 min** for the committed run |
| `redshift_idempotent` | second load of the same run changes nothing | pass |
| `cloud_cost_usd` | AWS bill for the project's tags | ≤ $10 (§20) |

## 14. Configuration and secrets

### 14.1 Environment (`.env.example` lists every variable)

| Var | Default | Used by |
|---|---|---|
| `INGESTD_RECIPES` | `/etc/recipes` | ingestd |
| `INGESTD_SINK` | `fs` | `fs` \| `s3` |
| `INGESTD_OUT` | `/data/raw` | fs sink |
| `INGESTD_BUCKET`, `AWS_REGION` | — / `us-east-1` | s3 sink |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | — | **kind only**; EKS uses Pod Identity |
| `INGESTD_WORKERS` | `8` | |
| `INGESTD_SHUTDOWN_GRACE` | `45s` | |
| `INGESTD_AS_OF` | today | committed runs pass `2026-09-01` |
| `EVENTIDE_API_KEY` | — | the `secret_env` for source 7 (also set in `mocksources`) |
| `MOCKSOURCES_URL` | `http://mocksources:8081` | recipes are templated at chart render time |
| `DATABRICKS_HOST`, `DATABRICKS_TOKEN` | — | bundle CLI |
| `REDSHIFT_WORKGROUP`, `REDSHIFT_DATABASE`, `REDSHIFT_IAM_ROLE` | — | load.py |

### 14.2 Committed config

`recipes/*.json` + `recipes/schema.json`; `spark/config/{ports.csv,
container_aliases.csv, fx_rates.csv, rules.yaml, source_priority.yaml}`;
`deploy/helm/rate-pipeline/values*.yaml`; `deploy/kind/cluster.yaml`.

### 14.3 `.gitignore`

`.env`, `.env.*`, `!.env.example`, `data/`, `dist/`, `*.whl`, `.venv/`,
`__pycache__/`, `.pytest_cache/`, `.ruff_cache/`, `spark-warehouse/`,
`metastore_db/`, `derby.log`, `.terraform/`, `*.tfstate*`, `bench/tmp/`,
`.databricks/`, `coverage.out`, `*.prof`.

## 15. Repository layout

```
logistics-rate-pipeline/
├── README.md                      ← §21.1 outline
├── LICENSE                        ← MIT
├── Makefile                       ← every recurring command (D-37)
├── go.mod / go.sum                ← go 1.27
├── Dockerfile / .dockerignore
├── .gitattributes / .gitignore / .env.example
├── .golangci.yml                  ← v2 config
├── .github/workflows/ci.yml       ← D-36
├── docs/
│   ├── PLAN.md                    ← this file
│   ├── architecture.md            ← §5 diagram, contracts, D-11/D-14/D-34 reasoning
│   ├── sources.md                 ← the eight feeds and their trap rows
│   ├── runbook-eks.md             ← Phase 7 timestamps + cost
│   └── plan-explain.txt           ← one committed Spark physical plan
├── recipes/                       ← 8 recipes + schema.json
├── cmd/ingestd/, cmd/mocksources/
├── internal/{recipe,source,parse,record,sink,pool,server,metrics,shutdown}/
├── scripts/bootstrap-wsl.sh, bootstrap-macos.sh, secret-scan.sh
├── deploy/
│   ├── kind/cluster.yaml
│   ├── helm/rate-pipeline/        ← §9.3
│   ├── terraform/eks/             ← §9.4
│   ├── databricks/                ← §12
│   └── redshift/                  ← §11
├── spark/
│   ├── pyproject.toml             ← package rate_normalizer, ruff, pytest
│   ├── src/rate_normalizer/{__main__.py, io.py, stages.py, rules.py, metrics.py, schema.py}
│   ├── config/                    ← §14.2
│   └── tests/{unit/, fixtures/raw/, fixtures/expected_counts.json}
├── bench/results/                 ← committed numbers + LATEST.md
└── tests/e2e/                     ← kind scripts used by CI's kind-e2e job and by make
```

## 16. Toolchain and pins

| Tool | Version | Install (WSL Ubuntu 24.04) |
|---|---|---|
| Go | **1.27.x** (latest patch at Phase 0, recorded) | official tarball to `/usr/local/go` |
| golangci-lint | v2 latest at Phase 0 | official install script |
| Docker Engine | latest from Docker's apt repo | `docker-ce`, user in `docker` group |
| kind | **v0.33.0** | GitHub release binary |
| kubectl | matching kind's default (1.36.x) | `dl.k8s.io` |
| Helm | **4.2.x** | `get-helm-4` script |
| kubeconform | latest | GitHub release |
| Terraform | latest 1.x at Phase 7 (recorded) | HashiCorp apt repo |
| AWS CLI v2 | latest | official installer (WSL copy; the Windows one is not used) |
| Java | OpenJDK **17** headless | apt |
| Python | 3.12 (Ubuntu 24.04 default) | apt + `venv` |
| PySpark | **4.2.0** | pip, pinned in `spark/pyproject.toml` |
| Databricks CLI | latest | official installer |
| gh | latest | GitHub apt repo |

Go module pins are whatever `go get` resolves at Phase 1 and are frozen
in `go.sum`; Python pins go into `spark/requirements.lock` at Phase 6.
Appendix A records the exact versions the first time each is installed.

## 17. Testing and CI

### 17.1 Go (`go test -race ./...`, no network, no cloud)

| Package | Asserts |
|---|---|
| `recipe` | every committed recipe validates; each schema violation type is rejected with a path in the error |
| `source` | pagination (page, cursor, until-empty); retry count and backoff schedule under `synctest` (deterministic jitter); `Retry-After` seconds and HTTP-date; no retry on 4xx; rate limit spacing; auth header present only when configured; context cancellation aborts a sleeping backoff immediately |
| `parse` | CSV with quotes/CRLF/BOM; EU/US decimals; gzip streaming; JSON `records_path` incl. nested arrays; unparseable price → null not error; `BenchmarkParseCSV` on the fathom fixture |
| `record` | `record_hash` stable across field order and independent of `fetched_at`/`run_id` |
| `sink/fs` | temp→rename promotion; abort leaves nothing promoted; manifest last |
| `sink/s3` | same against `gofakes3`; `_tmp/` cleanup; `gc` |
| `pool` | `workers=1` vs `8` identical record sets; a failing source does not fail the run; cancellation mid-run → no manifest, all temp aborted; **goleak** in every test; bounded channel backpressure (a slow sink does not grow memory — asserted via a counting fake) |
| `server` | `/readyz` false before recipes load and during shutdown; `POST /runs` rejects when shutting down; metrics registered once |
| `shutdown` | SIGTERM → cancel → wait ≤ grace → exit code 0 (subprocess test on the built binary) |
| `cmd/mocksources` | determinism: two `dump`s are byte-identical; row totals equal §6.2 |

### 17.2 Spark (`pytest`, `local[2]`, Java 17)

One session-scoped `SparkSession`; tests for each stage in §10.2 and
each rule in §10.3 (pass + fail); the full fixture produces
`expected_counts.json` exactly; dedup keeps the highest-priority/latest
row on a crafted tie; output schema matches `schema.py`; the D-26 `ast`
purity test; `run_metrics.json` shape.

### 17.3 Helm / Kubernetes

`helm lint`, `helm template` + `kubeconform -strict`, and in
`kind-e2e`: create cluster, load images, install chart with
`values-kind.yaml`, `helm test`, then the graceful-shutdown script
(`tests/e2e/shutdown.sh`) asserting no manifest and no promoted parts
after a mid-run pod delete.

### 17.4 CI matrix (D-36)

`go` · `spark` · `helm` · `docker` on every push/PR; `kind-e2e` on
`main` and label; GHCR push on `main` only. No cloud credentials in
CI; nothing in CI talks to AWS or Databricks.

## 18. Phases with acceptance checks

Sessions are the 09:00–11:00 daily upskilling block (2 h) **after
Project 1 Phase 8 is ticked** (A17); Phase 0 may run in evenings before
that. Do phases in order; tick the box **and** write the progress-log
line in the vault (Project 2 note) before starting the next. Every
session gets a row in Appendix A. Machine: WSL on the Windows laptop
unless stated.

### Phase 0 — Machine bootstrap, repo scaffold, cloud account check (2 sessions, may be evenings)

- [ ] `.wslconfig` written; Ubuntu 24.04 installed to `D:\wsl\ubuntu`
      (Appendix C.1); `free -h` inside WSL shows ≤ 5 GB
- [ ] `scripts/bootstrap-wsl.sh` written and run: Docker Engine, Go
      1.27, golangci-lint, kind, kubectl, Helm 4, kubeconform, Java 17,
      Python venv, AWS CLI, gh, make, jq — versions recorded in Appendix A
- [ ] `kind create cluster` + `kubectl get nodes` Ready + `kind delete
      cluster` once, with `free -h` before/after recorded
- [x] (local commits only — no GitHub push yet, `gh` not installed) Repo scaffold: `go.mod`, `Makefile`, `.gitattributes`, `.gitignore`,
      `.env.example`, `LICENSE`, `README.md` stub, this file at
      `docs/PLAN.md`, CI workflow with the `go` job only; `gh repo create
      subburajan-perumal/logistics-rate-pipeline --private`; push
- [ ] Windows mirror: `git clone` into `D:\projects\logistics-rate-pipeline`
      (replacing the plan-only directory this file was written in)
- [ ] AWS: console login; old access key deleted; IAM user `rates-dev`
      with a policy limited to S3 (one bucket), Redshift Serverless,
      Redshift Data API, EKS/EC2/IAM as needed for Terraform (scoped by
      tag `project=logistics-rate-pipeline`); new key in WSL `~/.aws`;
      `aws sts get-caller-identity` works; **AWS Budget $5 alert**
      created; account plan type (legacy / Free / Paid) recorded
- [ ] Bucket `logistics-rate-pipeline-<account-suffix>` created, private,
      lifecycle rule expiring `_tmp/` after 1 day and `runs/` after 30 days
- [ ] Secret scan script (`scripts/secret-scan.sh`: `git log -p --all |
      grep -E "AKIA|ASIA|dapi|ghp_"`) runs clean

**Acceptance:** WSL toolchain complete and recorded; kind proven on this
machine; repo private on GitHub with green `go` job; AWS access
restored with a budget alarm; bucket exists.

### Phase 1 — mocksources, recipes, raw contract, fs sink, sequential run (2 sessions)

- [x] `internal/record` types + hashing + tests
- [x] `cmd/mocksources` with all eight feeds per §6.2, seeded, `--dump`;
      determinism test; totals test
- [x] `recipes/schema.json` + 8 recipes; `internal/recipe` loader + tests
- [x] (D-44) `internal/parse` CSV + JSON (json/v2 streaming) + tests + benchmark
- [x] `internal/source` fetcher with pagination and auth (retry in
      Phase 2) + tests
- [x] `internal/sink/fs` + tests; `ingestd run --workers 1 --sink fs`
      lands a complete run with manifest against a local `mocksources`
- [x] (D-39 counts) `mocksources dump` → `spark/tests/fixtures/raw/` committed;
      `expected_counts.json` derived and committed

**Acceptance:** a sequential run lands 20 810 records + manifest; every
package above has tests; fixture committed.

### Phase 2 — Worker pool, cancellation, retry, S3 sink, benchmark (3 sessions)

- [x] `internal/pool` per §8.2 with errgroup fan-out, per-source
      pipeline, fan-in; goleak in all tests; `workers=1` ≡ `workers=8`
- [x] (fake-clock tests instead of synctest) Retry/backoff/`Retry-After`/rate-limit per D-13 with `synctest`
      tests; `delphine` completes with retries counted in the manifest
- [x] (drain tested in-process; the SIGTERM-on-binary path is exercised by tests/e2e/shutdown.sh on kind) `internal/shutdown` + `serve` mode SIGTERM test on the binary
- [x] `internal/sink/s3` against `gofakes3`; `gc` subcommand
- [x] (1.82×, D-40) `ingestd bench` implemented; **benchmark run committed**
      (`bench/results/<ts>-pool.json/.md`) with `pool_speedup` and peak
      RSS per worker count
- [ ] Real S3: `ingestd run --sink s3` lands one run in the bucket;
      `aws s3 ls` output pasted into Appendix A

**Acceptance:** `go test -race ./...` clean with goleak; benchmark
committed showing ≥ 4× (if lower, the number is still committed and a
Decision Log row explains); one real run in S3.

### Phase 3 — Observability and profiling (1 session)

- [x] `slog` attributes per §8.6; Prometheus metrics; `/healthz`,
      `/readyz` semantics incl. shutdown flip; `/debug/pprof` with
      `goroutineleak`
- [x] (−64 % allocs) CPU + alloc profile of `BenchmarkParseCSV`; one optimization;
      before/after committed to `bench/results/<ts>-parse.md` with the
      flame-graph text summary (`go tool pprof -top`)
- [ ] `curl /debug/pprof/goroutineleak?debug=1` after a run shows 0
      leaked goroutines — output committed

**Acceptance:** metrics and pprof live; optimization documented with
numbers; leak profile empty.

### Phase 4 — Images, kind, raw manifests (2 sessions)

- [x] `Dockerfile` per D-21; both images < 20 MB — built and pushed by the CI `docker` job on 2026-09-17: `ingestd` 7.8 MB, `mocksources` 5.5 MB compressed (GHCR amd64 manifests; result file still to commit)
- [~] `deploy/kind/cluster.yaml`, `make kind-up/kind-down`; ingress-nginx
      up — cluster config proven by the CI `kind-e2e` job (kind 0.33, K8s 1.36.1, ingress disabled there); not yet run on the laptop
- [ ] Raw manifests (before Helm) for every D-22 object applied by hand
      once, so each object is understood individually; `kubectl
      describe` outputs for the Deployment saved to `docs/` as a learning
      artifact (short)
- [ ] `POST /runs` through the Ingress lands a run on the PVC; `kubectl
      top pod` peak recorded

**Acceptance:** the service runs on kind with probes, limits, ConfigMap
recipes, Secret, PVC, Service, Ingress; a run completes via the Ingress.

### Phase 5 — Helm chart, in-cluster tests, CI e2e (2 sessions)

- [x] Chart per §9.3; `helm lint` + `kubeconform` in CI; `helm test`
      passes on kind (CI run 35255407983, 2026-09-17: Phase Succeeded, in-cluster run with records > 0)
- [~] Config-rollout test and graceful-shutdown test (§9.5) executed and
      recorded with timestamps; `tests/e2e/shutdown.sh` scripted — shutdown test PASS in CI 2026-09-17 (pod gone in 1 s, run `20260917T175531Z-3712e6` left no manifest/parts); config-rollout test still to do
- [x] `kind-e2e` CI job green on `main` (2026-09-17, after three fixes — see Appendix A); GHCR images published
- [ ] Raw manifests from Phase 4 deleted (chart is the source of truth)

**Acceptance:** one `helm upgrade --install` deploys everything; `helm
test` and the shutdown e2e pass locally and in CI.

### Phase 6 — PySpark normalizer, local (3 sessions)

- [x] (on the Windows host, D-41/D-48) `spark/` package, venv, `pyspark==4.2.0`, Java 17 in WSL;
      `pytest` skeleton with the session fixture
- [x] Stages §10.2 + rules §10.3 + tests; `ast` purity test;
      `expected_counts.json` matched exactly
- [ ] `python -m rate_normalizer` over the Phase 2 real S3 run (mirrored
      with `aws s3 sync`) → Parquet + rejects + `run_metrics.json`;
      metrics committed to `bench/results/<ts>-normalize.json`
- [x] (CI written; 25 tests green locally; CI runs after the GitHub push) `docs/plan-explain.txt` committed; `spark` CI job green
- [ ] End-to-end on the laptop: `make kind-up` → run → `make kind-down`
      → Spark over the PVC output (copied out with `kubectl cp`) — the
      one time both run in one session, sequentially

**Acceptance:** all rules tested; committed run metrics match §13
expectations; CI green.

### Phase 7 — EKS via Terraform, one 3-hour window (1–2 sessions; the second only if the first is cut short)

- [x] (written, not yet validated — Terraform not installed here) Terraform per §9.4 written and `terraform validate`d **before** the
      window (session 1 may be entirely this)
- [ ] Window: `apply` (start time logged) → `aws eks update-kubeconfig`
      → ingress-nginx → `helm upgrade --install … -f values-eks.yaml` →
      run via Ingress → `aws s3 ls` shows the run → `kubectl top` → mid-run
      delete test once → `helm uninstall` → `terraform destroy` (end
      time logged); everything in `docs/runbook-eks.md`
- [ ] Next-day bill for the tag recorded in Appendix A and §20

**Acceptance:** a run landed in S3 from EKS with Pod Identity (no keys
in the cluster); cluster destroyed the same day; cost recorded.

### Phase 8 — Databricks Free Edition (2 sessions)

- [ ] Account, LinkedIn verification, CLI auth; `workspace.rates` schema
      and `raw` Volume created
- [ ] External-location attempt (§12) — result logged as a Decision Log
      row
- [ ] Bundle per §12: `validate`, `deploy`, `run` over the synced run →
      Delta tables; `MERGE` idempotency shown by a second run; `DESCRIBE
      HISTORY` committed
- [ ] Parquet copy written to the Volume for the Redshift path (or read
      directly from S3 if the external location worked)

**Acceptance:** the same wheel runs unchanged on serverless; Delta
tables exist; bundle files committed.

### Phase 9 — Redshift Serverless load and end-to-end latency (1–2 sessions)

- [ ] `create.sh` (4/4 RPU, IAM role, database `rates`), `ddl.sql`
      applied via Data API
- [ ] `load.py` loads the run; `verify.sql` output committed; second load
      unchanged (idempotency)
- [ ] One fresh **timed** end-to-end run: kind (or the Phase 7 EKS run's
      manifest timestamps) → S3 → Spark → `aws s3 cp` → `COPY` →
      `bench/results/<ts>-e2e.json`
- [ ] `destroy.sh`; next-day bill recorded

**Acceptance:** `rate_matrix` queryable in Redshift with the expected
row count; idempotency shown; `e2e_latency` committed; Redshift deleted.

### Phase 10 — Docs and the 15-minute run (1 session)

- [x] (numbers from bench/results/*) README per §21.1 with real numbers copied from `bench/results/LATEST.md`
- [x] `docs/architecture.md`, `docs/sources.md` match the code
- [ ] Fresh clone on the **Mac**: `scripts/bootstrap-macos.sh`, `make
      kind-up deploy-kind run-kind`, `make spark-local` — timed, following
      only the README; must be ≤ 15 minutes excluding downloads

**Acceptance:** fresh-clone kind + Spark run ≤ 15 min; README numbers
match committed results.

### Phase 11 — Publish and promote (1 session)

- [ ] `scripts/secret-scan.sh` clean on full history; `.env*` absent
- [ ] Repo → public; topics `go`, `kubernetes`, `helm`, `pyspark`,
      `databricks`, `redshift`, `terraform`, `eks`, `concurrency`
- [ ] Vault: `4 Career Profile/Project - Concurrent Go Ingestion Pipeline
      on Kubernetes with PySpark and Redshift.md` (metric, artifact link,
      one STAR line) passes the promotion checklist
- [ ] Master resume gains the three §1 bullets with real numbers
- [ ] `2 Upskilling/_Overview.md` rows for Kubernetes, Go, PySpark →
      `practicing`; skill notes' progress logs updated
- [ ] LinkedIn post drafted in Personal Branding Strategy (numbers first,
      wording rules, posted by the user)
- [ ] Playbook: `2 Upskilling/Playbooks/Go Concurrency Patterns - Ingestion
      Worker Pool.md` written from `internal/pool` (the vault's Go note
      promised this)

**Acceptance:** everything in §22 ticked.

**Session budget:** 0 ≈ 2 (evenings), 1–3 ≈ 6, 4–5 ≈ 4, 6 ≈ 3, 7–9 ≈ 4,
10–11 ≈ 2 → **~21 two-hour sessions**. At one session per weekday from
**2026-10-12** that is ~4.5 weeks; target finish **2026-11-13**, hard
stop for the vault's tracking **2026-11-20**.

## 19. Risks

| Risk | Decision already made |
|---|---|
| 8 GB RAM cannot hold kind + Spark + browser (A1) | WSL capped at 5 GB with swap on D:; kind single-node; kind deleted after every session; Spark `local[2]`/1 GB; the only co-run is Phase 6's sequential e2e; if even that fails, the e2e is split across two sessions with the PVC output copied out first |
| WSL distro on D: is slow or flaky | Symptoms logged; fallback is the Mac for everything from Phase 4 on (Appendix C.2) |
| AWS account is closed / cannot be reactivated (A6) | Phase 0 finds out first; fallback is a **new** account on the Free plan ($100 credits) — Redshift trial then unavailable but § 20 already assumes no credits |
| Redshift Serverless minimum in the account/region turns out to be 8 RPU | Cost doubles to $3/h; sessions shortened to 1 h; still within the $10 cap |
| Databricks Free Edition refuses wheel tasks or external locations (A10) | Documented fallbacks in D-32 / §12; either outcome is a logged decision, not a blocker |
| Free Edition outbound-internet restriction blocks `pip`/S3 | LinkedIn verification in Phase 8 first task; Volume upload path needs no outbound access |
| EKS window overruns or `destroy` fails | Runbook has a hard stop at 2 h 30 for teardown; `terraform destroy` retried; a manual checklist (NLB, node group, cluster, VPC) in the runbook; Budget alarm at $5 catches a leak by the next day |
| `pool_speedup` comes in under 4× | The number is committed anyway with the cause (likely CPU-bound `fathom` parse dominating); no re-tuning of `mocksources` latency to hit a target — that would be dishonest and the README says the latency profile is fixed |
| Go 1.27 `goroutineleak` misses a leak class (documented limitation) | `goleak` in tests is the primary guard; the profile is the live demo |
| Spark 4.2 local vs. Databricks serverless behavioural drift | DataFrame-only + purity test (D-26); the Databricks run must reproduce `expected_counts.json` — a diff is a bug, not a note |
| Two machines drift | `.gitattributes`, bootstrap scripts, `go.sum`, `requirements.lock`, Phase 10 fresh-clone test on the Mac |
| Reviewer thinks the feeds/carriers are real | Invented names, `docs/sources.md` and README say synthetic in the first screen; `mocksources` is in the repo |
| Scope creep (streaming, HPA, dashboards) | §4 non-goals; deferred items go to the vault roadmap notes |
| Plan drift | Decision Log append-only; a change without a row is a bug |

## 20. Cost budget

Assumes **no credits** (A8). Hard cap **$10**; AWS Budget alarm at $5.

| Item | Rate | Planned usage | Est. |
|---|---|---|---|
| S3 | $0.023/GB-mo + requests | < 1 GB, < 10 k requests | < $0.10 |
| EKS control plane | $0.10/h | 3 h | $0.30 |
| EC2 `t3.small` on-demand | ≈ $0.021/h | 3 h | $0.07 |
| NLB (ingress-nginx) | ≈ $0.0225/h + LCU | 3 h | $0.10 |
| Redshift Serverless | $0.375/RPU-h × 4 | ≤ 4 h active | ≤ $6.00 (or $0 under the trial) |
| Redshift storage | $0.024/GB-mo | negligible, deleted | ≈ $0 |
| Databricks Free Edition | $0 | | $0 |
| GHCR, GitHub Actions (public repo) | $0 | | $0 |
| **Total** | | | **≤ $6.60** |

Every AWS resource carries `project=logistics-rate-pipeline`; the
post-Phase-9 bill for that tag goes into Appendix A.

## 21. Publishing — README, LinkedIn, Career Profile

### 21.1 README outline (60-second skim first, 15-minute run second)

1. One paragraph: eight synthetic carrier feeds → Go worker pool → S3 →
   PySpark → Redshift/Delta; **the numbers table** (`pool_speedup`,
   records/run, leaks 0, image size, rows out / dedup / rejects,
   e2e latency) with run ids.
2. Architecture diagram (§5) and the layer-contract table.
3. "What each layer proves" — three short subsections mapping to the
   three resume bullets, each linking the code path and the committed
   result file.
4. Run it locally (kind path): bootstrap script, `make kind-up
   deploy-kind run-kind`, `make spark-local`; then the cloud paths
   (EKS, Databricks, Redshift) as separate, optional sections with cost
   notes.
5. Design notes: why ingestion stays faithful (D-11), atomic runs
   (D-14), why a Deployment not a CronJob (D-17), why no UDFs (D-26),
   why static FX (D-29), DISTKEY/SORTKEY reasoning (D-34), what is
   synthetic (D-07).
6. Links: `docs/architecture.md`, `docs/sources.md`, `docs/PLAN.md`,
   `bench/results/`.

### 21.2 LinkedIn post shape

Numbers first ("Same eight feeds, same laptop. One worker: 31 s. Eight
workers: 7 s. Zero goroutine leaks, zero records lost when Kubernetes
killed the pod mid-run."), one sentence per layer, repo link. Follows
the vault's wording rules; drafted in the vault, posted by the user.

### 21.3 Career Profile note

`4 Career Profile/Project - Concurrent Go Ingestion Pipeline on
Kubernetes with PySpark and Redshift.md`: the three metrics, the repo
URL, one STAR line per layer, links to the playbook and this plan.

## 22. Definition of done

- [ ] All Phase 0–11 acceptance checks ticked
- [ ] `bench/results/` contains: pool benchmark, parse before/after,
      image sizes, kind shutdown test log, normalize metrics, Delta
      history, Redshift verify output, e2e latency — each with a run id
- [ ] `pool_speedup ≥ 4×` (or the committed number with its Decision Log
      explanation), `goroutine_leaks = 0`, `shutdown_records_lost = 0`,
      `image_size_mb < 20`, `e2e_latency < 5 min`, `redshift_idempotent`
      pass, `cloud_cost_usd ≤ 10`
- [~] Repo public, CI green including `kind-e2e` (both 2026-09-17); README shows the
      numbers table with run ids — cloud numbers still missing
- [ ] EKS destroyed, Redshift deleted, S3 lifecycle rules in place — the
      project's standing AWS cost is **$0**
- [ ] Career Profile note written and passes the promotion checklist
- [ ] Master resume has the three bullets with real numbers
- [ ] Upskilling tracker rows moved to `practicing`; three skill notes'
      progress logs updated
- [ ] LinkedIn post drafted
- [ ] Go concurrency playbook written
- [ ] Vault build-plan note and this file agree (the vault note links
      here as the frozen spec)

---

## Appendix A — Session log

| Date | Machine | Phase | Done | Result / numbers | Next |
|---|---|---|---|---|---|
| 2026-09-17 | Windows | plan | Audited the machine (RAM, disk, WSL, toolchain, AWS CLI), the vault's roadmaps/interview notes, and current docs for Go 1.27, kind 0.33, Helm 4, PySpark 4.2, Databricks Free Edition, Redshift Serverless, EKS, AWS Free Tier, MinIO/LocalStack status; wrote this plan | Findings A1–A18; decisions D-01–D-38; no code, no installs | Phase 0 (evenings) once Project 1 is past Phase 8 |
| 2026-09-17 | Windows (host toolchain, D-41) | 0–3, 6 built and tested; artifacts for 4, 5, 7, 8, 9, 10 written | Installed Go 1.27.0, Temurin 17.0.20, Helm 4.3.0, WinLibs gcc 16.1; wrote all Go packages + tests, `mocksources`, 8 recipes, the PySpark package + 25 tests, Dockerfile, kind config, Helm chart, CI workflow, Makefile, bootstrap scripts, Terraform, bundle, Redshift DDL/loader, README/architecture/sources/runbook docs | `go test -race ./...` clean (8 pkgs, goleak); fixture run 20 810 records, 8/8 sources; pool bench 6.85 s → 3.77 s (1.82×, D-40); parse profile −64 % allocs; Spark 20 810 → 2 294 rows, 66 rejects/7 reasons, 18 450 dedup, exact-count test green; `helm lint` + `helm template` clean for kind and EKS profiles. **Not run here:** Docker/kind/`helm test`/shutdown e2e (no Docker), GitHub push + CI (no `gh`), S3/EKS/Databricks/Redshift (dead AWS key, no accounts) | Phase 0 remainder: WSL + Docker, `gh repo create` + push, AWS key + budget + bucket; then Phases 4/5 on kind, 7–9 cloud, 10 on the Mac, 11 |
| 2026-09-17 | Windows + GitHub Actions | 11.5 showcase + CI fix; **4/5 first executed in CI** | Repo pushed public; GitHub Pages serving `docs/index.html`; Streamlit Community Cloud app `logistics-rate-pipeline` deployed from `demo/app.py` (two fixes: pyarrow ≥ 22 for Python 3.14, un-ignore `demo/data`); first public CI run red on `golangci-lint` → 13 errcheck / 2 gocritic / 1 ineffassign / 1 staticcheck fixed in code, `bodyclose` scoped out for the fake-response test file (D-50); CI badge on README and results page; `setup-java` v4 → v5. With `go` green the `docker` and `kind-e2e` jobs ran for the first time: pinned kind v0.33.0 (action default 0.31 wrote v1beta3 kubeadm config), dropped `hook-succeeded` from the test pod so `helm test --logs` can read it, dropped the `latencyScale=0.2` override so the shutdown test deletes the pod mid-run deterministically | https://subburajan-perumal.github.io/logistics-rate-pipeline/ live · https://logistics-rate-pipeline.streamlit.app live (20 810 / 2 294 / 18 450 / 66 shown) · `golangci-lint run` 0 issues · `go test -race ./...` clean · **CI run 35255407983 all green**: images built and pushed to GHCR (`ingestd` 7.8 MB, `mocksources` 5.5 MB compressed, amd64), kind 0.33 / K8s 1.36.1 cluster up, `helm upgrade --wait` healthy, `helm test` Phase Succeeded (in-cluster run with records > 0), shutdown e2e PASS: pod gone in 1 s of a 60 s grace, interrupted run `20260917T175531Z-3712e6` left no manifest and no promoted parts | Phase 0 remainder unchanged (WSL + Docker locally, AWS key + budget + bucket); commit an image-size result file from the GHCR manifests so the results page can show it; peak-memory number from a kind run still open; Grafana Cloud dashboard when a kind run is done locally |

## Appendix B — Sources checked on 2026-09-17

- Go 1.27 release notes (generic methods, `encoding/json` v2 default,
  `goroutineleak` profile GA, `synctest.Sleep`, macOS 13 minimum):
  https://go.dev/doc/go1.27 · https://go.dev/blog/go1.27
- kind releases (v0.33.0, default `kindest/node:v1.36.1`, v1beta4):
  https://github.com/kubernetes-sigs/kind/releases ·
  https://kind.sigs.k8s.io/docs/user/quick-start/
- Helm 4 released (Nov 2025) and Helm 3 end of life (last minor 3.22.0
  on 2026-09-10; security fixes end 2027-02-10):
  https://helm.sh/blog/helm-4-released/ · https://helm.sh/blog/helm-v3-end-of-life/
- PySpark 4.2.0 installation (Python ≥ 3.10, Java 17+, extras,
  `pyspark-client`): https://spark.apache.org/docs/latest/api/python/getting_started/install.html
- Databricks Free Edition limitations (serverless only, 5 concurrent
  tasks, 2X-Small warehouse, no Scala/R, outbound restricted until
  LinkedIn verification):
  https://docs.databricks.com/aws/en/getting-started/free-edition-limitations
- Databricks serverless environment versions (v6 on 2026-09-03, Python
  3.12.3, Spark Connect client API):
  https://docs.databricks.com/aws/en/release-notes/serverless/environment-version/
- Databricks community: Free Edition cannot use `_jsc` for S3 keys;
  Volumes recommended:
  https://community.databricks.com/t5/data-engineering/databricks-free-edition-accessing-files-in-s3/td-p/142719
- Databricks community: Unity Catalog preconfigured in Free Edition
  (`workspace` catalog):
  https://community.databricks.com/t5/databricks-free-edition-help/unity-catalog/td-p/144526
- Databricks community: bundles on Free Edition with `serverless: true`:
  https://community.databricks.com/t5/get-started-discussions/free-edition-and-databricks-asset-bundles/td-p/122137
- Declarative Automation Bundles (formerly Asset Bundles):
  https://docs.databricks.com/aws/en/dev-tools/bundles/
- Amazon Redshift pricing (Serverless $0.375/RPU-h us-east-1, 4–1024
  RPU, 60 s minimum, $300/90-day trial, $0.024/GB-mo storage):
  https://aws.amazon.com/redshift/pricing/
- AWS Free Tier since 2025-07-15 (credits, Free vs Paid plan; service
  trials such as Redshift Serverless are Paid-plan only):
  https://aws.amazon.com/about-aws/whats-new/2025/07/aws-free-tier-credits-month-free-plan/ ·
  https://aws.amazon.com/free/free-tier-faqs/
- EKS pricing ($0.10/h standard, $0.60/h extended, Auto Mode ~12 % on
  EC2): https://www.cloudzero.com/blog/eks-pricing/ ·
  https://aws.amazon.com/pt/blogs/containers/amazon-eks-extended-support-for-kubernetes-versions-pricing/
- MinIO open-source archived (Feb 2026), no images since Oct 2025:
  https://itsfoss.com/news/minio-moves-away-from-open-source/ ·
  https://linuxiac.com/minio-ends-active-development/
- LocalStack Community discontinued 2026-03-23, non-commercial tier
  requires registration: https://www.srvrlss.io/provider/localstack/ ·
  https://agentdeals.dev/vendor/localstack
- Machine audit commands and outputs are in the vault practice-log
  entry for 2026-09-17 (Project 2 plan).

## Appendix C — Machine bootstrap, exact commands

### C.1 Windows: WSL distro on D: (run once, PowerShell as the user)

```powershell
# 1. Memory/swap cap — file: C:\Users\Subbu\.wslconfig
@"
[wsl2]
memory=5GB
processors=6
swap=8GB
swapFile=D:\\wsl\\swap.vhdx
localhostForwarding=true
"@ | Out-File -Encoding ascii $env:USERPROFILE\.wslconfig

# 2. Distro on D:
New-Item -ItemType Directory -Force D:\wsl | Out-Null
wsl --install Ubuntu-24.04 --location D:\wsl\ubuntu --no-launch
wsl --set-default Ubuntu-24.04
wsl -d Ubuntu-24.04            # first launch: create user 'subbu'
```

Inside the distro, `/etc/wsl.conf`:

```ini
[boot]
systemd=true
[automount]
options="metadata"
```

then `wsl --shutdown` from PowerShell and relaunch.

### C.2 `scripts/bootstrap-wsl.sh` (idempotent; the committed script is the source of truth — this is its outline)

```bash
set -euo pipefail
sudo apt-get update && sudo apt-get install -y ca-certificates curl gnupg make jq unzip git openjdk-17-jre-headless python3-venv python3-pip
# Docker Engine (docker.com apt repo), user in docker group
# Go 1.27.x tarball → /usr/local/go; PATH in ~/.profile
# golangci-lint v2 install script; kind v0.33.0; kubectl 1.36.x; helm 4 (get-helm-4); kubeconform
# AWS CLI v2 (Linux installer); gh (GitHub apt repo); Databricks CLI; Terraform (HashiCorp apt repo)
# python3 -m venv ~/.venvs/rates && pip install -e spark[dev]
# Print every version → paste into Appendix A
```

`scripts/bootstrap-macos.sh` mirrors it with Homebrew (`brew install go
kind kubectl helm kubeconform awscli gh terraform openjdk@17
databricks`) and **Colima** (`brew install colima docker && colima start
--memory 4`) instead of Docker Desktop.

### C.3 Daily session start / end

```bash
make kind-up        # only for Kubernetes sessions
…
make kind-down      # always, before closing the laptop
free -h             # sanity; note in the session row if swap was used
```
