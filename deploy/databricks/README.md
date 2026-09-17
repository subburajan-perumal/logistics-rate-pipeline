# Databricks Free Edition — layer 3 on serverless

Not runnable from CI (needs a workspace token). Steps for Phase 8 of
`docs/PLAN.md`:

1. Free Edition account → verify with LinkedIn (lifts the outbound-internet
   restriction) → `databricks configure` with a PAT.
2. One-time in a SQL editor:
   ```sql
   CREATE SCHEMA IF NOT EXISTS workspace.rates;
   CREATE VOLUME IF NOT EXISTS workspace.rates.raw;
   CREATE VOLUME IF NOT EXISTS workspace.rates.normalized;
   CREATE VOLUME IF NOT EXISTS workspace.rates.config;
   ```
3. Upload a run and the reference tables (the D-32 fallback path — try the
   external location first and log the outcome in the Decision Log):
   ```bash
   aws s3 sync s3://<bucket>/runs/run_id=<id> data/raw/runs/run_id=<id>
   databricks fs cp -r data/raw/runs/run_id=<id> dbfs:/Volumes/workspace/rates/raw/runs/run_id=<id>
   databricks fs cp -r spark/config              dbfs:/Volumes/workspace/rates/config
   ```
4. `databricks bundle validate -t dev && databricks bundle deploy -t dev`
5. `databricks bundle run rate_normalizer_job -t dev --params run_id=<id>`
6. Verify idempotency: run it twice, then
   `SELECT COUNT(*) FROM workspace.rates.rate_matrix WHERE run_id = '<id>'`
   is unchanged and `DESCRIBE HISTORY workspace.rates.rate_matrix` shows the
   second MERGE touching 0 rows. Save that output under `bench/results/`.

If `python_wheel_task` is refused on Free Edition, switch the task to a
notebook that runs `%pip install /Workspace/.../rate_normalizer-*.whl`
followed by `from rate_normalizer.__main__ import main; main([...])`, and
add a Decision Log row.
