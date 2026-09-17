-- Redshift Serverless schema (docs/PLAN.md D-34).
-- lane_id as DISTKEY co-locates every row of a lane for lane-level joins and
-- aggregations; the compound SORTKEY matches the dominant filter
-- (carrier, then validity). The audit table is tiny → DISTSTYLE ALL.
CREATE SCHEMA IF NOT EXISTS rates;

CREATE TABLE IF NOT EXISTS rates.rate_matrix (
  run_id             VARCHAR(64)   NOT NULL,
  carrier            VARCHAR(32)   NOT NULL,
  lane_id            VARCHAR(16)   NOT NULL,
  origin_locode      VARCHAR(8)    NOT NULL,
  destination_locode VARCHAR(8)    NOT NULL,
  container_type     VARCHAR(8)    NOT NULL,
  price_usd          DECIMAL(12,2) NOT NULL,
  base_price         DECIMAL(14,2) NOT NULL,
  base_currency      VARCHAR(3)    NOT NULL,
  surcharges_usd     DECIMAL(12,2) NOT NULL,
  all_in_usd         DECIMAL(12,2) NOT NULL,
  unit_source        VARCHAR(16)   NOT NULL,
  valid_from         DATE          NOT NULL,
  valid_to           DATE          NOT NULL,
  source_id          VARCHAR(32)   NOT NULL,
  fetched_at         TIMESTAMP,
  record_hash        VARCHAR(64)   NOT NULL,
  normalized_at      TIMESTAMP
)
DISTSTYLE KEY DISTKEY (lane_id)
COMPOUND SORTKEY (carrier, valid_from);

CREATE TABLE IF NOT EXISTS rates.rejects (
  run_id             VARCHAR(64) NOT NULL,
  source_id          VARCHAR(32) NOT NULL,
  carrier            VARCHAR(32),
  reject_reason      VARCHAR(32) NOT NULL,
  origin_raw         VARCHAR(128),
  destination_raw    VARCHAR(128),
  container_type_raw VARCHAR(32),
  price_raw          VARCHAR(64),
  currency           VARCHAR(16),
  unit               VARCHAR(16),
  valid_from_raw     VARCHAR(32),
  valid_to_raw       VARCHAR(32),
  record_hash        VARCHAR(64),
  page               INTEGER,
  row_index          INTEGER
)
DISTSTYLE EVEN;

CREATE TABLE IF NOT EXISTS rates.load_runs (
  run_id      VARCHAR(64) NOT NULL,
  loaded_at   TIMESTAMP   NOT NULL DEFAULT GETDATE(),
  rows_loaded BIGINT      NOT NULL,
  rejects     BIGINT      NOT NULL,
  status      VARCHAR(16) NOT NULL,
  source_uri  VARCHAR(512)
)
DISTSTYLE ALL;
