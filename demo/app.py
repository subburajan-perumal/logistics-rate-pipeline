"""Rate-matrix explorer — a read-only view over one committed run of the
PySpark normalizer (`demo/data/`, produced from the `fixture` raw run).

Deliberately pandas/pyarrow, not PySpark: the job's *output* is what a
non-engineer wants to see, and Spark does not fit a free Streamlit dyno.
The pipeline itself is untouched by this file.
"""

import json
from pathlib import Path

import pandas as pd
import streamlit as st

DATA = Path(__file__).parent / "data"
REPO = "https://github.com/subburajan-perumal/logistics-rate-pipeline"

st.set_page_config(page_title="logistics-rate-pipeline — rate matrix explorer", layout="wide")


@st.cache_data
def load():
    rates = pd.read_parquet(DATA / "rate_matrix.parquet")
    rejects = pd.read_parquet(DATA / "rejects.parquet")
    metrics = json.loads((DATA / "run_metrics.json").read_text())
    return rates, rejects, metrics


rates, rejects, metrics = load()

st.title("logistics-rate-pipeline — normalized rate matrix")
st.caption(
    "Output of one run: 8 synthetic carrier feeds → Go ingestion service (bounded worker "
    "pool) → PySpark normalizer. **All feeds are invented**; the engineering is the point. "
    f"[Code and benchmarks]({REPO}) · run id `{metrics['run_id']}` · Spark {metrics['spark_version']}"
)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Raw rows in", f"{metrics['rows_in']:,}")
c2.metric("Normalized rows out", f"{metrics['rows_out']:,}")
c3.metric("Duplicates removed", f"{metrics['dedup_removed']:,}")
c4.metric("Rejected (named rules)", f"{metrics['rejects_total']:,}")

tab_rates, tab_rejects, tab_run = st.tabs(["Rate matrix", "Quarantine (rejects)", "Run metrics"])

with tab_rates:
    f1, f2, f3, f4 = st.columns(4)
    carrier = f1.multiselect("Carrier", sorted(rates.carrier.unique()))
    origin = f2.multiselect("Origin (UN/LOCODE)", sorted(rates.origin_locode.unique()))
    dest = f3.multiselect("Destination (UN/LOCODE)", sorted(rates.destination_locode.unique()))
    box = f4.multiselect("Container", sorted(rates.container_type.unique()))

    view = rates
    if carrier:
        view = view[view.carrier.isin(carrier)]
    if origin:
        view = view[view.origin_locode.isin(origin)]
    if dest:
        view = view[view.destination_locode.isin(dest)]
    if box:
        view = view[view.container_type.isin(box)]

    st.write(f"**{len(view):,}** lanes · one row per (carrier, origin, destination, container) after window dedup")
    cols = [
        "carrier", "origin_locode", "destination_locode", "container_type",
        "base_price", "base_currency", "unit_source", "price_usd", "surcharges_usd",
        "all_in_usd", "valid_from", "valid_to", "source_id",
    ]
    st.dataframe(
        view[cols].sort_values(["carrier", "origin_locode", "destination_locode", "container_type"]),
        width="stretch", hide_index=True, height=480,
    )

    st.subheader("All-in USD by carrier")
    st.caption("Per-TEU feeds were converted per container; EU-decimal and non-USD feeds were mapped via broadcast lookups (FX, TEU factors).")
    summary = (
        view.groupby("carrier")["all_in_usd"]
        .agg(lanes="count", min="min", median="median", max="max")
        .round(2)
        .reset_index()
    )
    st.dataframe(summary, width="stretch", hide_index=True)

with tab_rejects:
    st.write(
        "Every rejected raw row keeps its **raw values** and the **rule that caught it**, "
        "so a reject is auditable instead of silently dropped."
    )
    by_reason = rejects.reject_reason.value_counts().rename_axis("reject_reason").reset_index(name="rows")
    left, right = st.columns([1, 2])
    left.dataframe(by_reason, hide_index=True, width="stretch")
    right.bar_chart(by_reason.set_index("reject_reason")["rows"])

    reason = st.multiselect("Filter by rule", sorted(rejects.reject_reason.unique()))
    rv = rejects if not reason else rejects[rejects.reject_reason.isin(reason)]
    st.dataframe(
        rv[[
            "reject_reason", "carrier", "source_id", "origin_raw", "destination_raw",
            "container_type_raw", "price_raw", "currency", "unit", "valid_from_raw", "valid_to_raw",
        ]],
        width="stretch", hide_index=True,
    )

with tab_run:
    st.write("`run_metrics.json` written by the job alongside the Parquet output (local[2] on an 8 GB laptop).")
    stages = pd.DataFrame(
        {"stage": list(metrics["stage_durations_ms"]), "ms": list(metrics["stage_durations_ms"].values())}
    )
    st.bar_chart(stages.set_index("stage")["ms"])
    st.json(metrics)
    st.markdown(
        f"Benchmarks for the Go ingestion side (worker-pool speedup, pprof-driven parse "
        f"optimization, race/leak checks) are committed under "
        f"[`bench/results/`]({REPO}/tree/main/bench/results) and summarized on the "
        f"[results page](https://subburajan-perumal.github.io/logistics-rate-pipeline/)."
    )
