"""
Real-time fraud monitoring dashboard.

Reads the Gold-layer Parquet windows produced by the Spark Structured
Streaming job and renders a live operations view: headline counters, a
transaction-volume timeline, a per-type fraud breakdown over time, and a
recent-windows alert table. Toggle auto-refresh to watch it update while the
streaming job is writing.

Run (inside the `fraud` conda env):
    streamlit run dashboard/app.py
"""

import glob
import os
import time

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GOLD_DIR = os.path.join(PROJECT_ROOT, "data", "gold")
SAMPLE_PATH = os.path.join(PROJECT_ROOT, "data", "gold_sample", "windows.parquet")

# ---------------------------------------------------------------------------
# Palette — "operations console": deep slate ground, amber signal, red alert
# ---------------------------------------------------------------------------
INK = "#0d1117"
PANEL = "#161b22"
GRID = "#21262d"
TEXT = "#c9d1d9"
MUTED = "#6e7681"
AMBER = "#d29922"
RED = "#f85149"
GREEN = "#3fb950"
BLUE = "#58a6ff"

st.set_page_config(
    page_title="Fraud Stream Monitor",
    page_icon="•",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Styling
# ---------------------------------------------------------------------------
st.markdown(
    f"""
    <style>
    .stApp {{ background: {INK}; color: {TEXT}; }}
    .block-container {{ padding-top: 3.5rem; padding-bottom: 4rem; max-width: 1300px; }}
    h1, h2, h3 {{ color: {TEXT}; font-family: 'Inter', system-ui, sans-serif; letter-spacing: -0.01em; }}
    .eyebrow {{
        font-family: ui-monospace, 'SFMono-Regular', Menlo, monospace;
        font-size: 0.72rem; letter-spacing: 0.22em; text-transform: uppercase;
        color: {AMBER}; margin-bottom: 0.35rem;
    }}
    .kpi {{
        background: {PANEL}; border: 1px solid {GRID}; border-radius: 10px;
        padding: 1.1rem 1.3rem;
    }}
    .kpi .label {{
        font-family: ui-monospace, monospace; font-size: 0.7rem;
        letter-spacing: 0.14em; text-transform: uppercase; color: {MUTED};
    }}
    .kpi .value {{
        font-family: ui-monospace, monospace; font-size: 2.0rem;
        font-weight: 600; color: {TEXT}; line-height: 1.25; margin-top: 0.2rem;
    }}
    .kpi .value.alert {{ color: {RED}; }}
    .kpi .value.warn {{ color: {AMBER}; }}
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data(ttl=5)
def load_gold() -> pd.DataFrame:
    # Local: read the full Gold folder written by Spark.
    # Deploy (no Spark): fall back to the small versioned sample.
    files = glob.glob(os.path.join(GOLD_DIR, "*.parquet"))
    if files:
        frames = []
        for f in files:
            try:
                frames.append(pd.read_parquet(f))
            except Exception:
                continue
        if frames:
            df = pd.concat(frames, ignore_index=True)
            df = df.drop_duplicates(subset=["window_start", "window_end"])
            return df.sort_values("window_start").reset_index(drop=True)

    if os.path.exists(SAMPLE_PATH):
        df = pd.read_parquet(SAMPLE_PATH)
        df = df.drop_duplicates(subset=["window_start", "window_end"])
        return df.sort_values("window_start").reset_index(drop=True)

    return pd.DataFrame()


def kpi(col, label, value, kind=""):
    col.markdown(
        f'<div class="kpi"><div class="label">{label}</div>'
        f'<div class="value {kind}">{value}</div></div>',
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------------
# Header + controls
# ---------------------------------------------------------------------------
left, right = st.columns([3, 1])
with left:
    st.markdown('<div class="eyebrow">Real-time card transactions</div>', unsafe_allow_html=True)
    st.markdown("# Fraud Stream Monitor")
with right:
    auto = st.toggle("Auto-refresh (5s)", value=False)
    st.caption("Reads Gold windows written by Spark.")

df = load_gold()

if df.empty:
    st.info(
        "No Gold windows yet. Start the pipeline: run the producer and the "
        "Spark streaming job, then this panel will fill in.",
    )
    st.stop()

# ---------------------------------------------------------------------------
# KPIs
# ---------------------------------------------------------------------------
total_tx = int(df["tx_count"].sum())
total_fraud = int(df["fraud_total"].sum())
fraud_rate = (total_fraud / total_tx * 100) if total_tx else 0
total_amount = float(df["total_amount"].sum())

c1, c2, c3, c4 = st.columns(4)
kpi(c1, "Transactions", f"{total_tx:,}")
kpi(c2, "Frauds detected", f"{total_fraud:,}", "alert")
kpi(c3, "Fraud rate", f"{fraud_rate:.1f}%", "warn")
kpi(c4, "Volume processed", f"${total_amount:,.0f}")

st.markdown("<div style='height:1.4rem'></div>", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Transaction volume over time
# ---------------------------------------------------------------------------
st.markdown('<div class="eyebrow">Throughput</div>', unsafe_allow_html=True)
st.markdown("### Transactions per minute")
vol = go.Figure()
vol.add_trace(go.Scatter(
    x=df["window_start"], y=df["tx_count"],
    mode="lines", line=dict(color=BLUE, width=2),
    fill="tozeroy", fillcolor="rgba(88,166,255,0.10)",
    name="tx/min",
))
vol.update_layout(
    height=300, paper_bgcolor=PANEL, plot_bgcolor=PANEL,
    margin=dict(l=10, r=10, t=10, b=10), font=dict(color=TEXT),
    xaxis=dict(gridcolor=GRID), yaxis=dict(gridcolor=GRID),
    showlegend=False,
)
st.plotly_chart(vol, use_container_width=True, config={"scrollZoom": False, "displayModeBar": False})

# ---------------------------------------------------------------------------
# Fraud by type over time
# ---------------------------------------------------------------------------
st.markdown('<div class="eyebrow">Detection</div>', unsafe_allow_html=True)
st.markdown("### Fraud signals by type")
fig = go.Figure()
for label, ccol, color in [
    ("Card testing", "fraud_card_testing", AMBER),
    ("High amount", "fraud_high_amount", RED),
    ("Impossible travel", "fraud_impossible_travel", GREEN),
]:
    fig.add_trace(go.Bar(
        x=df["window_start"], y=df[ccol], name=label, marker_color=color,
    ))
fig.update_layout(
    barmode="stack", height=320, paper_bgcolor=PANEL, plot_bgcolor=PANEL,
    margin=dict(l=10, r=10, t=10, b=10), font=dict(color=TEXT),
    xaxis=dict(gridcolor=GRID), yaxis=dict(gridcolor=GRID),
    legend=dict(orientation="h", y=1.12, x=0),
)
st.plotly_chart(fig, use_container_width=True, config={"scrollZoom": False, "displayModeBar": False})

# ---------------------------------------------------------------------------
# Recent windows — alert table
# ---------------------------------------------------------------------------
st.markdown('<div class="eyebrow">Latest windows</div>', unsafe_allow_html=True)
st.markdown("### Recent activity")
recent = df.tail(12).iloc[::-1].copy()
recent["avg_amount"] = recent["avg_amount"].round(2)
recent["total_amount"] = recent["total_amount"].round(2)
show = recent[[
    "window_start", "tx_count", "total_amount", "avg_amount",
    "fraud_card_testing", "fraud_high_amount",
    "fraud_impossible_travel", "fraud_total",
]].rename(columns={
    "window_start": "Window",
    "tx_count": "Tx",
    "total_amount": "Total $",
    "avg_amount": "Avg $",
    "fraud_card_testing": "Card testing",
    "fraud_high_amount": "High amount",
    "fraud_impossible_travel": "Impossible travel",
    "fraud_total": "Frauds",
})


def highlight_fraud(row):
    color = RED if row["Frauds"] >= 40 else (AMBER if row["Frauds"] >= 20 else "")
    return [f"color: {color}" if color else "" for _ in row]


st.dataframe(
    show.style.apply(highlight_fraud, axis=1).format({
        "Total $": "${:,.0f}", "Avg $": "${:,.2f}", "Tx": "{:,}",
    }),
    use_container_width=True, hide_index=True,
)

st.caption(
    f"{len(df)} windows · pipeline: producer → Redpanda → Spark Structured "
    f"Streaming → Parquet (Gold) → this dashboard",
)

if auto:
    time.sleep(5)
    st.rerun()
