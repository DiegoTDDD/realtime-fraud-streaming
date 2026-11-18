# 0006. The public dashboard reads a committed Gold sample

**Status:** Accepted

## Context
Streamlit Community Cloud runs plain Python only: no Docker, no JVM and no broker. It cannot run the pipeline.

## Decision
`spark/export_sample.py` merges a run's Gold windows into `data/gold_sample/windows.parquet`, which is committed. When `data/gold/` is empty, as on the cloud host, the dashboard falls back to this sample.

## Alternatives considered
- **Host the full pipeline on a paid VM.** It would be live, but it costs money and goes down when the budget does.
- **Screenshots only.** Nothing for a visitor to interact with.

## Consequences
- The public link always works and costs nothing.
- The public dashboard shows a past run, not live data. The README says so, and the full pipeline produces live data locally with three commands.
