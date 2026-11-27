# 0006. The public dashboard reads a committed Gold sample

**Status:** Accepted

## Context
Streamlit Community Cloud runs plain Python only: no Docker, no JVM and no broker. It cannot run the pipeline.

## Decision
`spark/export_sample.py` writes a run's Gold windows from the Delta table to `data/gold_sample/windows.parquet`, which is committed; the first window is left out, because the stream starts part-way through that minute. When there is no Delta Gold table in `data/delta/gold`, as on the cloud host, the dashboard falls back to this sample.

## Alternatives considered
- **Host the full pipeline on a paid VM.** It would be live, but it costs money and goes down when the budget does.
- **Screenshots only.** Nothing for a visitor to interact with.

## Consequences
- The public link always works and costs nothing.
- The public dashboard shows a past run, not live data. The dashboard and the README say so, and the README's steps produce live data locally.
