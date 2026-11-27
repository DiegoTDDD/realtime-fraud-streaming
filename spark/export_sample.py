"""
Export a small, deploy-friendly Gold sample.

The streaming job writes Gold as a Delta table (data/delta/gold). The deployed
dashboard has no Spark and no table, so this writes every Gold window to a single
compact Parquet at data/gold_sample/windows.parquet, which is the only data file
versioned in Git. It reads the table with deltalake, so the job can keep running.

Run (inside the `fraud` conda env, after stopping the streaming job):
    python spark/export_sample.py
"""

import logging
import os

import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GOLD_DIR = os.path.join(PROJECT_ROOT, "data", "delta", "gold")
SAMPLE_DIR = os.path.join(PROJECT_ROOT, "data", "gold_sample")
SAMPLE_PATH = os.path.join(SAMPLE_DIR, "windows.parquet")

log = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if not os.path.isdir(os.path.join(GOLD_DIR, "_delta_log")):
        raise SystemExit("No Gold table found. Run the pipeline first.")

    from deltalake import DeltaTable

    df: pd.DataFrame = DeltaTable(GOLD_DIR).to_pandas()
    df = df.sort_values("window_start").reset_index(drop=True)

    os.makedirs(SAMPLE_DIR, exist_ok=True)
    df.to_parquet(SAMPLE_PATH, index=False)

    log.info(
        "Wrote %d windows to %s (%.1f KB)",
        len(df),
        SAMPLE_PATH,
        os.path.getsize(SAMPLE_PATH) / 1024,
    )


if __name__ == "__main__":
    main()
