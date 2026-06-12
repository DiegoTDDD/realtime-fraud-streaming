"""
Export a small, deploy-friendly Gold sample.

The streaming job writes many tiny Parquet files to data/gold/. The deployed
dashboard can't run Spark and shouldn't carry hundreds of files, so this
consolidates every Gold window into a single compact Parquet at
data/gold_sample/windows.parquet, which is the only data file versioned in Git.

Run (inside the `fraud` conda env, after stopping the streaming job):
    python spark/export_sample.py
"""

import glob
import os

import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GOLD_DIR = os.path.join(PROJECT_ROOT, "data", "gold")
SAMPLE_DIR = os.path.join(PROJECT_ROOT, "data", "gold_sample")
SAMPLE_PATH = os.path.join(SAMPLE_DIR, "windows.parquet")


def main():
    files = glob.glob(os.path.join(GOLD_DIR, "*.parquet"))
    if not files:
        raise SystemExit("No Gold parquet files found. Run the pipeline first.")

    frames = [pd.read_parquet(f) for f in files]
    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates(subset=["window_start", "window_end"])
    df = df.sort_values("window_start").reset_index(drop=True)

    os.makedirs(SAMPLE_DIR, exist_ok=True)
    df.to_parquet(SAMPLE_PATH, index=False)

    print(f"Wrote {len(df)} windows to {SAMPLE_PATH}")
    print(f"File size: {os.path.getsize(SAMPLE_PATH) / 1024:.1f} KB")


if __name__ == "__main__":
    main()
