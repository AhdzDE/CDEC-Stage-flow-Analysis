#!/usr/bin/env python3
"""
Combine Napa OneRain .txt exports (tab-delimited) and summarize by year.

Expected columns (case-insensitive):
  Reading, Receive, Value, Unit, Data Quality
"""

import argparse
import glob
import os
import sys
import pandas as pd


def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df


def load_onerain_txt(path: str) -> pd.DataFrame:
    # Read tab-delimited, tolerate weird quoting
    df = pd.read_csv(path, sep="\t", engine="python")
    df = _norm_cols(df)

    # Expected normalized names
    required = ["reading", "receive", "value", "unit", "data_quality"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"File '{os.path.basename(path)}' missing columns {missing}. "
            f"Found: {list(df.columns)}"
        )

    out = df[["reading", "receive", "value", "unit", "data_quality"]].copy()
    out.columns = ["Reading", "Receive", "Value", "Unit", "DataQuality"]

    out["Reading"] = pd.to_datetime(out["Reading"], errors="coerce")
    out["Receive"] = pd.to_datetime(out["Receive"], errors="coerce")
    out["Value"] = pd.to_numeric(out["Value"], errors="coerce")

    out = out.dropna(subset=["Reading", "Value"])
    out["SourceFile"] = os.path.basename(path)
    return out


def summarize_by_year(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["Year"] = df["Reading"].dt.year

    g = df.groupby("Year")["Value"]

    out = pd.DataFrame({
        "Year": g.mean().index,
        "Mean": g.mean().values,
        "Max": g.max().values,
        "Min": g.min().values,
        "Median": g.median().values,
        "25%": g.quantile(0.25).values,
        "50%": g.quantile(0.50).values,
        "95%": g.quantile(0.95).values,
        "99%": g.quantile(0.99).values,
        "N": g.size().values,
    })

    num_cols = [c for c in out.columns if c not in ("Year", "N")]
    out[num_cols] = out[num_cols].round(6)

    return out.sort_values("Year").reset_index(drop=True)


def main():
    p = argparse.ArgumentParser(description="Combine OneRain tab-delimited .txt exports and summarize by year.")
    p.add_argument("--input-dir", "-d", required=True, help="Folder containing OneRain .txt files")
    p.add_argument("--pattern", default="*.txt", help="Glob pattern (default: *.txt)")
    p.add_argument("--combined-out", default="onerain_combined.csv", help="Combined output CSV path")
    p.add_argument("--summary-out", default="onerain_yearly_summary.csv", help="Yearly summary CSV path")
    p.add_argument("--quality", default="", help="Optional: keep only this DataQuality (e.g., A)")
    p.add_argument("--unit", default="", help="Optional: keep only this Unit (e.g., ft, cfs)")

    args = p.parse_args()

    files = sorted(glob.glob(os.path.join(args.input_dir, args.pattern)))
    if not files:
        raise FileNotFoundError(f"No files matched: {os.path.join(args.input_dir, args.pattern)}")

    frames = []
    for f in files:
        try:
            frames.append(load_onerain_txt(f))
        except Exception as e:
            print(f"WARNING: Skipping '{f}' due to error: {e}", file=sys.stderr)

    if not frames:
        raise ValueError("No files could be loaded. Check the delimiter/format and headers.")

    combined = pd.concat(frames, ignore_index=True)

    if args.quality:
        combined = combined[combined["DataQuality"].astype(str).str.strip().eq(args.quality)].copy()
    if args.unit:
        combined = combined[combined["Unit"].astype(str).str.strip().eq(args.unit)].copy()

    if combined.empty:
        raise ValueError("After filtering, no data remained. Remove filters or verify values.")

    combined.to_csv(args.combined_out, index=False)
    print(f"Wrote combined: {args.combined_out} (rows={len(combined)})")

    summary = summarize_by_year(combined)
    summary.to_csv(args.summary_out, index=False)
    print(f"Wrote summary:  {args.summary_out} (years={summary['Year'].nunique()})")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

