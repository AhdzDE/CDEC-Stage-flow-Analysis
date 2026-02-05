#!/usr/bin/env python3
"""
Summarize WDL-style flow statistics per year.

"""
import argparse
import sys
import pandas as pd
import os
import re

DEFAULT_VALUE_KEYWORDS = ["flow", "stream", "discharge", "cfs"]

def find_datetime_and_value_columns(df: pd.DataFrame, datetime_col_hint: str = None, value_col_hint: str = None):
    # If user provided explicit names, prefer those (if present)
    if datetime_col_hint and datetime_col_hint in df.columns:
        dt_col = datetime_col_hint
    else:
        # common names for datetime
        candidates = [c for c in df.columns if re.search(r"date|time", c, flags=re.I)]
        dt_col = candidates[0] if candidates else None

    if value_col_hint and value_col_hint in df.columns:
        val_col = value_col_hint
    else:
        # try to find any column with a keyword like "flow" or "cfs"
        candidates = [c for c in df.columns if any(k in c.lower() for k in DEFAULT_VALUE_KEYWORDS)]
        if candidates:
            val_col = candidates[0]
        else:
            # fallback: find first numeric column (excluding datetime)
            numeric_cols = [c for c in df.columns if pd.to_numeric(df[c], errors="coerce").notna().any()]
            # avoid picking columns that clearly aren't the value (like quality code)
            if dt_col and dt_col in numeric_cols:
                numeric_cols = [c for c in numeric_cols if c != dt_col]
            val_col = numeric_cols[0] if numeric_cols else None

    return dt_col, val_col

def load_wdl_csv(path, sep=",", sheet_name=None):
    """
    Loads a WDL-style CSV/TSV that contains comment lines starting with '#'.
    If file is Excel, loads using pandas.read_excel.

    This version forces reading only the first two columns (Date Time + Flow),
    which avoids parsing problems when the Quality Code column contains commas.
    """
    path_l = path.lower()
    if path_l.endswith((".xls", ".xlsx")):
        # Excel file
        df = pd.read_excel(path, sheet_name=(sheet_name or 0))
        return df
    else:
        # Text file: use comment="#" so pandas skips lines starting with '#'
        # Use usecols=[0,1] to only read the first two columns (Date Time and Flow).
        # This avoids malformed extra commas in trailing columns (e.g. Quality Code).
        df = pd.read_csv(
            path,
            sep=sep,
            comment="#",
            engine="python",
            usecols=[0, 1],      # <-- read only the first two columns
            dtype=str            # read as strings initially (parsing later)
        )
        # Trim whitespace from headers (sometimes WDL headers have leading/trailing spaces)
        df.columns = [c.strip() for c in df.columns]
        return df


def summarize_by_year(df: pd.DataFrame, datetime_col: str, value_col: str) -> pd.DataFrame:
    if datetime_col not in df.columns:
        raise ValueError(f"Datetime column '{datetime_col}' not found in data columns: {list(df.columns)}")
    if value_col not in df.columns:
        raise ValueError(f"Value column '{value_col}' not found in data columns: {list(df.columns)}")

    # Parse datetimes robustly
    dt = pd.to_datetime(df[datetime_col], errors="coerce", infer_datetime_format=True)
    if dt.isna().all():
        # try common US formats explicitly
        dt = pd.to_datetime(df[datetime_col], format="%m/%d/%Y %H:%M", errors="coerce")
    if dt.isna().all():
        dt = pd.to_datetime(df[datetime_col], format="%m/%d/%Y %H:%M:%S", errors="coerce")

    if dt.isna().all():
        raise ValueError(f"Could not parse datetimes from column '{datetime_col}' (first 10 values shown):\n{df[datetime_col].head(10)}")

    vals = pd.to_numeric(df[value_col], errors="coerce")

    work = pd.DataFrame({"datetime": dt, "value": vals}).dropna(subset=["datetime", "value"])
    if work.empty:
        raise ValueError("No rows left after parsing datetimes and numeric values. Check your input file/columns.")
    work["Year"] = work["datetime"].dt.year

    g = work.groupby("Year")["value"]

    out = pd.DataFrame({
        "Year": g.mean().index,
        "Average Flow Rate (CFS)": g.mean().values,
        "Max Flow Rate (CFS)": g.max().values,
        "Min Flow Rate (CFS)": g.min().values,
        "Median(CFS)": g.median().values,
        "25%": g.quantile(0.25).values,
        "50%": g.quantile(0.50).values,
        "95%": g.quantile(0.95).values,
        "99%": g.quantile(0.99).values,
    })

    # Round numbers for neatness
    numeric_cols = [c for c in out.columns if c != "Year"]
    out[numeric_cols] = out[numeric_cols].round(6)

    out = out.sort_values("Year").reset_index(drop=True)
    return out

def main():
    p = argparse.ArgumentParser(description="Compute yearly flow stats table from WDL-style exports.")
    p.add_argument("--input", "-i", required=True, help="Input CSV/TSV/Excel file path")
    p.add_argument("--output", "-o", default="", help="Output CSV or XLSX path. If omitted, prints to console.")
    p.add_argument("--datetime-col", default=None, help="Datetime column name (optional). Default: auto-detect.")
    p.add_argument("--value-col", default=None, help="Value/flow column name (optional). Default: auto-detect.")
    p.add_argument("--sheet", default=None, help="Excel sheet name (if input is Excel)")
    p.add_argument("--sep", default=",", help="Separator for text files (default ','). Use '\\t' for TSV")
    args = p.parse_args()

    # Load
    df = load_wdl_csv(args.input, sep=args.sep, sheet_name=args.sheet)

    # Auto-detect columns
    dt_col, val_col = find_datetime_and_value_columns(df, args.datetime_col, args.value_col)

    if dt_col is None or val_col is None:
        raise ValueError(f"Unable to auto-detect datetime or value columns. Columns found: {list(df.columns)}\n"
                         f"Detected datetime: {dt_col}, detected value: {val_col}\n"
                         f"Try specifying --datetime-col and/or --value-col explicitly.")

    # Show what we detected (useful)
    print(f"Using datetime column: '{dt_col}'")
    print(f"Using value column:    '{val_col}'")

    summary = summarize_by_year(df, dt_col, val_col)

    # Save or print
    if args.output:
        out_path = args.output
        if out_path.lower().endswith(".xlsx"):
            try:
                summary.to_excel(out_path, index=False)
            except Exception as e:
                print("ERROR writing .xlsx. Do you have openpyxl installed? try: pip install openpyxl", file=sys.stderr)
                raise
        else:
            summary.to_csv(out_path, index=False)
        print(f"Wrote: {out_path}")
    else:
        with pd.option_context("display.max_columns", None, "display.width", 200):
            print(summary.to_string(index=False))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr)
        sys.exit(1)
    

