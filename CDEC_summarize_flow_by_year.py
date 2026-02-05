#!/usr/bin/env python3
"""
Summarize flow statistics per year (mean, max, min, median, and percentiles).

Example:
  python summarize_flow_by_year.py --input data.csv --output yearly_summary.csv \
    --datetime-col "DATE TIME" --value-col "VALUE"
"""

import argparse
import sys
import pandas as pd


def summarize_by_year(df: pd.DataFrame, datetime_col: str, value_col: str) -> pd.DataFrame:
    # Parse datetime
    dt = pd.to_datetime(df[datetime_col], errors="coerce")
    if dt.isna().all():
        raise ValueError(
            f"Could not parse any datetimes from column '{datetime_col}'. "
            f"Check the column name and datetime format."
        )

    # Parse numeric values
    vals = pd.to_numeric(df[value_col], errors="coerce")

    work = pd.DataFrame({"datetime": dt, "value": vals}).dropna(subset=["datetime", "value"])
    work["Year"] = work["datetime"].dt.year

    g = work.groupby("Year")["value"]

    out = pd.DataFrame(
        {
            "Average Flow Rate (CFS)": g.mean(),
            "Max Flow Rate (CFS)": g.max(),
            "Min Flow Rate (CFS)": g.min(),
            "Median(CFS)": g.median(),
            "25%": g.quantile(0.25),
            "50%": g.quantile(0.50),
            "95%": g.quantile(0.95),
            "99%": g.quantile(0.99),
        }
    ).reset_index()

    # Optional: round for display (change/remove if you want full precision)
    numeric_cols = [c for c in out.columns if c != "Year"]
    out[numeric_cols] = out[numeric_cols].round(6)

    return out.sort_values("Year").reset_index(drop=True)


def main():
    p = argparse.ArgumentParser(description="Compute yearly flow stats table.")

    p.add_argument(
        "--input", "-i",
        required=True,
        help="Input CSV/TSV/Excel file path"
    )
    p.add_argument(
        "--output", "-o",
        default="",
        help="Output CSV path (optional)"
    )
    p.add_argument(
        "--datetime-col",
        default="DATE TIME",
        help="Datetime column name"
    )
    p.add_argument(
        "--value-col",
        default="VALUE",
        help="Numeric value column name (flow)"
    )
    p.add_argument(
        "--sheet",
        default="",
        help="Excel sheet name (only used if input is .xlsx)"
    )
    p.add_argument(
        "--sep",
        default=",",
        help="Delimiter for text files (default ','). Use '\\t' for TSV."
    )

    args = p.parse_args()


    in_path = args.input.lower()

    # Load data
    if in_path.endswith((".xlsx", ".xls")):
        df = pd.read_excel(args.input, sheet_name=(args.sheet or 0))
    else:
        sep = "\t" if args.sep == r"\t" else args.sep
        df = pd.read_csv(args.input, sep=sep, engine="python")

    # Build summary
    summary = summarize_by_year(df, args.datetime_col, args.value_col)

    # Save or print
    if args.output:
        summary.to_csv(args.output, index=False)
        print(f"Wrote: {args.output}")
    else:
        # Print a clean console table
        with pd.option_context("display.max_columns", None, "display.width", 200):
            print(summary.to_string(index=False))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
