import pandas as pd

# Load CSV (file path or URL)
source = "your_cdec_file_or_url.csv"
df = pd.read_csv(source)

# Clean column names
df.columns = df.columns.str.strip()

# Parse date and numeric values
df["DATE TIME"] = pd.to_datetime(df["DATE TIME"], errors="coerce")
df["VALUE"] = pd.to_numeric(df["VALUE"], errors="coerce")

# Drop invalid rows
df = df.dropna(subset=["DATE TIME", "VALUE"])

# Extract calendar year
df["Year"] = df["DATE TIME"].dt.year

# Group and compute stats
summary = (
    df.groupby(["Year", "Water Year Type"])["VALUE"]
    .agg(
        Minimum_Flow="min",
        Maximum_Flow="max",
        Median_Flow="median",
        Mean_Flow="mean",
    )
    .reset_index()
)

# Optional: round values
summary[["Minimum_Flow", "Maximum_Flow", "Median_Flow", "Mean_Flow"]] = \
    summary[["Minimum_Flow", "Maximum_Flow", "Median_Flow", "Mean_Flow"]].round(2)

print(summary)
