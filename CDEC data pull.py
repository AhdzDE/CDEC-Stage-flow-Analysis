import urllib.request
import urllib.parse
import json
import pandas as pd
from datetime import date, timedelta

CDEC_JSON_URL = "https://cdec.water.ca.gov/dynamicapp/req/JSONDataServlet"

def fetch_cdec(station_id: str, sensor_num: int, dur_code: str, start: str, end: str) -> pd.DataFrame:
    params = {
        "Stations": station_id,
        "SensorNums": str(sensor_num),
        "dur_code": dur_code,
        "Start": start,
        "End": end,
    }
    url = CDEC_JSON_URL + "?" + urllib.parse.urlencode(params)

    with urllib.request.urlopen(url) as resp:
        payload = resp.read().decode("utf-8")

    data = json.loads(payload)
    if not data:
        return pd.DataFrame(columns=["datetime", "value"])

    df = pd.DataFrame(data)

    # CDEC JSON typically uses keys: "date" and "value"
    df["datetime"] = pd.to_datetime(df.get("date"), errors="coerce")
    df["value"] = pd.to_numeric(df.get("value"), errors="coerce")

    df = df[df["datetime"].notna() & df["value"].notna()]
    df = df[df["value"] != -9999]

    return df[["datetime", "value"]].sort_values("datetime")


def find_earliest_date(station_id: str, sensor_num: int, dur_code: str) -> date:
    """
    Finds the earliest date with data by probing decades, then narrowing.
    """
    # Broad probes (go back to 1900 to be safe)
    probe_years = [1900, 1930, 1950, 1970, 1980, 1990, 2000, 2010]
    today = date.today()

    earliest_found = None
    for y in probe_years:
        start = date(y, 1, 1)
        end = date(y, 12, 31)
        df = fetch_cdec(station_id, sensor_num, dur_code, start.isoformat(), end.isoformat())
        if not df.empty:
            earliest_found = df["datetime"].min().date()
            break

    if earliest_found is None:
        # If nothing in early probes, do a fallback recent probe
        df = fetch_cdec(station_id, sensor_num, dur_code, "2000-01-01", today.isoformat())
        if df.empty:
            raise RuntimeError(f"No data found for {station_id} sensor {sensor_num} dur {dur_code}")
        return df["datetime"].min().date()

    # Narrow down year-by-year to get the earliest year precisely
    y = earliest_found.year
    while y > 1900:
        prev_start = date(y - 1, 1, 1)
        prev_end = date(y - 1, 12, 31)
        df_prev = fetch_cdec(station_id, sensor_num, dur_code, prev_start.isoformat(), prev_end.isoformat())
        if df_prev.empty:
            break
        earliest_found = df_prev["datetime"].min().date()
        y -= 1

    return earliest_found


def fetch_all_in_chunks(station_id: str, sensor_num: int, dur_code: str,
                        start_date: date, end_date: date,
                        chunk_days: int = 365) -> pd.DataFrame:
    """
    Pull all data from start_date to end_date in chunks to avoid timeouts/limits.
    """
    frames = []
    cur = start_date

    while cur <= end_date:
        chunk_end = min(cur + timedelta(days=chunk_days), end_date)
        df = fetch_cdec(station_id, sensor_num, dur_code, cur.isoformat(), chunk_end.isoformat())
        frames.append(df)
        print(f"{station_id} sensor {sensor_num}: {cur} to {chunk_end} -> {len(df)} rows")
        cur = chunk_end + timedelta(days=1)

    if not frames:
        return pd.DataFrame(columns=["datetime", "value"])

    out = pd.concat(frames, ignore_index=True)
    out = out.drop_duplicates(subset=["datetime"]).sort_values("datetime")
    return out


def export_station_all_time(station_id: str, out_csv: str):
    # For LCH: stage=1, flow=20 (event data)
    dur_code = "E"

    today = date.today()

    earliest_stage = find_earliest_date(station_id, 1, dur_code)
    earliest_flow = find_earliest_date(station_id, 20, dur_code)
    start_date = min(earliest_stage, earliest_flow)

    print(f"Earliest stage: {earliest_stage}")
    print(f"Earliest flow : {earliest_flow}")
    print(f"Using start   : {start_date}")
    print(f"Using end     : {today}")

    stage = fetch_all_in_chunks(station_id, 1, dur_code, start_date, today, chunk_days=365)
    flow  = fetch_all_in_chunks(station_id, 20, dur_code, start_date, today, chunk_days=365)

    stage = stage.rename(columns={"value": "stage_ft"})
    flow  = flow.rename(columns={"value": "flow_cfs"})

    combined = pd.merge(stage, flow, on="datetime", how="outer").sort_values("datetime")

    # Save CSVs
    combined.to_csv(out_csv, index=False)
    stage.to_csv("LCH_stage_ALL.csv", index=False)
    flow.to_csv("LCH_flow_ALL.csv", index=False)

    print(f"\nSaved: {out_csv}")
    print(f"Rows: combined={len(combined)}, stage={len(stage)}, flow={len(flow)}")


if __name__ == "__main__":
    export_station_all_time("LCH", "LCH_stage_flow_ALL.csv")
