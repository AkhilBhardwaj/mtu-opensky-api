"""
OpenSky Network → CSV Writer
Fetches live aircraft data every 30 seconds and appends to a CSV file.

VS Code  : set OUTPUT_PATH to any local folder, e.g. r"C:\\Users\\akhil\\opensky"
Databricks: set OUTPUT_PATH to "/dbfs/tmp/opensky"  (accessible in file browser)

The CSV grows with each fetch — every row has a fetched_at timestamp so you
can query history: "show me all aircraft at 09:00 vs 09:30"
"""

import os
import time
import requests
import pandas as pd
from datetime import datetime, timezone

# ── Config ────────────────────────────────────────────────────────────────────
# CHANGE THIS PATH:
#   VS Code    →  r"C:\Users\akhil\opensky"   (Windows)  or  "/tmp/opensky"  (Mac/Linux)
#   Databricks →  "/dbfs/tmp/opensky"
OUTPUT_PATH     = r"C:\Akhil\Business\TechnologyConsulting\Azure Architecture\CodeExp2\Data"

LAMIN, LOMIN    = 47.0, 7.0      # Bounding box: Central Europe
LAMAX, LOMAX    = 53.5, 15.0
REFRESH_SECONDS = 30
USERNAME        = os.getenv("OPENSKY_USER", "")
PASSWORD        = os.getenv("OPENSKY_PASS", "")

FIELDS = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
    "true_track", "vertical_rate", "sensors", "geo_altitude",
    "squawk", "spi", "position_source",
]

# ── Fetch ─────────────────────────────────────────────────────────────────────
def fetch_states():
    url = "https://opensky-network.org/api/states/all"
    params = {"lamin": LAMIN, "lomin": LOMIN, "lamax": LAMAX, "lomax": LOMAX}
    auth = (USERNAME, PASSWORD) if USERNAME else None
    try:
        r = requests.get(url, params=params, auth=auth, timeout=15)
        r.raise_for_status()
        return r.json().get("states", []) or []
    except requests.exceptions.HTTPError as e:
        if r.status_code == 429:
            print("  Rate limit hit — waiting 60s...")
            time.sleep(60)
        else:
            print(f"  HTTP error: {e}")
        return []
    except Exception as e:
        print(f"  Error: {e}")
        return []

# ── Parse ─────────────────────────────────────────────────────────────────────
def parse(states, fetched_at):
    df = pd.DataFrame(states, columns=FIELDS)

    # Drop ground traffic and rows with no useful data
    df = df[
        (~df["on_ground"]) &
        (df["callsign"].notna()) &
        (df["baro_altitude"].notna())
    ].copy()

    df["callsign"]           = df["callsign"].str.strip()
    df["baro_altitude_ft"]   = (df["baro_altitude"]   * 3.28084).round(0)
    df["velocity_kmh"]       = (df["velocity"]        * 3.6).round(1)
    df["vertical_rate_fpm"]  = (df["vertical_rate"]   * 196.85).round(0)

    def phase(row):
        alt = row["baro_altitude"] or 0
        vr  = row["vertical_rate"] or 0
        if alt < 3000 and vr >  2: return "CLIMBING"
        if alt < 3000 and vr < -2: return "DESCENDING"
        if alt < 3000:             return "LOW ALT"
        if vr >  2:                return "CLIMBING"
        if vr < -2:                return "DESCENDING"
        return "CRUISE"

    df["phase"]       = df.apply(phase, axis=1)
    df["fetched_at"]  = fetched_at          # timestamp of this fetch — key for history queries

    # Keep only the columns worth storing
    return df[[
        "fetched_at", "icao24", "callsign", "origin_country",
        "latitude", "longitude", "baro_altitude_ft", "velocity_kmh",
        "vertical_rate_fpm", "phase", "squawk",
    ]]

# ── Write CSV ─────────────────────────────────────────────────────────────────
def write_csv(df, path, fetched_at):
    os.makedirs(path, exist_ok=True)
    # e.g. opensky_2026-06-14_09-32-15.csv
    timestamp = fetched_at.replace(" ", "_").replace(":", "-")
    filename  = f"opensky_{timestamp}.csv"
    filepath  = os.path.join(path, filename)
    df.to_csv(filepath, index=False)
    return filepath

# ── Main loop ─────────────────────────────────────────────────────────────────
def main():
    print(f"\n  OpenSky → CSV  |  Writing to: {OUTPUT_PATH}")
    print(f"  Bounding box: lat {LAMIN}–{LAMAX}, lon {LOMIN}–{LOMAX}")
    print(f"  Refreshing every {REFRESH_SECONDS}s  |  Ctrl+C to stop\n")

    iteration = 0
    total_rows = 0

    while True:
        iteration += 1
        fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        print(f"  [{fetched_at}]  Fetch #{iteration} ...", end=" ", flush=True)
        states = fetch_states()

        if not states:
            print("no data")
        else:
            df = parse(states, fetched_at)
            filepath = write_csv(df, OUTPUT_PATH, fetched_at)
            total_rows += len(df)
            print(f"{len(df)} rows  →  {os.path.basename(filepath)}  |  {total_rows} total rows so far")

        time.sleep(REFRESH_SECONDS)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n  Stopped.\n")
