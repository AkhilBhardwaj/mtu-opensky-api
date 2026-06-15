"""
OpenSky Network — Live Aircraft Feed
Run in VS Code: python opensky_live.py
 
Shows live aircraft over Europe, refreshing every 30 seconds.
No API key required for anonymous access (rate limit: 10 req/min).
 
Optional: register free at https://opensky-network.org to get higher rate limits.
Set OPENSKY_USER and OPENSKY_PASS environment variables if you have an account.
"""
 
import os
import time
import requests
import pandas as pd
from datetime import datetime, timezone
 
# ── Config ────────────────────────────────────────────────────────────────────
# Bounding box: Europe (covers MTU sites — Munich, Amsterdam, Hannover, Berlin)
LAMIN, LOMIN = 47.0, 7.0      # SW corner (roughly Munich area)
LAMAX, LOMAX = 53.5, 15.0     # NE corner (roughly Berlin area)
 
# To watch all of Europe, use: LAMIN=35.0, LOMIN=-10.0, LAMAX=71.0, LOMAX=40.0
 
REFRESH_SECONDS = 30           # OpenSky anonymous: max 10 req/min, stay safe
MAX_ROWS        = 30           # How many aircraft to display per refresh
 
# Optional credentials (higher rate limits + more data)
USERNAME = os.getenv("OPENSKY_USER", "")
PASSWORD = os.getenv("OPENSKY_PASS", "")
 
# ── OpenSky field names (in order the API returns them) ──────────────────────
FIELDS = [
    "icao24",           # Unique ICAO 24-bit transponder address
    "callsign",         # Flight callsign (e.g. DLH123)
    "origin_country",   # Country of registration
    "time_position",    # Unix timestamp of last position update
    "last_contact",     # Unix timestamp of last signal received
    "longitude",        # Decimal degrees (WGS-84)
    "latitude",         # Decimal degrees (WGS-84)
    "baro_altitude",    # Barometric altitude (metres)
    "on_ground",        # True if aircraft is on ground
    "velocity",         # Ground speed (m/s)
    "true_track",       # True track angle (degrees clockwise from north)
    "vertical_rate",    # Vertical rate (m/s — positive = climbing)
    "sensors",          # IDs of receivers that contributed (list)
    "geo_altitude",     # Geometric altitude (metres)
    "squawk",           # Transponder squawk code
    "spi",              # Special purpose indicator
    "position_source",  # 0=ADS-B, 1=ASTERIX, 2=MLAT, 3=FLARM
]
 
 
def fetch_states(lamin, lomin, lamax, lomax):
    """Fetch aircraft states from OpenSky REST API."""
    url = "https://opensky-network.org/api/states/all"
    params = {"lamin": lamin, "lomin": lomin, "lamax": lamax, "lomax": lomax}
    auth = (USERNAME, PASSWORD) if USERNAME else None
 
    try:
        response = requests.get(url, params=params, auth=auth, timeout=15)
        response.raise_for_status()
        data = response.json()
        return data.get("states", []) or []
    except requests.exceptions.HTTPError as e:
        if response.status_code == 429:
            print("  ⚠  Rate limit hit — waiting 60 seconds...")
            time.sleep(60)
        else:
            print(f"  ✗  HTTP error: {e}")
        return []
    except requests.exceptions.ConnectionError:
        print("  ✗  Cannot reach opensky-network.org — check your internet connection.")
        return []
    except Exception as e:
        print(f"  ✗  Unexpected error: {e}")
        return []
 
 
def parse_states(states):
    """Convert raw state vectors to a clean DataFrame."""
    df = pd.DataFrame(states, columns=FIELDS)
 
    # Clean up
    df["callsign"]       = df["callsign"].str.strip().replace("", "N/A")
    df["baro_altitude_ft"] = (df["baro_altitude"] * 3.28084).round(0)
    df["velocity_kmh"]   = (df["velocity"] * 3.6).round(1)
    df["vertical_rate_fpm"] = (df["vertical_rate"] * 196.85).round(0)
 
    # Derived: phase of flight
    def flight_phase(row):
        if row["on_ground"]:
            return "ON GROUND"
        alt = row["baro_altitude"] or 0
        vr  = row["vertical_rate"] or 0
        if alt < 1000 and vr > 1:      return "CLIMBING"
        if alt < 1000 and vr < -1:     return "DESCENDING"
        if alt < 3000:                 return "LOW ALT"
        if vr > 2:                     return "CLIMBING"
        if vr < -2:                    return "DESCENDING"
        return "CRUISE"
 
    df["phase"] = df.apply(flight_phase, axis=1)
 
    # Position source label
    src_map = {0: "ADS-B", 1: "ASTERIX", 2: "MLAT", 3: "FLARM"}
    df["source"] = df["position_source"].map(src_map).fillna("?")
 
    # Filter: airborne only, has callsign, has altitude
    df = df[
        (~df["on_ground"]) &
        (df["callsign"].notna()) &
        (df["callsign"] != "N/A") &
        (df["baro_altitude"].notna())
    ].copy()
 
    return df.sort_values("baro_altitude", ascending=False)
 
 
def display(df, fetch_time):
    """Print a formatted table to the terminal."""
    # Clear terminal
    os.system("cls" if os.name == "nt" else "clear")
 
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print("=" * 100)
    print(f"  ✈   OPENSKY LIVE — Central Europe Airspace          {ts}   ({fetch_time:.1f}s fetch)")
    print(f"       Bounding box: lat {LAMIN}–{LAMAX} N  |  lon {LOMIN}–{LOMAX} E  |  {len(df)} airborne aircraft")
    print("=" * 100)
 
    if df.empty:
        print("  No airborne aircraft found in this bounding box right now.")
        return
 
    display_df = df[[
        "callsign", "origin_country", "latitude", "longitude",
        "baro_altitude_ft", "velocity_kmh", "vertical_rate_fpm", "phase", "source"
    ]].head(MAX_ROWS).copy()
 
    display_df.columns = [
        "Callsign", "Country", "Lat", "Lon",
        "Alt (ft)", "Speed (km/h)", "V/S (fpm)", "Phase", "Source"
    ]
 
    display_df["Lat"]  = display_df["Lat"].round(2)
    display_df["Lon"]  = display_df["Lon"].round(2)
 
    try:
        from tabulate import tabulate
        print(tabulate(display_df, headers="keys", tablefmt="simple",
                       showindex=False, numalign="right"))
    except ImportError:
        print(display_df.to_string(index=False))
 
    print()
    print(f"  Showing top {min(MAX_ROWS, len(df))} of {len(df)} airborne aircraft  |  "
          f"Refreshing every {REFRESH_SECONDS}s  |  Ctrl+C to stop")
    print()
 
    # Quick summary
    countries = df["origin_country"].value_counts().head(5)
    print("  Top countries: " + "  |  ".join(f"{c}: {n}" for c, n in countries.items()))
    phases = df["phase"].value_counts()
    print("  Flight phases: " + "  |  ".join(f"{p}: {n}" for p, n in phases.items()))
    print("=" * 100)
 
 
def main():
    print("\n  OpenSky Live Feed starting...")
    print(f"  Region: Central Europe (lat {LAMIN}–{LAMAX}, lon {LOMIN}–{LOMAX})")
    print(f"  Refresh: every {REFRESH_SECONDS} seconds\n")
 
    iteration = 0
    while True:
        iteration += 1
        t0 = time.time()
        states = fetch_states(LAMIN, LOMIN, LAMAX, LOMAX)
        fetch_time = time.time() - t0
 
        if states:
            df = parse_states(states)
            display(df, fetch_time)
        else:
            print(f"  [{iteration}] No data returned. Retrying in {REFRESH_SECONDS}s...")
 
        time.sleep(REFRESH_SECONDS)
 
 
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n  Stopped. Press Ctrl+C again to exit.\n")