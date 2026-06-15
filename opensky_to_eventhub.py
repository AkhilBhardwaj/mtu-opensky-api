"""
OpenSky → Azure Event Hub
Run locally in VS Code. Polls OpenSky every 30 seconds and sends each
aircraft as a separate JSON event to Event Hub.

Install dependency first:
    pip install azure-eventhub requests
"""

import json
import time
import requests
from datetime import datetime, timezone
from azure.eventhub import EventHubProducerClient, EventData

# ── Config ────────────────────────────────────────────────────────────────────
CONNECTION_STRING = (
    "Endpoint=sb://akhilseventhub.servicebus.windows.net/;"
    "SharedAccessKeyName=RootManageSharedAccessKey;"
    "SharedAccessKey=kfgM4MOJ1weKeNpEFut430/++JwpzcM9R+AEhIdDAQE="
)
EVENTHUB_NAME   = "opensky-feed"

LAMIN, LOMIN    = 47.0, 7.0
LAMAX, LOMAX    = 53.5, 15.0
REFRESH_SECONDS = 30

FIELDS = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
    "true_track", "vertical_rate", "sensors", "geo_altitude",
    "squawk", "spi", "position_source",
]

# ── Fetch from OpenSky ────────────────────────────────────────────────────────
def fetch_states():
    url = "https://opensky-network.org/api/states/all"
    params = {"lamin": LAMIN, "lomin": LOMIN, "lamax": LAMAX, "lomax": LOMAX}
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        return r.json().get("states", []) or []
    except Exception as e:
        print(f"  Fetch error: {e}")
        return []

# ── Parse into clean dicts ────────────────────────────────────────────────────
def parse(states, fetched_at):
    records = []
    for s in states:
        row = dict(zip(FIELDS, s))

        # Skip ground traffic or missing data
        if row.get("on_ground") or not row.get("baro_altitude") or not row.get("callsign"):
            continue

        records.append({
            "fetched_at":          fetched_at,
            "icao24":              row["icao24"],
            "callsign":            row["callsign"].strip(),
            "origin_country":      row["origin_country"],
            "latitude":            round(row["latitude"] or 0, 4),
            "longitude":           round(row["longitude"] or 0, 4),
            "baro_altitude_ft":    round((row["baro_altitude"] or 0) * 3.28084, 0),
            "velocity_kmh":        round((row["velocity"] or 0) * 3.6, 1),
            "vertical_rate_fpm":   round((row["vertical_rate"] or 0) * 196.85, 0),
            "squawk":              row.get("squawk", ""),
        })
    return records

# ── Send to Event Hub ─────────────────────────────────────────────────────────
def send_to_eventhub(records):
    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STRING,
        eventhub_name=EVENTHUB_NAME
    )
    with producer:
        batch = producer.create_batch()
        for record in records:
            batch.add(EventData(json.dumps(record)))
        producer.send_batch(batch)

# ── Main loop ─────────────────────────────────────────────────────────────────
def main():
    print(f"\n  OpenSky → Event Hub [{EVENTHUB_NAME}]")
    print(f"  Namespace : akhilseventhub.servicebus.windows.net")
    print(f"  Region    : Central Europe (lat {LAMIN}–{LAMAX}, lon {LOMIN}–{LOMAX})")
    print(f"  Refresh   : every {REFRESH_SECONDS}s  |  Ctrl+C to stop\n")

    iteration  = 0
    total_sent = 0

    while True:
        iteration += 1
        fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        print(f"  [{fetched_at}]  Fetch #{iteration} ...", end=" ", flush=True)
        states = fetch_states()

        if not states:
            print("no data")
        else:
            records = parse(states, fetched_at)
            send_to_eventhub(records)
            total_sent += len(records)
            print(f"{len(records)} events sent  |  {total_sent} total sent")

        time.sleep(REFRESH_SECONDS)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n  Stopped.\n")
