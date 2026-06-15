"""
OpenSky -> Azure Event Hub via Kafka Protocol (confluent-kafka)
Run locally in VS Code.

pip install confluent-kafka requests
"""

import json
import time
import requests
from datetime import datetime, timezone
from confluent_kafka import Producer, KafkaException

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = "akhilseventhub.servicebus.windows.net:9093"
KAFKA_TOPIC     = "opensky-feed"
KAFKA_PASSWORD  = (
    "Endpoint=sb://akhilseventhub.servicebus.windows.net/;"
    "SharedAccessKeyName=RootManageSharedAccessKey;"
    "SharedAccessKey=kfgM4MOJ1weKeNpEFut430/++JwpzcM9R+AEhIdDAQE="
)

LAMIN, LOMIN    = 47.0, 7.0
LAMAX, LOMAX    = 53.5, 15.0
REFRESH_SECONDS = 30

FIELDS = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
    "true_track", "vertical_rate", "sensors", "geo_altitude",
    "squawk", "spi", "position_source",
]

# ── Confluent Kafka Producer ──────────────────────────────────────────────────
def create_producer():
    return Producer({
        "bootstrap.servers":          KAFKA_BOOTSTRAP,
        "security.protocol":          "SASL_SSL",
        "sasl.mechanism":             "PLAIN",
        "sasl.username":              "$ConnectionString",
        "sasl.password":              KAFKA_PASSWORD,
        "socket.timeout.ms":          30000,
        "message.timeout.ms":         30000,
        "connections.max.idle.ms":    180000,
        "reconnect.backoff.ms":       1000,
        "reconnect.backoff.max.ms":   10000,
    })

def delivery_report(err, msg):
    if err:
        print(f"  Delivery failed: {err}")

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

# ── Parse ─────────────────────────────────────────────────────────────────────
def parse(states, fetched_at):
    records = []
    for s in states:
        row = dict(zip(FIELDS, s))
        if row.get("on_ground") or not row.get("baro_altitude") or not row.get("callsign"):
            continue
        records.append({
            "fetched_at":         fetched_at,
            "icao24":             row["icao24"],
            "callsign":           row["callsign"].strip(),
            "origin_country":     row["origin_country"],
            "latitude":           round(row["latitude"] or 0, 4),
            "longitude":          round(row["longitude"] or 0, 4),
            "baro_altitude_ft":   round((row["baro_altitude"] or 0) * 3.28084, 0),
            "velocity_kmh":       round((row["velocity"] or 0) * 3.6, 1),
            "vertical_rate_fpm":  round((row["vertical_rate"] or 0) * 196.85, 0),
            "squawk":             row.get("squawk", ""),
        })
    return records

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print(f"\n  OpenSky -> Kafka/Event Hub [{KAFKA_TOPIC}]")
    print(f"  Bootstrap : {KAFKA_BOOTSTRAP}")
    print(f"  Connecting to Kafka...", end=" ", flush=True)

    producer = create_producer()
    print("Connected.\n")

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
            sent = 0
            for record in records:
                producer.poll(0)
                try:
                    producer.produce(
                        KAFKA_TOPIC,
                        value=json.dumps(record).encode("utf-8"),
                        callback=delivery_report
                    )
                    sent += 1
                except KafkaException as e:
                    print(f"\n  Queue error, recreating producer: {e}")
                    producer = create_producer()
            producer.flush()
            total_sent += sent
            print(f"{sent} events sent  |  {total_sent} total sent")

        time.sleep(REFRESH_SECONDS)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n  Stopped.\n")