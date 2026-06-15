# ─────────────────────────────────────────────────────────────────────────────
# Databricks Notebook — OpenSky Live Stream from Event Hub → Delta Bronze
#
# BEFORE RUNNING:
# 1. Install Maven library on your cluster:
#    com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22
#    (Cluster → Libraries → Install New → Maven → paste above coordinate)
#
# 2. Restart cluster after library installs.
#
# Run each cell one at a time (Cmd+Enter / Shift+Enter).
# ─────────────────────────────────────────────────────────────────────────────

# ── CELL 1: Config ────────────────────────────────────────────────────────────
CONNECTION_STRING = (
    "Endpoint=sb://akhilseventhub.servicebus.windows.net/;"
    "SharedAccessKeyName=RootManageSharedAccessKey;"
    "SharedAccessKey=kfgM4MOJ1weKeNpEFut430/++JwpzcM9R+AEhIdDAQE=;"
    "EntityPath=opensky-feed"          # <-- required for Spark connector
)

EVENTHUB_NAME    = "opensky-feed"
BRONZE_TABLE     = "opensky_bronze"    # Delta table name
CHECKPOINT_PATH  = "/tmp/opensky_checkpoint"

# Event Hub config dict for Spark connector
ehConf = {
    "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs \
        .EventHubsUtils.encrypt(CONNECTION_STRING)
}


# ── CELL 2: Schema ────────────────────────────────────────────────────────────
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import from_json, col, current_timestamp

schema = StructType([
    StructField("fetched_at",         StringType(),  True),
    StructField("icao24",             StringType(),  True),
    StructField("callsign",           StringType(),  True),
    StructField("origin_country",     StringType(),  True),
    StructField("latitude",           DoubleType(),  True),
    StructField("longitude",          DoubleType(),  True),
    StructField("baro_altitude_ft",   DoubleType(),  True),
    StructField("velocity_kmh",       DoubleType(),  True),
    StructField("vertical_rate_fpm",  DoubleType(),  True),
    StructField("squawk",             StringType(),  True),
])


# ── CELL 3: Read stream from Event Hub ────────────────────────────────────────
raw_stream = (
    spark.readStream
        .format("eventhubs")
        .options(**ehConf)
        .load()
)

# Event Hub delivers the payload as binary in the "body" column — decode it
parsed_stream = (
    raw_stream
        .select(
            col("body").cast("string").alias("raw_json"),
            col("enqueuedTime").alias("enqueued_at")
        )
        .select(
            from_json(col("raw_json"), schema).alias("data"),
            col("enqueued_at")
        )
        .select("data.*", "enqueued_at")
        .withColumn("ingested_at", current_timestamp())
)


# ── CELL 4: Write stream to Delta Bronze ──────────────────────────────────────
query = (
    parsed_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .toTable(BRONZE_TABLE)
)

print(f"Streaming to Delta table: {BRONZE_TABLE}")
print("Stream is running. Open CELL 5 to watch live data arrive.")


# ── CELL 5: Watch live data (run in a separate cell while stream runs) ─────────
# This cell queries the Delta table every few seconds and shows new rows.
# Run this AFTER Cell 4 is running and your local opensky_to_eventhub.py is running.

display(
    spark.readStream
        .format("delta")
        .table(BRONZE_TABLE)
)

# display() in Databricks auto-refreshes — you'll see rows appearing live.


# ── CELL 6: Quick analytics (run anytime after data has arrived) ───────────────
df = spark.read.format("delta").table(BRONZE_TABLE)

print(f"Total rows: {df.count()}")

# Aircraft by country
display(
    df.groupBy("origin_country")
      .count()
      .orderBy("count", ascending=False)
      .limit(10)
)

# Altitude distribution
display(
    df.select("callsign", "baro_altitude_ft", "velocity_kmh", "fetched_at")
      .orderBy("baro_altitude_ft", ascending=False)
      .limit(20)
)
