from fastapi import FastAPI, HTTPException, Query
from databricks import sql
import os
from dotenv import load_dotenv
import logging

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="OpenSky Flight API",
    description="Live flight data from Databricks Silver layer — MTU demo",
    version="1.0.0"
)

DATABRICKS_SERVER_HOSTNAME = os.getenv("DATABRICKS_SERVER_HOSTNAME")
DATABRICKS_HTTP_PATH       = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN           = os.getenv("DATABRICKS_TOKEN")
SILVER_TABLE               = "akhils2nddatabricks.default.opensky_silver"
ANOMALY_TABLE              = "default.engine_anomalies"


def get_conn():
    return sql.connect(
        server_hostname=DATABRICKS_SERVER_HOSTNAME,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
    )


def rows_to_dicts(cursor):
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health", tags=["meta"])
def health():
    return {"status": "ok"}


# ── Flights ───────────────────────────────────────────────────────────────────

@app.get("/flights", tags=["flights"])
def get_flights(limit: int = Query(50, ge=1, le=500)):
    """
    Latest N flights from the Silver table, newest first.
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT icao24, callsign, origin_country,
                           latitude, longitude, baro_altitude_ft,
                           velocity_kmh, vertical_rate_fpm, squawk, fetched_at
                    FROM {SILVER_TABLE}
                    ORDER BY fetched_at DESC
                    LIMIT {limit}
                """)
                return rows_to_dicts(cur)
    except Exception as e:
        logger.error(f"/flights error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/flights/{icao24}", tags=["flights"])
def get_flight_history(icao24: str, limit: int = Query(100, ge=1, le=1000)):
    """
    Full history for a specific aircraft (by ICAO24 hex code), newest first.
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT icao24, callsign, origin_country,
                           latitude, longitude, baro_altitude_ft,
                           velocity_kmh, vertical_rate_fpm, squawk, fetched_at
                    FROM {SILVER_TABLE}
                    WHERE icao24 = ?
                    ORDER BY fetched_at DESC
                    LIMIT {limit}
                    """,
                    [icao24]
                )
                result = rows_to_dicts(cur)
                if not result:
                    raise HTTPException(
                        status_code=404,
                        detail=f"No data found for aircraft '{icao24}'"
                    )
                return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"/flights/{icao24} error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ── Anomalies ─────────────────────────────────────────────────────────────────

@app.get("/anomalies", tags=["anomalies"])
def get_anomalies(
    engine_id: str = Query(None, description="Filter by engine ID e.g. MTU-PW1100G-003"),
    severity:  str = Query(None, description="Filter by severity: CRITICAL, WARNING, INFO"),
    limit:     int = Query(50, ge=1, le=500)
):
    """
    Engine sensor anomalies detected by Isolation Forest.
    Pre-scored by Databricks ML notebook, served from Delta table.
    Ordered by anomaly_score ascending (most anomalous first).
    """
    try:
        where_clauses = []
        params = []
        if engine_id:
            where_clauses.append("engine_id = ?")
            params.append(engine_id)
        if severity:
            where_clauses.append("severity = ?")
            params.append(severity.upper())
        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT engine_id, flight_cycle, phase, severity,
                           ROUND(egt_celsius, 1)      AS egt_celsius,
                           ROUND(n1_pct, 2)           AS n1_pct,
                           ROUND(oil_pressure_bar, 3) AS oil_pressure_bar,
                           ROUND(vibration_g, 4)      AS vibration_g,
                           ROUND(fuel_flow_kg_hr, 1)  AS fuel_flow_kg_hr,
                           ROUND(anomaly_score, 4)    AS anomaly_score,
                           event_time
                    FROM {ANOMALY_TABLE}
                    {where_sql}
                    ORDER BY anomaly_score ASC
                    LIMIT {limit}
                    """,
                    params or None
                )
                results = rows_to_dicts(cur)
                return {
                    "total_returned": len(results),
                    "filters": {"engine_id": engine_id, "severity": severity},
                    "anomalies": results
                }
    except Exception as e:
        logger.error(f"/anomalies error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ── Stats ─────────────────────────────────────────────────────────────────────

@app.get("/stats", tags=["stats"])
def get_stats():
    """
    Aggregates over the last hour:
    - Top 10 countries by distinct aircraft count
    - Average altitude, average speed, total unique aircraft
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:

                # Top countries
                cur.execute(f"""
                    SELECT origin_country,
                           COUNT(DISTINCT icao24) AS aircraft_count
                    FROM {SILVER_TABLE}
                    WHERE fetched_at >= current_timestamp() - INTERVAL 1 HOUR
                      AND origin_country IS NOT NULL
                    GROUP BY origin_country
                    ORDER BY aircraft_count DESC
                    LIMIT 10
                """)
                top_countries = rows_to_dicts(cur)

                # Summary metrics
                cur.execute(f"""
                    SELECT
                        COUNT(DISTINCT icao24)             AS total_aircraft,
                        ROUND(AVG(baro_altitude_ft), 0)   AS avg_altitude_ft,
                        ROUND(AVG(velocity_kmh), 1)       AS avg_velocity_kmh,
                        ROUND(AVG(vertical_rate_fpm), 1)  AS avg_vertical_rate_fpm,
                        MIN(fetched_at)                   AS data_from,
                        MAX(fetched_at)                   AS data_to
                    FROM {SILVER_TABLE}
                    WHERE fetched_at >= current_timestamp() - INTERVAL 1 HOUR
                      AND baro_altitude_ft IS NOT NULL
                """)
                summary_rows = rows_to_dicts(cur)
                summary = summary_rows[0] if summary_rows else {}

        return {
            "summary": summary,
            "top_countries": top_countries,
        }
    except Exception as e:
        logger.error(f"/stats error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
