# sensor_server.py
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Deque, Optional, List
from collections import deque
import time
import asyncio
import uvicorn
import logging
import os
import json
import asyncpg
from statistics import mean
from datetime import datetime, timedelta
import math

app = FastAPI(title="High-Frequency Sensor Server")
logger = logging.getLogger("sensor_server")
logging.basicConfig(level=logging.INFO)

# -------------------------------
# Model for incoming sensor data
# -------------------------------
class VitalPacket(BaseModel):
    deviceId: str = Field(..., example="CERIS_D1")
    ts_ms: int
    ax_g: float
    ay_g: float
    az_g: float
    bpm: int
    spo2_pct: float

# -------------------------------
# Rolling buffer
# -------------------------------
N_BUFFER = 10_000
history: Deque[VitalPacket] = deque(maxlen=N_BUFFER)
latest: Optional[VitalPacket] = None
latest_server_ts: Optional[float] = None

# -------------------------------
# WebSocket clients
# -------------------------------
connected_clients: List[WebSocket] = []

# -------------------------------
# PostgreSQL configuration
# -------------------------------
POSTGRES_DSN = os.getenv(
    "DATABASE_URL",
    "postgresql://sensordata_twcy_user:QMpGEMAS0nfjTgOAvOmP0qnDGPZajLIV@localhost/sensordata_twcy"
)
pg_pool: Optional[asyncpg.pool.Pool] = None

# -------------------------------
# Minute-level raw buffer for computation
# -------------------------------
minute_buffer: List[VitalPacket] = []
minute_start_time: Optional[float] = None

# -------------------------------
# Idle time tracking
# -------------------------------
last_idle_reset: Optional[datetime] = None
idle_minutes_today: int = 0

# -------------------------------
# HTTP Endpoints
# -------------------------------
@app.post("/upload")
async def upload_sensor_data(pkt: VitalPacket):
    global latest, latest_server_ts, minute_buffer, minute_start_time

    latest = pkt
    latest_server_ts = time.time()
    history.append(pkt)

    # Append to minute buffer
    if minute_start_time is None:
        minute_start_time = time.time()
    minute_buffer.append(pkt)

    # Forward to WebSocket clients
    disconnected = []
    for ws in connected_clients:
        try:
            await ws.send_json(pkt.dict())
        except Exception:
            disconnected.append(ws)
    for ws in disconnected:
        try:
            connected_clients.remove(ws)
        except ValueError:
            pass

    # Optional: Store raw packet in PostgreSQL
    if pg_pool:
        async with pg_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO raw_packets (device_id, ts_ms, ax_g, ay_g, az_g, bpm, spo2_pct)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                """,
                pkt.deviceId, pkt.ts_ms, pkt.ax_g, pkt.ay_g, pkt.az_g, pkt.bpm, pkt.spo2_pct
            )

    return {"status": "ok", "count": len(history)}

@app.get("/data/latest")
def get_latest():
    if latest is None:
        return JSONResponse(content={"message": "No data from sensor yet"}, status_code=404)
    return {
        "received_at": latest_server_ts,
        "packet": latest.dict(),
    }

@app.get("/data/recent")
def get_recent(limit: int = 100):
    if not history:
        return JSONResponse(content={"message": "No data from sensor yet"}, status_code=404)
    limit = max(1, min(limit, len(history)))
    return [p.dict() for p in list(history)[-limit:]]

@app.get("/data/24h")
async def get_24h_data():
    """
    Returns per-minute statistics for the last 24 hours.
    Each minute row includes mean, variance and std for ax/ay/az/bpm/spo2.
    """
    if pg_pool is None:
        return JSONResponse(content={"message": "Database not initialized"}, status_code=500)

    async with pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT minute_start,
                   ax_g as mean_ax, ay_g as mean_ay, az_g as mean_az,
                   var_ax, var_ay, var_az,
                   std_ax, std_ay, std_az,
                   bpm as mean_bpm, var_bpm, std_bpm,
                   spo2_pct as mean_spo2, var_spo2, std_spo2
            FROM minute_average
            WHERE minute_start >= NOW() - INTERVAL '24 hours'
            ORDER BY minute_start ASC
            """
        )

    result = []
    for r in rows:
        result.append({
            "minute_start": r["minute_start"].isoformat(),
            "mean_ax": r["mean_ax"],
            "mean_ay": r["mean_ay"],
            "mean_az": r["mean_az"],
            "var_ax": r["var_ax"],
            "var_ay": r["var_ay"],
            "var_az": r["var_az"],
            "std_ax": r["std_ax"],
            "std_ay": r["std_ay"],
            "std_az": r["std_az"],
            "mean_bpm": r["mean_bpm"],
            "var_bpm": r["var_bpm"],
            "std_bpm": r["std_bpm"],
            "mean_spo2": r["mean_spo2"],
            "var_spo2": r["var_spo2"],
            "std_spo2": r["std_spo2"],
        })
    return result

@app.get("/data/idle_time")
async def get_idle_time():
    """
    Returns idle minutes since midnight (server local / UTC as configured).
    Counts minutes where magnitude (from mean axes) < threshold.
    """
    global last_idle_reset, idle_minutes_today

    now = datetime.utcnow()

    # Reset at midnight UTC
    if last_idle_reset is None or now.date() != last_idle_reset.date():
        idle_minutes_today = 0
        last_idle_reset = now

    idle_count = 0
    if pg_pool:
        async with pg_pool.acquire() as conn:
            # Use mean axes recorded per minute to compute magnitude
            rows = await conn.fetch(
                """
                SELECT mean_ax, mean_ay, mean_az FROM minute_average
                WHERE minute_start >= DATE_TRUNC('day', NOW())
                """
            )
            for r in rows:
                # some rows may be null if data was not populated; guard against that
                ma = r.get("mean_ax") or 0.0
                mb = r.get("mean_ay") or 0.0
                mc = r.get("mean_az") or 0.0
                magnitude = math.sqrt((ma ** 2) + (mb ** 2) + ((mc - 1) ** 2))
                if magnitude < 0.55:
                    idle_count += 1

    idle_minutes_today = idle_count
    return {"idle_minutes": idle_minutes_today}

@app.get("/health")
def health():
    return {"ok": True, "buffer_size": len(history)}

# -------------------------------
# WebSocket endpoint
# -------------------------------
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    connected_clients.append(ws)

    async def send_idle_time_periodically():
        while True:
            if pg_pool:
                async with pg_pool.acquire() as conn:
                    idle_count = await conn.fetchval("""
                        SELECT COUNT(*) FROM (
                          SELECT SQRT(POWER(ax_g, 2) + POWER(ay_g, 2) + POWER(az_g - 1, 2)) AS magnitude
                          FROM minute_average
                          WHERE minute_start >= NOW() - INTERVAL '24 hours'
                        ) AS sub
                        WHERE magnitude < 0.55
                    """)
                try:
                    await ws.send_json({"type": "idle_time", "idle_minutes": idle_count})
                except Exception:
                    break
            await asyncio.sleep(60)

    # Start background task for this WebSocket client
    asyncio.create_task(send_idle_time_periodically())
    try:
        while True:
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        try:
            connected_clients.remove(ws)
        except ValueError:
            pass
    except Exception:
        try:
            connected_clients.remove(ws)
        except ValueError:
            pass
        await ws.close()

# -------------------------------
# Background task for periodic computation and database storage
# -------------------------------
async def minute_aggregator_task():
    """
    Every minute aggregates raw packets in minute_buffer and stores:
      - mean for each signal
      - variance for each signal
      - std (sqrt variance) for each signal
    """
    global minute_buffer, minute_start_time

    while True:
        await asyncio.sleep(1)

        if minute_start_time is None or len(minute_buffer) == 0:
            continue

        elapsed = time.time() - minute_start_time
        if elapsed >= 60:
            # compute means
            ax_list = [p.ax_g for p in minute_buffer]
            ay_list = [p.ay_g for p in minute_buffer]
            az_list = [p.az_g for p in minute_buffer]
            bpm_list = [float(p.bpm) for p in minute_buffer]
            spo2_list = [p.spo2_pct for p in minute_buffer]

            # defensive: if lists empty (should not happen) skip
            if not ax_list:
                minute_buffer = []
                minute_start_time = time.time()
                continue

            mean_ax = mean(ax_list)
            mean_ay = mean(ay_list)
            mean_az = mean(az_list)
            mean_bpm = mean(bpm_list)
            mean_spo2 = mean(spo2_list)

            # compute variances (population variance: divide by n)
            var_ax = mean([(x - mean_ax) ** 2 for x in ax_list])
            var_ay = mean([(y - mean_ay) ** 2 for y in ay_list])
            var_az = mean([(z - mean_az) ** 2 for z in az_list])
            var_bpm = mean([(b - mean_bpm) ** 2 for b in bpm_list])
            var_spo2 = mean([(s - mean_spo2) ** 2 for s in spo2_list])

            # standard deviations
            std_ax = math.sqrt(var_ax)
            std_ay = math.sqrt(var_ay)
            std_az = math.sqrt(var_az)
            std_bpm = math.sqrt(var_bpm)
            std_spo2 = math.sqrt(var_spo2)

            minute_start_dt = datetime.utcfromtimestamp(minute_start_time)

            # Store computed aggregates in PostgreSQL
            if pg_pool:
                async with pg_pool.acquire() as conn:
                    # ensure columns exist (safe to run on every startup; will be ignored if already present)
                    await conn.execute("""
                    CREATE TABLE IF NOT EXISTS minute_average (
                        id SERIAL PRIMARY KEY,
                        minute_start TIMESTAMP,
                        ax_g REAL, ay_g REAL, az_g REAL,
                        var_ax REAL, var_ay REAL, var_az REAL,
                        std_ax REAL, std_ay REAL, std_az REAL,
                        bpm REAL, var_bpm REAL, std_bpm REAL,
                        spo2_pct REAL, var_spo2 REAL, std_spo2 REAL
                    )
                    """)

                    await conn.execute(
                        """
                        INSERT INTO minute_average (
                          minute_start,
                          ax_g, ay_g, az_g,
                          var_ax, var_ay, var_az,
                          std_ax, std_ay, std_az,
                          bpm, var_bpm, std_bpm,
                          spo2_pct, var_spo2, std_spo2
                        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
                        """,
                        minute_start_dt,
                        mean_ax, mean_ay, mean_az,
                        var_ax, var_ay, var_az,
                        std_ax, std_ay, std_az,
                        mean_bpm, var_bpm, std_bpm,
                        mean_spo2, var_spo2, std_spo2
                    )

                    # Delete older than 24 hours
                    await conn.execute(
                        "DELETE FROM minute_average WHERE minute_start < NOW() - INTERVAL '24 hours'"
                    )

            # Clear buffer
            minute_buffer = []
            minute_start_time = time.time()

# -------------------------------
# Startup event
# -------------------------------
@app.on_event("startup")
async def startup_event():
    global pg_pool
    pg_pool = await asyncpg.create_pool(dsn=POSTGRES_DSN, min_size=1, max_size=5)
    logger.info("PostgreSQL connection pool created")

    async with pg_pool.acquire() as conn:
        # ensure raw_packets table exists
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_packets (
            id SERIAL PRIMARY KEY,
            device_id TEXT,
            ts_ms BIGINT,
            ax_g REAL,
            ay_g REAL,
            az_g REAL,
            bpm INT,
            spo2_pct REAL,
            received_at TIMESTAMP DEFAULT now()
        )
        """)

        # create minute_average if not exists (columns included)
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS minute_average (
            id SERIAL PRIMARY KEY,
            minute_start TIMESTAMP,
            ax_g REAL, ay_g REAL, az_g REAL,
            var_ax REAL, var_ay REAL, var_az REAL,
            std_ax REAL, std_ay REAL, std_az REAL,
            bpm REAL, var_bpm REAL, std_bpm REAL,
            spo2_pct REAL, var_spo2 REAL, std_spo2 REAL
        )
        """)

        # in case older schema exists without variance/std columns, try to add them (safe: ignore errors)
        # asyncpg will raise if column exists; to be robust we check information_schema
        cols = await conn.fetch(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_name='minute_average'
            """
        )
        existing = {r["column_name"] for r in cols}
        alterations = []
        if "var_ax" not in existing:
            alterations.append("ALTER TABLE minute_average ADD COLUMN var_ax REAL")
        if "var_ay" not in existing:
            alterations.append("ALTER TABLE minute_average ADD COLUMN var_ay REAL")
        if "var_az" not in existing:
            alterations.append("ALTER TABLE minute_average ADD COLUMN var_az REAL")
        if "std_ax" not in existing:
            alterations.append("ALTER TABLE minute_average ADD COLUMN std_ax REAL")
        if "std_ay" not in existing:
            alterations.append("ALTER TABLE minute_average ADD COLUMN std_ay REAL")
        if "std_az" not in existing:
            alterations.append("ALTER TABLE minute_average ADD COLUMN std_az REAL")
        if "var_bpm" not in existing:
            alterations.append("ALTER TABLE minute_average ADD COLUMN var_bpm REAL")
        if "std_bpm" not in existing:
            alterations.append("ALTER TABLE minute_average ADD COLUMN std_bpm REAL")
        if "var_spo2" not in existing:
            alterations.append("ALTER TABLE minute_average ADD COLUMN var_spo2 REAL")
        if "std_spo2" not in existing:
            alterations.append("ALTER TABLE minute_average ADD COLUMN std_spo2 REAL")

        for stmt in alterations:
            try:
                await conn.execute(stmt)
            except Exception:
                logger.exception("Failed to alter table with stmt: %s", stmt)

    # Start background tasks
    asyncio.create_task(minute_aggregator_task())

    # Optional logging task
    async def periodic_task():
        while True:
            await asyncio.sleep(0.1)
            if latest:
                logger.info(f"[0.1s Task] Latest packet: {latest.dict()}")
    asyncio.create_task(periodic_task())

# -------------------------------
# Run server
# -------------------------------
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
