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
import math
from statistics import mean
from datetime import datetime, timedelta
import asyncpg

# -------------------------------
# Setup
# -------------------------------
app = FastAPI(title="High-Frequency Sensor Server")
logger = logging.getLogger("sensor_server")
logging.basicConfig(level=logging.INFO)

# -------------------------------
# Sensor data model
# -------------------------------
class VitalPacket(BaseModel):
    deviceId: str
    ts_ms: int
    ax_g: float
    ay_g: float
    az_g: float
    bpm: int
    spo2_pct: float

# -------------------------------
# Buffers
# -------------------------------
N_BUFFER = 10_000
history: Deque[VitalPacket] = deque(maxlen=N_BUFFER)
latest: Optional[VitalPacket] = None
latest_server_ts: Optional[float] = None

minute_buffer: List[VitalPacket] = []
minute_start_time: Optional[float] = None

# -------------------------------
# WebSocket clients
# -------------------------------
connected_clients: List[WebSocket] = []

# -------------------------------
# PostgreSQL config
# -------------------------------
POSTGRES_DSN = os.getenv(
    "DATABASE_URL",
    "postgresql://sensordata_twcy_user:QMpGEMAS0nfjTgOAvOmP0qnDGPZajLIV@localhost/sensordata_twcy"
)
pg_pool: Optional[asyncpg.pool.Pool] = None

# -------------------------------
# HTTP Endpoints
# -------------------------------
@app.post("/upload")
async def upload_sensor_data(pkt: VitalPacket):
    """Receive a single sensor packet and store it in memory & DB"""
    global latest, latest_server_ts, minute_buffer, minute_start_time

    latest = pkt
    latest_server_ts = time.time()
    history.append(pkt)

    # Add to minute buffer
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
        connected_clients.remove(ws)

    # Store raw packet in DB
    if pg_pool:
        async with pg_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO raw_packets (device_id, ts_ms, ax_g, ay_g, az_g, bpm, spo2_pct)
                VALUES ($1,$2,$3,$4,$5,$6,$7)
                """,
                pkt.deviceId, pkt.ts_ms, pkt.ax_g, pkt.ay_g, pkt.az_g, pkt.bpm, pkt.spo2_pct
            )

    return {"status": "ok", "count": len(history)}

@app.get("/data/latest")
def get_latest():
    if latest is None:
        return JSONResponse({"message": "No data from sensor yet"}, status_code=404)
    return {"received_at": latest_server_ts, "packet": latest.dict()}

@app.get("/data/recent")
def get_recent(limit: int = 100):
    if not history:
        return JSONResponse({"message": "No data from sensor yet"}, status_code=404)
    limit = max(1, min(limit, len(history)))
    return [p.dict() for p in list(history)[-limit:]]

@app.get("/data/idle_time")
async def get_idle_time():
    """Fetch the precomputed idle minutes from DB for today"""
    if pg_pool is None:
        return JSONResponse({"message": "DB not initialized"}, status_code=500)

    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT idle_minutes
            FROM idle_time
            WHERE day = CURRENT_DATE
            """
        )
        idle_minutes = row["idle_minutes"] if row else 0
    return {"idle_minutes": idle_minutes}

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
                    row = await conn.fetchrow(
                        "SELECT idle_minutes FROM idle_time WHERE day = CURRENT_DATE"
                    )
                    idle_minutes = row["idle_minutes"] if row else 0
                    logger.info(f"[Idle Time Endpoint] Returning idle_minutes={idle_minutes}")

                    try:
                        await ws.send_json({"type": "idle_time", "idle_minutes": idle_minutes})
                    except Exception:
                        break
            await asyncio.sleep(60)

    asyncio.create_task(send_idle_time_periodically())

    try:
        while True:
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        connected_clients.remove(ws)
    except Exception:
        connected_clients.remove(ws)
        await ws.close()

# -------------------------------
# Background task: Minute aggregation + idle time
# -------------------------------
async def minute_aggregator_task():
    global minute_buffer, minute_start_time

    while True:
        await asyncio.sleep(1)
        if minute_start_time is None or len(minute_buffer) == 0:
            continue

        elapsed = time.time() - minute_start_time
        if elapsed >= 60:
            # Compute minute averages
            ax_list = [p.ax_g for p in minute_buffer]
            ay_list = [p.ay_g for p in minute_buffer]
            az_list = [p.az_g for p in minute_buffer]
            bpm_list = [float(p.bpm) for p in minute_buffer]
            spo2_list = [p.spo2_pct for p in minute_buffer]

            mean_ax = mean(ax_list)
            mean_ay = mean(ay_list)
            mean_az = mean(az_list)
            mean_bpm = mean(bpm_list)
            mean_spo2 = mean(spo2_list)

            var_ax = mean([(x - mean_ax)**2 for x in ax_list])
            var_ay = mean([(y - mean_ay)**2 for y in ay_list])
            var_az = mean([(z - mean_az)**2 for z in az_list])
            var_bpm = mean([(b - mean_bpm)**2 for b in bpm_list])
            var_spo2 = mean([(s - mean_spo2)**2 for s in spo2_list])

            std_ax = math.sqrt(var_ax)
            std_ay = math.sqrt(var_ay)
            std_az = math.sqrt(var_az)
            std_bpm = math.sqrt(var_bpm)
            std_spo2 = math.sqrt(var_spo2)

            minute_start_dt = datetime.utcnow()

            if pg_pool:
                async with pg_pool.acquire() as conn:
                    # Store minute aggregates
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

                    # Compute idle minutes for today
                    rows = await conn.fetch(
                        "SELECT ax_g, ay_g, az_g FROM minute_average WHERE minute_start >= DATE_TRUNC('day', NOW())"
                    )
                    idle_count = 0
                    for r in rows:
                        magnitude = math.sqrt(r["ax_g"]**2 + r["ay_g"]**2 + (r["az_g"] - 1)**2)
                        if magnitude < 0.55:
                            idle_count += 1

                    # Upsert into idle_time table
                    await conn.execute(
                        """
                        INSERT INTO idle_time(day, idle_minutes)
                        VALUES (CURRENT_DATE, $1)
                        ON CONFLICT (day) DO UPDATE SET idle_minutes = $1
                        """,
                        idle_count
                    )

                    # Delete old minute averages (>24h)
                    await conn.execute(
                        "DELETE FROM minute_average WHERE minute_start < NOW() - INTERVAL '24 hours'"
                    )

            # Clear buffer
            logger.info(
    f"[Minute Aggregate] {minute_start_dt}: "
    f"var_ax={var_ax:.4f}, var_ay={var_ay:.4f}, var_az={var_az:.4f}, "
    f"var_bpm={var_bpm:.2f}, var_spo2={var_spo2:.2f}, "
    f"idle_minutes={idle_count}"
)

            minute_buffer = []
            minute_start_time = time.time()

# -------------------------------
# Startup
# -------------------------------
@app.on_event("startup")
async def startup_event():
    global pg_pool
    pg_pool = await asyncpg.create_pool(dsn=POSTGRES_DSN, min_size=1, max_size=5)
    logger.info("PostgreSQL connection pool created")

    async with pg_pool.acquire() as conn:
        # Create tables if missing
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

        await conn.execute("""
        CREATE TABLE IF NOT EXISTS idle_time (
            day DATE PRIMARY KEY,
            idle_minutes INT
        )
        """)

    # Start background task
    asyncio.create_task(minute_aggregator_task())

# -------------------------------
# Run server
# -------------------------------
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
