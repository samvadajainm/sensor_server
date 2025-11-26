# sensor_server.py
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Deque, Optional, List
from collections import deque
import time
import asyncio
import uvicorn
import logging
import os
import math
from statistics import mean
from datetime import datetime
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

second_buffer: List[VitalPacket] = []
second_start_time: Optional[float] = None
idle_seconds_today: int = 0

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
    global latest, latest_server_ts, second_buffer, second_start_time

    latest = pkt
    latest_server_ts = time.time()
    history.append(pkt)

    # Add to second buffer
    if second_start_time is None:
        second_start_time = time.time()
    second_buffer.append(pkt)

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
    """Fetch idle minutes from DB for today"""
    if pg_pool is None:
        return JSONResponse({"message": "DB not initialized"}, status_code=500)

    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT idle_minutes FROM idle_time WHERE day = CURRENT_DATE"
        )
        idle_minutes = row["idle_minutes"] if row else 0
    return {"idle_minutes": idle_minutes}

@app.get("/health")
def health():
    return {"ok": True, "buffer_size": len(history)}

@app.get("/data/24h")
async def get_24h_graph():
    """Return last 24 hours of minute averages including variance"""
    if pg_pool is None:
        return JSONResponse({"message": "DB not initialized"}, status_code=500)

    async with pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT minute_start, bpm AS mean_bpm, var_bpm
            FROM minute_average
            WHERE minute_start >= NOW() - INTERVAL '24 hours'
            ORDER BY minute_start ASC
            """
        )

        data = []
        for r in rows:
            ts = int(r["minute_start"].timestamp() * 1000)
            mean_bpm = r["mean_bpm"]
            var_bpm = r["var_bpm"]
            data.append({
                "timestamp": ts,
                "mean_bpm": mean_bpm,
                "var_bpm": var_bpm
            })
            # Log the values
            print(f"[24h Graph] ts={ts}, mean_bpm={mean_bpm}, var_bpm={var_bpm}")

        print(f"[24h Graph] Total points: {len(data)}")
        return data



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
# Background task: per-second idle calculation
# -------------------------------
async def per_second_aggregator_task():
    global second_buffer, second_start_time, idle_seconds_today

    while True:
        await asyncio.sleep(0.5)
        if second_start_time is None or len(second_buffer) == 0:
            continue

        elapsed = time.time() - second_start_time
        if elapsed >= 1.0:
            # Compute average acceleration for the second
            avg_ax = mean([p.ax_g for p in second_buffer])
            avg_ay = mean([p.ay_g for p in second_buffer])
            avg_az = mean([p.az_g for p in second_buffer])

            magnitude = math.sqrt(avg_ax**2 + avg_ay**2 + (avg_az - 1)**2)
            if magnitude < 0.55:
                idle_seconds_today += 1

            # Clear buffer for next second
            second_buffer = []
            second_start_time = time.time()

            # Update DB every 60 seconds
            if idle_seconds_today % 60 == 0 and pg_pool:
                async with pg_pool.acquire() as conn:
                    await conn.execute(
                        """
                        INSERT INTO idle_time(day, idle_minutes)
                        VALUES (CURRENT_DATE, $1)
                        ON CONFLICT (day) DO UPDATE SET idle_minutes = $1
                        """,
                        idle_seconds_today // 60
                    )

            logger.debug(f"[Idle] magnitude={magnitude:.3f}, idle_seconds_today={idle_seconds_today}")

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
        CREATE TABLE IF NOT EXISTS idle_time (
            day DATE PRIMARY KEY,
            idle_minutes INT
        )
        """)

    # Start background task
    asyncio.create_task(per_second_aggregator_task())

# -------------------------------
# Run server
# -------------------------------
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
