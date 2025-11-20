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
import asyncpg
from statistics import mean
from datetime import datetime, timedelta

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
N_BUFFER = 10_000  # enough for ~100 packets/sec for 100 seconds
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
        connected_clients.remove(ws)

    # Optional: Store raw packet in PostgreSQL (comment if not needed)
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
    if pg_pool is None:
        return JSONResponse(content={"message": "Database not initialized"}, status_code=500)

    async with pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT minute_start, ax_g, ay_g, az_g, bpm, spo2_pct
            FROM minute_average
            WHERE minute_start >= NOW() - INTERVAL '24 hours'
            ORDER BY minute_start ASC
            """
        )
    return [
        {
            "minute_start": r["minute_start"].isoformat(),
            "ax_g": r["ax_g"],
            "ay_g": r["ay_g"],
            "az_g": r["az_g"],
            "bpm": r["bpm"],
            "spo2_pct": r["spo2_pct"],
        } for r in rows
    ]


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
    try:
        while True:
            await asyncio.sleep(1)  # keep connection alive
    except WebSocketDisconnect:
        connected_clients.remove(ws)
    except Exception:
        connected_clients.remove(ws)
        await ws.close()

# -------------------------------
# Background task for periodic computation and database storage
# -------------------------------
async def minute_aggregator_task():
    global minute_buffer, minute_start_time

    while True:
        await asyncio.sleep(1)  # check every second

        if minute_start_time is None or len(minute_buffer) == 0:
            continue

        elapsed = time.time() - minute_start_time
        if elapsed >= 60:  # 1 minute passed
            # Compute averages
            avg_ax = mean(p.ax_g for p in minute_buffer)
            avg_ay = mean(p.ay_g for p in minute_buffer)
            avg_az = mean(p.az_g for p in minute_buffer)
            avg_bpm = mean(p.bpm for p in minute_buffer)
            avg_spo2 = mean(p.spo2_pct for p in minute_buffer)

            minute_start_dt = datetime.utcfromtimestamp(minute_start_time)

            # Store computed averages in PostgreSQL
            if pg_pool:
                async with pg_pool.acquire() as conn:
                    await conn.execute(
                        """
                        INSERT INTO minute_average (minute_start, ax_g, ay_g, az_g, bpm, spo2_pct)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        """,
                        minute_start_dt, avg_ax, avg_ay, avg_az, avg_bpm, avg_spo2
                    )

                    # Delete older than 24 hours
                    await conn.execute(
                        "DELETE FROM minute_average WHERE minute_start < NOW() - INTERVAL '24 hours'"
                    )

            # Clear buffer
            minute_buffer = []
            minute_start_time = time.time()


# -------------------------------
# Startup event: initialize DB and background tasks
# -------------------------------
@app.on_event("startup")
async def startup_event():
    global pg_pool
    # Initialize PostgreSQL pool
    pg_pool = await asyncpg.create_pool(dsn=POSTGRES_DSN, min_size=1, max_size=5)
    logger.info("PostgreSQL connection pool created")

    # Ensure tables exist
    async with pg_pool.acquire() as conn:
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
            ax_g REAL,
            ay_g REAL,
            az_g REAL,
            bpm REAL,
            spo2_pct REAL
        )
        """)

    # Start background tasks
    asyncio.create_task(minute_aggregator_task())

    # Existing optional logging task
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
