# sensor_server.py
import os
import time
import math
import asyncio
import logging
from typing import Deque, Optional, List
from collections import deque
from statistics import mean

import asyncpg
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# -------------------------------
# Setup
# -------------------------------
app = FastAPI(title="Heart Rate & SpO2 Server")

# Allow CORS for WebSocket + API (replace "*" with your frontend/android domain in production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Logging
logger = logging.getLogger("sensor_server")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

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
POSTGRES_DSN = os.getenv("DATABASE_URL")
if not POSTGRES_DSN:
    logger.error("DATABASE_URL is not set in environment")
    raise RuntimeError("DATABASE_URL is not set in environment")

pg_pool: Optional[asyncpg.pool.Pool] = None

# -------------------------------
# HTTP Endpoints
# -------------------------------
@app.post("/upload")
async def upload_sensor_data(pkt: VitalPacket):
    """Receive a single sensor packet and store it in memory & DB"""
    global latest, latest_server_ts, second_buffer, second_start_time

    try:
        latest = pkt
        latest_server_ts = time.time()
        history.append(pkt)
        logger.info(f"Received packet: {pkt.deviceId} bpm={pkt.bpm} spo2={pkt.spo2_pct}")

        # Add to per-second buffer
        if second_start_time is None:
            second_start_time = time.time()
        second_buffer.append(pkt)

        # Broadcast to WebSocket clients
        disconnected = []
        for ws in connected_clients:
            try:
                await ws.send_json(pkt.dict())
            except Exception as e:
                logger.warning(f"WebSocket disconnected: {e}")
                disconnected.append(ws)
        for ws in disconnected:
            connected_clients.remove(ws)

        # Store in DB
        if pg_pool:
            try:
                async with pg_pool.acquire() as conn:
                    await conn.execute(
                        """
                        INSERT INTO raw_packets (device_id, ts_ms, ax_g, ay_g, az_g, bpm, spo2_pct)
                        VALUES ($1,$2,$3,$4,$5,$6,$7)
                        """,
                        pkt.deviceId, pkt.ts_ms, pkt.ax_g, pkt.ay_g, pkt.az_g, pkt.bpm, pkt.spo2_pct
                    )
            except Exception as e:
                logger.error(f"Failed to store packet in DB: {e}")

        return {"status": "ok", "count": len(history)}

    except Exception as e:
        logger.error(f"Error in /upload: {e}")
        return JSONResponse({"message": str(e)}, status_code=500)

@app.get("/data/latest")
def get_latest():
    """Get the latest packet"""
    try:
        if latest is None:
            logger.info("No latest packet available")
            return JSONResponse({"message": "No data from sensor yet"}, status_code=404)
        return {"received_at": latest_server_ts, "packet": latest.dict()}
    except Exception as e:
        logger.error(f"Error in /data/latest: {e}")
        return JSONResponse({"message": str(e)}, status_code=500)

# -------------------------------
# WebSocket endpoint
# -------------------------------
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    """Real-time data push to clients (Android app)"""
    await ws.accept()
    connected_clients.append(ws)
    logger.info("WebSocket client connected")

    try:
        while True:
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected")
        connected_clients.remove(ws)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        if ws in connected_clients:
            connected_clients.remove(ws)
        await ws.close()

# -------------------------------
# Background per-second aggregator (optional, for idle detection)
# -------------------------------
async def per_second_aggregator_task():
    global second_buffer, second_start_time, idle_seconds_today
    logger.info("Per-second aggregator task started")

    while True:
        try:
            await asyncio.sleep(0.5)
            if second_start_time is None or len(second_buffer) == 0:
                continue

            elapsed = time.time() - second_start_time
            if elapsed >= 1.0:
                avg_ax = mean([p.ax_g for p in second_buffer])
                avg_ay = mean([p.ay_g for p in second_buffer])
                avg_az = mean([p.az_g for p in second_buffer])
                magnitude = math.sqrt(avg_ax**2 + avg_ay**2 + (avg_az - 1)**2)

                if magnitude < 0.55:
                    idle_seconds_today += 1

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

        except Exception as e:
            logger.error(f"Error in per-second aggregator: {e}")

# -------------------------------
# Startup
# -------------------------------
@app.on_event("startup")
async def startup_event():
    global pg_pool
    try:
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
        # Start background aggregator
        asyncio.create_task(per_second_aggregator_task())
        logger.info("Background per-second aggregator started")
    except Exception as e:
        logger.error(f"Startup error: {e}")
        raise

# -------------------------------
# Run server
# -------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    logger.info(f"Starting server on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
