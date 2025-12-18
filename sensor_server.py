# sensor_server.py
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, List
import time
import asyncio
import uvicorn
import logging
import os
import math
import asyncpg

# -------------------------------
# Setup
# -------------------------------
app = FastAPI(title="High-Frequency Sensor Server")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sensor_server")

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
    server_ts: Optional[float] = None

# -------------------------------
# Latest sensor packet
# -------------------------------
latest: Optional[VitalPacket] = None
latest_server_ts: Optional[float] = None

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
    global latest, latest_server_ts

    latest = pkt
    latest_server_ts = time.time()
    pkt.server_ts = latest_server_ts

    # Prepare for DB / WS, scale BPM only if >0
    pkt_to_store = pkt.dict()
    if pkt.bpm is not None and pkt.bpm > 0:
        pkt_to_store['bpm'] = pkt.bpm / 2

    # Forward to WebSocket clients
    disconnected = []
    for ws in connected_clients:
        try:
            await ws.send_json(pkt_to_store)
        except Exception as e:
            disconnected.append(ws)
    for ws in disconnected:
        connected_clients.remove(ws)

    # Store in DB
    if pg_pool:
        async with pg_pool.acquire() as conn:
            try:
                await conn.execute(
                    """
                    INSERT INTO raw_packets (device_id, ts_ms, ax_g, ay_g, az_g, bpm, spo2_pct)
                    VALUES ($1,$2,$3,$4,$5,$6,$7)
                    """,
                    pkt_to_store['deviceId'],
                    pkt_to_store['ts_ms'],
                    pkt_to_store['ax_g'],
                    pkt_to_store['ay_g'],
                    pkt_to_store['az_g'],
                    pkt_to_store['bpm'],
                    pkt_to_store['spo2_pct']
                )
            except Exception as e:
                logger.error(f"[Upload] Failed to insert packet into DB: {e}")

    return {"status": "ok"}

@app.get("/data/latest")
def get_latest():
    if latest is None:
        return JSONResponse({"message": "No data from sensor yet"}, status_code=404)
    return {"received_at": latest_server_ts, "packet": latest.dict()}

@app.get("/data/recent")
def get_recent(limit: int = 100):
    # Query last N rows directly from DB (no history buffer)
    if pg_pool is None:
        return JSONResponse({"message": "DB not initialized"}, status_code=500)

    async def fetch_recent():
        async with pg_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT device_id, ts_ms, ax_g, ay_g, az_g, bpm, spo2_pct
                FROM raw_packets
                ORDER BY id DESC
                LIMIT $1
                """,
                limit
            )
            return [dict(r) for r in rows]

    # Run async inside sync endpoint
    return asyncio.run(fetch_recent())

# -------------------------------
# Health check
# -------------------------------
@app.get("/health")
def health():
    return {"ok": True}

# -------------------------------
# WebSocket endpoint
# -------------------------------
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    connected_clients.append(ws)
    try:
        while True:
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        connected_clients.remove(ws)
    except Exception:
        connected_clients.remove(ws)
        await ws.close()

# -------------------------------
# Startup
# -------------------------------
@app.on_event("startup")
async def startup_event():
    global pg_pool
    pg_pool = await asyncpg.create_pool(dsn=POSTGRES_DSN, min_size=1, max_size=5)
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
        );
        """)

# -------------------------------
# Run server
# -------------------------------
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
