# sensor_server.py
import time
import math
import os
import asyncio
import logging
from typing import Deque, Optional, List
from collections import deque
from statistics import mean

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# -------------------------------
# Setup
# -------------------------------
app = FastAPI(title="Heart Rate & SpO2 Server")

# Allow CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Change "*" to your frontend domain in production
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
    ir_raw: int
    finger: int
    bpm: int
    spo2_pct: float

# -------------------------------
# Buffers & latest packet
# -------------------------------
N_BUFFER = 10_000
history: Deque[VitalPacket] = deque(maxlen=N_BUFFER)
latest: Optional[VitalPacket] = None
latest_server_ts: Optional[float] = None
last_finger: Optional[int] = None

# Idle time
second_buffer: List[VitalPacket] = []
second_start_time: Optional[float] = None
idle_seconds_today: int = 0

# Step detection
step_seconds_buffer: List[VitalPacket] = []
step_start_time: Optional[float] = None
steps_today: int = 0

# -------------------------------
# WebSocket clients
# -------------------------------
connected_clients: List[WebSocket] = []

# -------------------------------
# HTTP Endpoints
# -------------------------------
@app.post("/upload")
async def upload_sensor_data(pkt: VitalPacket):
    """Receive a single sensor packet"""
    global latest, latest_server_ts, second_buffer, second_start_time, last_finger
    global step_seconds_buffer, step_start_time

    try:
        latest = pkt
        latest_server_ts = time.time()
        last_finger = pkt.finger
        history.append(pkt)

        logger.info(f"Received packet: {pkt.deviceId} bpm={pkt.bpm} spo2={pkt.spo2_pct} finger={pkt.finger}")

        # Idle buffer
        if second_start_time is None:
            second_start_time = time.time()
        second_buffer.append(pkt)

        # Step buffer
        if step_start_time is None:
            step_start_time = time.time()
        step_seconds_buffer.append(pkt)

        # Broadcast to WebSocket clients
        ws_payload = {"received_at": latest_server_ts, "packet": pkt.dict(), "last_finger": last_finger}
        disconnected = []
        for ws in connected_clients:
            try:
                await ws.send_json(ws_payload)
            except Exception as e:
                logger.warning(f"WebSocket disconnected: {e}")
                disconnected.append(ws)
        for ws in disconnected:
            connected_clients.remove(ws)

        return {"status": "ok", "count": len(history)}

    except Exception as e:
        logger.error(f"Error in /upload: {e}")
        return JSONResponse({"message": str(e)}, status_code=500)


@app.get("/data/latest")
def get_latest():
    """Return the latest packet"""
    try:
        if latest is None:
            return JSONResponse({"message": "No data from sensor yet"}, status_code=404)
        return {"received_at": latest_server_ts, "packet": latest.dict(), "last_finger": last_finger}
    except Exception as e:
        logger.error(f"Error in /data/latest: {e}")
        return JSONResponse({"message": str(e)}, status_code=500)


@app.get("/data/recent")
def get_recent(limit: int = 100):
    """Return recent N packets"""
    try:
        if not history:
            return JSONResponse({"message": "No data from sensor yet"}, status_code=404)
        limit = max(1, min(limit, len(history)))
        return [p.dict() for p in list(history)[-limit:]]
    except Exception as e:
        logger.error(f"Error in /data/recent: {e}")
        return JSONResponse({"message": str(e)}, status_code=500)


@app.get("/health")
def health():
    try:
        return {"ok": True, "buffer_size": len(history)}
    except Exception as e:
        logger.error(f"Error in /health: {e}")
        return JSONResponse({"message": str(e)}, status_code=500)


@app.get("/data/steps")
def get_steps():
    """Return today's step count"""
    try:
        return {"steps_today": steps_today}
    except Exception as e:
        return JSONResponse({"message": str(e)}, status_code=500)


@app.get("/data/idle_time")
def get_idle_time():
    """Return today's idle minutes"""
    try:
        idle_minutes_today = idle_seconds_today // 60
        return {"idle_minutes": idle_minutes_today}
    except Exception as e:
        return JSONResponse({"message": str(e)}, status_code=500)


# -------------------------------
# WebSocket endpoint
# -------------------------------
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    connected_clients.append(ws)
    logger.info("WebSocket client connected")
    try:
        while True:
            await asyncio.sleep(10)
    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected")
        connected_clients.remove(ws)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        if ws in connected_clients:
            connected_clients.remove(ws)
        await ws.close()


# -------------------------------
# Background per-second aggregator (idle time)
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
                logger.debug(f"[Idle] magnitude={magnitude:.3f}, idle_seconds_today={idle_seconds_today}")

        except Exception as e:
            logger.error(f"Error in per-second aggregator: {e}")


# -------------------------------
# Background per-second step detection
# -------------------------------
async def per_second_step_task():
    global step_seconds_buffer, step_start_time, steps_today
    logger.info("Per-second step task started")

    while True:
        try:
            await asyncio.sleep(0.5)
            if step_start_time is None or len(step_seconds_buffer) == 0:
                continue

            elapsed = time.time() - step_start_time
            if elapsed >= 1.0:
                avg_ax = mean([p.ax_g for p in step_seconds_buffer])
                avg_ay = mean([p.ay_g for p in step_seconds_buffer])
                avg_az = mean([p.az_g for p in step_seconds_buffer])
                magnitude = math.sqrt(avg_ax**2 + avg_ay**2 + (avg_az - 1)**2)

                if magnitude > 0.55:
                    steps_today += 1

                step_seconds_buffer = []
                step_start_time = time.time()
                logger.debug(f"[Steps] magnitude={magnitude:.3f}, steps_today={steps_today}")

        except Exception as e:
            logger.error(f"Error in per-second step task: {e}")


# -------------------------------
# Startup
# -------------------------------
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(per_second_aggregator_task())
    asyncio.create_task(per_second_step_task())
    logger.info("Background per-second aggregator and step detection started")


# -------------------------------
# Run server
# -------------------------------
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    logger.info(f"Starting server on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
