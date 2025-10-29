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
# HTTP Endpoints
# -------------------------------
@app.post("/upload")
async def upload_sensor_data(pkt: VitalPacket):
    global latest, latest_server_ts
    latest = pkt
    latest_server_ts = time.time()
    history.append(pkt)

    disconnected = []
    logger.info(f"[Upload] Received packet from sensor: {pkt.dict()}")
    for ws in connected_clients:
        try:
            logger.info(f"[WebSocket] Sending packet to client #{i}")
            await ws.send_json(pkt.dict())
            logger.info(f"[WebSocket] Sent packet to client: {pkt.dict()}")
        except Exception:
            logger.info(f"[WebSocket] exception")
            disconnected.append(ws)
    for ws in disconnected:
        connected_clients.remove(ws)
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
    logger.info(f"[WebSocket] Client connected, total clients: {len(connected_clients)}")
    try:
        while True:
            await asyncio.sleep(1)  # keep connection alive
    except WebSocketDisconnect:
        connected_clients.remove(ws)
        logger.info(f"[WebSocket] Client disconnected, total clients: {len(connected_clients)}")
    except Exception as e:
        connected_clients.remove(ws)
        logger.error(f"[WebSocket] Exception: {e}")
        await ws.close()


# -------------------------------
# Background task for logging (optional)
# -------------------------------
@app.on_event("startup")
async def start_background_task():
    async def periodic_task():
        while True:
            await asyncio.sleep(0.5)  # half a second
            if latest:
               logger.info(f"[0.5s Task] Latest packet: {latest.dict()}")
    asyncio.create_task(periodic_task())


# -------------------------------
# Run server
# -------------------------------
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
