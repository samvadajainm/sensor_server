# sensor_server.py
from fastapi import FastAPI, WebSocket
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Deque, Optional, List
from collections import deque
import time
import uvicorn
import asyncio

app = FastAPI(title="High-Frequency Sensor Server")

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
# Rolling buffer and WebSocket clients
# -------------------------------
N_BUFFER = 10_000  # store ~100 packets/sec for 100 seconds
history: Deque[VitalPacket] = deque(maxlen=N_BUFFER)
latest: Optional[VitalPacket] = None
latest_server_ts: Optional[float] = None

connected_clients: List[WebSocket] = []

# -------------------------------
# HTTP Endpoints
# -------------------------------
@app.post("/upload")
async def upload_sensor_data(pkt: VitalPacket):
    """
    Receive a sensor packet and immediately broadcast it to all connected WebSockets.
    """
    global latest, latest_server_ts
    latest = pkt
    latest_server_ts = time.time()
    history.append(pkt)

    # Push immediately to all connected WebSocket clients
    disconnected = []
    for ws in connected_clients:
        try:
            await ws.send_json(pkt.dict())
        except Exception:
            disconnected.append(ws)
    # Remove disconnected clients
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

@app.on_event("startup")
async def start_background_task():
    async def periodic_task():
        while True:
            await asyncio.sleep(0.5)
            if latest:
                print(f"[0.5s Task] Latest packet: {latest.dict()}")
    asyncio.create_task(periodic_task())

# -------------------------------
# WebSocket Endpoint
# -------------------------------
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.append(websocket)
    try:
        while True:
            await asyncio.sleep(1)  # keep connection alive
    except Exception:
        pass
    finally:
        connected_clients.remove(websocket)

# -------------------------------
# Run server
# -------------------------------
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
