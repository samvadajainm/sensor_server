# sensor_server.py
from fastapi import FastAPI, Request, WebSocket
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
# Rolling buffer setup
# -------------------------------
N_BUFFER = 10_000  # enough for ~100 packets/sec for 100 seconds
history: Deque[VitalPacket] = deque(maxlen=N_BUFFER)
latest: Optional[VitalPacket] = None
latest_server_ts: Optional[float] = None

# -------------------------------
# HTTP Endpoints
# -------------------------------
@app.post("/upload")
async def upload_sensor_data(pkt: VitalPacket):
    """
    Endpoint to receive sensor packets (called ~100 times per second).
    """
    global latest, latest_server_ts
    latest = pkt
    latest_server_ts = time.time()
    history.append(pkt)
    return {"status": "ok", "count": len(history)}

@app.get("/data/latest")
def get_latest():
    """
    Return the most recent sensor packet.
    """
    if latest is None:
        return JSONResponse(content={"error": "No data yet"}, status_code=404)
    return {
        "received_at": latest_server_ts,
        "packet": latest.dict(),
    }

@app.get("/data/recent")
def get_recent(limit: int = 100):
    """
    Return the most recent N packets.
    """
    if not history:
        return JSONResponse(content={"error": "No data yet"}, status_code=404)
    limit = max(1, min(limit, len(history)))
    return [p.dict() for p in list(history)[-limit:]]

@app.get("/health")
def health():
    """
    Health check endpoint.
    """
    return {"ok": True, "buffer_size": len(history)}

# -------------------------------
# WebSocket for real-time streaming
# -------------------------------
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    last_index = len(history)
    try:
        while True:
            await asyncio.sleep(0.01)  # 10ms interval = ~100Hz
            if len(history) > last_index:
                new_data = [p.dict() for p in list(history)[last_index:]]
                await websocket.send_json(new_data)
                last_index = len(history)
    except Exception:
        await websocket.close()

# -------------------------------
# Run server
# -------------------------------
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)