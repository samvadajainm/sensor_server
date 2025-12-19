# sensor_server.py
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Deque, Optional
from collections import deque
import time
import uvicorn

app = FastAPI()

class VitalPacket(BaseModel):
    deviceId: str = Field(..., examples=["CERIS_D1"])
    ts_ms: int
    ax_g: float
    ay_g: float
    az_g: float
    bpm: int
    spo2_pct: float

# Keep a rolling buffer of the last N packets
N_BUFFER = 300  # ~5 minutes if 1 pkt/sec
history: Deque[VitalPacket] = deque(maxlen=N_BUFFER)
latest: Optional[VitalPacket] = None
latest_server_ts: Optional[float] = None

@app.post("/upload")
async def upload_sensor_data(pkt: VitalPacket):
    global latest, latest_server_ts
    latest = pkt
    latest_server_ts = time.time()
    history.append(pkt)
    return {"status": "ok", "count": len(history)}

@app.get("/data/latest")
def get_latest():
    if latest is None:
        return JSONResponse(content={"error": "No data yet"}, status_code=404)
    return {
        "received_at": latest_server_ts,
        "packet": latest.dict(),
    }

@app.get("/data/recent")
def get_recent(limit: int = 60):
    if not history:
        return JSONResponse(content={"error": "No data yet"}, status_code=404)
    limit = max(1, min(limit, len(history)))
    return [p.dict() for p in list(history)[-limit:]]

@app.get("/health")
def health():
    return {"ok": True, "count": len(history)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)