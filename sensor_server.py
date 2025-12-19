# sensor_server.py
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import time
import uvicorn
from typing import Optional

app = FastAPI()

# Store only the latest packet
latest: Optional["VitalPacket"] = None
latest_server_ts: Optional[float] = None

class VitalPacket(BaseModel):
    deviceId: str = Field(..., examples=["CERIS_D1"])
    ts_ms: int
    ax_g: float
    ay_g: float
    az_g: float
    bpm: int
    spo2_pct: float

@app.post("/upload")
async def upload_sensor_data(pkt: VitalPacket):
    global latest, latest_server_ts
    latest = pkt
    latest_server_ts = time.time()
    return {"status": "ok"}

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
    # Without history or DB, just repeat the latest packet
    if latest is None:
        return JSONResponse(content={"error": "No data yet"}, status_code=404)
    return [latest.dict()] * limit

@app.get("/health")
def health():
    return {"ok": True, "latest_received": latest_server_ts is not None}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
