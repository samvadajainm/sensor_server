# sensor_server.py
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import time
import uvicorn
from typing import Optional

app = FastAPI()

# Store only the latest validated packet
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

# Thresholds to detect valid readings
MIN_BPM = 30
MAX_BPM = 220
MIN_SPO2 = 70
MAX_SPO2 = 100

def is_valid_reading(pkt: VitalPacket) -> bool:
    """Return True if the sensor reading is likely valid."""
    if pkt.bpm < MIN_BPM or pkt.bpm > MAX_BPM:
        return False
    if pkt.spo2_pct < MIN_SPO2 or pkt.spo2_pct > MAX_SPO2:
        return False
    # Optional: ignore packets with zero motion? (ax/ay/az too low)
    return True

@app.post("/upload")
async def upload_sensor_data(pkt: VitalPacket):
    global latest, latest_server_ts
    latest_server_ts = time.time()
    if is_valid_reading(pkt):
        latest = pkt
    return {"status": "ok", "valid": is_valid_reading(pkt)}

@app.get("/data/latest")
def get_latest():
    if latest is None:
        return JSONResponse(content={"error": "No valid data yet"}, status_code=404)
    return {
        "received_at": latest_server_ts,
        "packet": latest.dict(),
    }

@app.get("/data/recent")
def get_recent(limit: int = 60):
    # Without history, just repeat the latest valid packet
    if latest is None:
        return JSONResponse(content={"error": "No valid data yet"}, status_code=404)
    return [latest.dict()] * limit

@app.get("/health")
def health():
    return {"ok": True, "latest_received": latest_server_ts is not None}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
