# sensor_server.py
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
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

@app.post("/upload")
async def upload_sensor_data(pkt: VitalPacket):
    # Simply acknowledge receipt, do not store
    return {"status": "received"}

@app.get("/data/latest")
def get_latest():
    # Cannot provide anything since nothing is stored
    return JSONResponse(content={"error": "No data stored on server"}, status_code=404)

@app.get("/data/recent")
def get_recent():
    # Cannot provide anything since nothing is stored
    return JSONResponse(content={"error": "No data stored on server"}, status_code=404)

@app.get("/health")
def health():
    return {"ok": True, "message": "Server running, no data stored"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
