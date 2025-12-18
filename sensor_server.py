# sensor_server.py
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import uvicorn
import time

app = FastAPI()

# Store latest sensor data in memory
latest_data = {"timestamp": None, "value": None}

@app.post("/upload")
async def upload_sensor_data(request: Request):
    """
    Sensor band calls this endpoint to upload data
    Example body: {"value": 25.6}
    """
    data = await request.json()
    latest_data["timestamp"] = time.time()
    latest_data["value"] = data.get("value")
    return {"status": "success", "received": latest_data}

@app.get("/data")
def get_sensor_data():
    """
    App fetches latest data every second
    """
    if latest_data["timestamp"]:
        return JSONResponse(content=latest_data)
    else:
        return JSONResponse(content={"error": "No data yet"}, status_code=404)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)