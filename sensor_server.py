from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import time

app = FastAPI()

latest_data = {}

@app.post("/upload")
async def upload_sensor_data(request: Request):
    data = await request.json()
    latest_data["timestamp"] = time.time()
    latest_data["data"] = data
    print("Received:", data)  # Logs in Render dashboard
    return {"status": "success", "received": data}

@app.get("/data")
def get_latest_data():
    if latest_data:
        return JSONResponse(content=latest_data)
    return JSONResponse(content={"error": "No data yet"}, status_code=404)
