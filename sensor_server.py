# sensor_server.py
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Deque, Optional, List
from collections import deque
from datetime import datetime, timezone
from statistics import mean, variance, pstdev
import time
import asyncio
import uvicorn
import logging
import os
import math
from statistics import mean, variance
from datetime import datetime
import asyncpg

# -------------------------------
# Setup
# -------------------------------
app = FastAPI(title="High-Frequency Sensor Server")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sensor_server")

# -------------------------------
# Sensor data model
# -------------------------------
class VitalPacket(BaseModel):
    deviceId: str
    ts_ms: int
    ax_g: float
    ay_g: float
    az_g: float
    bpm: int
    spo2_pct: float
    server_ts: Optional[float] = None

# -------------------------------
# Buffers
# -------------------------------
N_BUFFER = 10_000
history: Deque[VitalPacket] = deque(maxlen=N_BUFFER)
latest: Optional[VitalPacket] = None
latest_server_ts: Optional[float] = None

second_buffer: List[VitalPacket] = []
second_start_time: Optional[float] = None
minute_buffer: List[VitalPacket] = []
idle_seconds_today: int = 0

# -------------------------------
# WebSocket clients
# -------------------------------
connected_clients: List[WebSocket] = []

# -------------------------------
# PostgreSQL config
# -------------------------------
POSTGRES_DSN = os.getenv(
    "DATABASE_URL",
    "postgresql://sensordata_twcy_user:QMpGEMAS0nfjTgOAvOmP0qnDGPZajLIV@localhost/sensordata_twcy"
)
pg_pool: Optional[asyncpg.pool.Pool] = None

# -------------------------------
# HTTP Endpoints
# -------------------------------
@app.post("/upload")
async def upload_sensor_data(pkt: VitalPacket):
    global latest, latest_server_ts, second_buffer, second_start_time, minute_buffer

    logger.info(f"[Upload] Received packet: {pkt}")

    # Update latest packet and timestamp
    latest = pkt
    latest_server_ts = time.time()
    pkt.server_ts = latest_server_ts 
    history.append(pkt)
    logger.info(f"[Upload] Appended to history, history length={len(history)}")

    # Add to second buffer
    if second_start_time is None:
        second_start_time = time.time()
        logger.info(f"[Upload] Initialized second_start_time={second_start_time}")
    second_buffer.append(pkt)
    logger.info(f"[Upload] Added to second_buffer, length={len(second_buffer)}")

    # Add to minute buffer
    if 'minute_buffer' not in globals():
        minute_buffer = []
        logger.info(f"[Upload] Initialized minute_buffer")
    minute_buffer.append(pkt)
    logger.info(f"[Upload] Added to minute_buffer, length={len(minute_buffer)}")

    # Forward to WebSocket clients
    disconnected = []
    for ws in connected_clients:
        try:
            await ws.send_json(pkt.dict())
            logger.info(f"[Upload] Sent packet to WebSocket client")
        except Exception as e:
            logger.warning(f"[Upload] WebSocket send failed: {e}")
            disconnected.append(ws)
    for ws in disconnected:
        connected_clients.remove(ws)
        logger.info(f"[Upload] Removed disconnected WebSocket client, remaining clients={len(connected_clients)}")

    # Store raw packet in DB
    if pg_pool:
        async with pg_pool.acquire() as conn:
            try:
                await conn.execute(
                    """
                    INSERT INTO raw_packets (device_id, ts_ms, ax_g, ay_g, az_g, bpm, spo2_pct)
                    VALUES ($1,$2,$3,$4,$5,$6,$7)
                    """,
                    pkt.deviceId, pkt.ts_ms, pkt.ax_g, pkt.ay_g, pkt.az_g, pkt.bpm, pkt.spo2_pct
                )
                logger.info(f"[Upload] Inserted packet into DB")
            except Exception as e:
                logger.error(f"[Upload] Failed to insert packet into DB: {e}")

    return {"status": "ok", "count": len(history)}

@app.get("/data/latest")
def get_latest():
    logger.info("message")
    if latest is None:
        return JSONResponse({"message": "No data from sensor yet"}, status_code=404)
    return {"received_at": latest_server_ts, "packet": latest.dict()}

@app.get("/data/recent")
def get_recent(limit: int = 100):
    if not history:
        return JSONResponse({"message": "No data from sensor yet"}, status_code=404)
    limit = max(1, min(limit, len(history)))
    return [p.dict() for p in list(history)[-limit:]]

@app.get("/data/idle_time")
async def get_idle_time():
    if pg_pool is None:
        return JSONResponse({"message": "DB not initialized"}, status_code=500)

    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT idle_minutes FROM idle_time WHERE day = CURRENT_DATE"
        )
        idle_minutes = row["idle_minutes"] if row else 0
    return {"idle_minutes": idle_minutes}

@app.get("/health")
def health():
    return {"ok": True, "buffer_size": len(history)}

@app.get("/data/24h")
async def get_24h_graph():
    if pg_pool is None:
        return JSONResponse({"message": "DB not initialized"}, status_code=500)

    async with pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT minute_start, bpm AS mean_bpm, var_bpm
            FROM minute_average
            WHERE minute_start >= NOW() - INTERVAL '24 hours'
            ORDER BY minute_start ASC
            """
        )

        data = []
        for r in rows:
            ts = int(r["minute_start"].timestamp() * 1000)
            mean_bpm = r["mean_bpm"]
            var_bpm = r["var_bpm"]
            data.append({
                "timestamp": ts,
                "mean": mean_bpm,
                "variance": var_bpm
            })
            logger.info(f"[24h Graph] ts={ts}, mean={mean_bpm}, var={var_bpm}")

        logger.info(f"[24h Graph] Total points: {len(data)}")
        return data
    """Return last 24 hours of minute averages including variance"""
    if pg_pool is None:
        return JSONResponse({"message": "DB not initialized"}, status_code=500)

    async with pg_pool.acquire() as conn:
        # Compute cutoff timestamp in epoch seconds
        cutoff_epoch = time.time() - 24 * 3600  # 24 hours ago

        # Fetch rows where minute_start >= cutoff_epoch
        rows = await conn.fetch(
            """
            SELECT minute_start, bpm AS mean_bpm, var_bpm
            FROM minute_average
            WHERE extract(epoch from minute_start) >= $1
            ORDER BY minute_start ASC
            """,
            cutoff_epoch
        )

        data = []
        for r in rows:
            ts = int(r["minute_start"].timestamp() * 1000)  # convert to ms
            mean_bpm = r["mean_bpm"]
            var_bpm = r["var_bpm"]
            data.append({
                "timestamp": ts,
                "mean": mean_bpm,
                "variance": var_bpm
            })
            # Optional logging
            print(f"[24h Graph] ts={ts}, mean={mean_bpm}, var={var_bpm}")

        print(f"[24h Graph] Total points: {len(data)}")
        return data

# -------------------------------
# WebSocket endpoint
# -------------------------------
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    connected_clients.append(ws)

    async def send_idle_time_periodically():
        while True:
            if pg_pool:
                async with pg_pool.acquire() as conn:
                    row = await conn.fetchrow(
                        "SELECT idle_minutes FROM idle_time WHERE day = CURRENT_DATE"
                    )
                    idle_minutes = row["idle_minutes"] if row else 0
                    try:
                        await ws.send_json({"type": "idle_time", "idle_minutes": idle_minutes})
                    except Exception:
                        break
            await asyncio.sleep(60)

    asyncio.create_task(send_idle_time_periodically())

    try:
        while True:
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        connected_clients.remove(ws)
    except Exception:
        connected_clients.remove(ws)
        await ws.close()

# -------------------------------
# Background task: per-second idle calculation
# -------------------------------
async def per_second_aggregator_task():
    global second_buffer, second_start_time, idle_seconds_today

    while True:
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

            if idle_seconds_today % 60 == 0 and pg_pool:
                async with pg_pool.acquire() as conn:
                    await conn.execute(
                        """
                        INSERT INTO idle_time(day, idle_minutes)
                        VALUES (CURRENT_DATE, $1)
                        ON CONFLICT (day) DO UPDATE SET idle_minutes = $1
                        """,
                        idle_seconds_today // 60
                    )

            logger.debug(f"[Idle] magnitude={magnitude:.3f}, idle_seconds_today={idle_seconds_today}")

# -----------------------------------------------------------
# Background task: per-minute aggregation (ACC + BPM + SPO2)
# -----------------------------------------------------------
# -------------------------------
# Background task: per-minute BPM aggregation with variance
# -------------------------------
async def per_minute_aggregation_task():
    logger.info("[Minute Agg] Background task started")
    
    while True:
        logger.info("[Minute Agg] Tick - starting aggregation cycle")
        await asyncio.sleep(60)  # run every minute

        if pg_pool is None:
            logger.info("[Minute Agg] pg_pool is not initialized, skipping")
            continue

        if not history:
            logger.info("[Minute Agg] history buffer is empty, skipping this cycle")
            continue

        # Use server timestamp for the minute
        now_ts = time.time()

        # Copy packets from history
        last_minute_packets = list(history)
        logger.info(f"[Minute Agg] Found {len(last_minute_packets)} packets in last minute")

        # Collect raw values
        ax_vals = [p.ax_g for p in last_minute_packets]
        ay_vals = [p.ay_g for p in last_minute_packets]
        az_vals = [p.az_g for p in last_minute_packets]
        bpm_vals = [p.bpm for p in last_minute_packets if p.bpm is not None]
        spo2_vals = [p.spo2_pct for p in last_minute_packets if p.spo2_pct is not None]

        # Helper: compute mean, variance, std; return None if empty
        def safe_stats(values):
            if not values:
                return None, None, None
            if len(values) == 1:
                return values[0], 0.0, 0.0
            m = mean(values)
            v = variance(values)
            s = math.sqrt(v)
            return m, v, s

        ax_mean, ax_var, ax_std = safe_stats(ax_vals)
        ay_mean, ay_var, ay_std = safe_stats(ay_vals)
        az_mean, az_var, az_std = safe_stats(az_vals)
        bpm_mean, bpm_var, bpm_std = safe_stats(bpm_vals)
        spo2_mean, spo2_var, spo2_std = safe_stats(spo2_vals)

        logger.info(f"[Minute Agg] Computed stats - AX({ax_mean},{ax_var},{ax_std}) "
                    f"AY({ay_mean},{ay_var},{ay_std}) AZ({az_mean},{az_var},{az_std}) "
                    f"BPM({bpm_mean},{bpm_var},{bpm_std}) SPO2({spo2_mean},{spo2_var},{spo2_std})")

        # Insert into DB safely
        async with pg_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO minute_average (
                    minute_start,
                    ax_g, ay_g, az_g,
                    bpm, spo2_pct,
                    var_ax, var_ay, var_az,
                    std_ax, std_ay, std_az,
                    var_bpm, std_bpm,
                    var_spo2, std_spo2
                )
                VALUES (
                    to_timestamp($1),
                    $2, $3, $4,
                    $5, $6,
                    $7, $8, $9,
                    $10, $11, $12,
                    $13, $14,
                    $15, $16
                )
                ON CONFLICT (minute_start)
                DO UPDATE SET
                    ax_g      = EXCLUDED.ax_g,
                    ay_g      = EXCLUDED.ay_g,
                    az_g      = EXCLUDED.az_g,
                    bpm       = EXCLUDED.bpm,
                    spo2_pct  = EXCLUDED.spo2_pct,
                    var_ax    = EXCLUDED.var_ax,
                    var_ay    = EXCLUDED.var_ay,
                    var_az    = EXCLUDED.var_az,
                    std_ax    = EXCLUDED.std_ax,
                    std_ay    = EXCLUDED.std_ay,
                    std_az    = EXCLUDED.std_az,
                    var_bpm   = EXCLUDED.var_bpm,
                    std_bpm   = EXCLUDED.std_bpm,
                    var_spo2  = EXCLUDED.var_spo2,
                    std_spo2  = EXCLUDED.std_spo2
                """,
                float(now_ts),   # timestamp in seconds
                ax_mean, ay_mean, az_mean,
                bpm_mean, spo2_mean,
                ax_var, ay_var, az_var,
                ax_std, ay_std, az_std,
                bpm_var, bpm_std,
                spo2_var, spo2_std
            )

        # Clear history after aggregation
        history.clear()
        logger.info("[Minute Agg] Aggregation done, history cleared")

# -------------------------------
# Startup
# -------------------------------
@app.on_event("startup")
async def startup_event():
    global pg_pool
    pg_pool = await asyncpg.create_pool(dsn=POSTGRES_DSN, min_size=1, max_size=5)
    logger.info("PostgreSQL connection pool created")

    async with pg_pool.acquire() as conn:

        await conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_packets (
            id SERIAL PRIMARY KEY,
            device_id TEXT,
            ts_ms BIGINT,
            ax_g REAL,
            ay_g REAL,
            az_g REAL,
            bpm INT,
            spo2_pct REAL,
            received_at TIMESTAMP DEFAULT now()
        );
        """)

        await conn.execute("""
        CREATE TABLE IF NOT EXISTS idle_time (
            day DATE PRIMARY KEY,
            idle_minutes INT
        );
        """)

        await conn.execute("""
        CREATE TABLE IF NOT EXISTS minute_average (
            minute_start TIMESTAMP PRIMARY KEY,

            ax_g REAL,
            ay_g REAL,
            az_g REAL,
            bpm REAL,
            spo2_pct REAL,

            var_ax REAL,
            var_ay REAL,
            var_az REAL,
            std_ax REAL,
            std_ay REAL,
            std_az REAL,

            var_bpm REAL,
            std_bpm REAL,

            var_spo2 REAL,
            std_spo2 REAL
        );
        """)

# -------------------------------
# Run server
# -------------------------------
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
