import asyncio
import json
import uuid
import os
from typing import AsyncGenerator

import redis.asyncio as aioredis
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

app = FastAPI(title="Analizador de Texto Distribuido")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = 6379

async def get_redis():
    return await aioredis.from_url(
        f"redis://{REDIS_HOST}:{REDIS_PORT}",
        decode_responses=True
    )

class TextoRequest(BaseModel):
    texto: str
    nombre: str = "Tarea"

@app.post("/tarea")
async def crear_tarea(request: TextoRequest):
    tarea_id = str(uuid.uuid4())
    tarea = {
        "id": tarea_id,
        "nombre": request.nombre,
        "texto": request.texto,
        "status": "pendiente",
        "resultado": None,
    }
    redis = await get_redis()
    await redis.set(f"tarea:{tarea_id}", json.dumps(tarea))
    await redis.rpush("cola:tareas", json.dumps(tarea))
    await redis.aclose()
    return {"tarea_id": tarea_id, "status": "pendiente"}

@app.get("/estado/{tarea_id}")
async def estado_tarea(tarea_id: str):
    async def generar_eventos() -> AsyncGenerator[str, None]:
        redis = await get_redis()
        try:
            ultimo_status = None
            for _ in range(120):
                datos_raw = await redis.get(f"tarea:{tarea_id}")
                if datos_raw:
                    datos = json.loads(datos_raw)
                    status_actual = datos["status"]
                    if status_actual != ultimo_status:
                        ultimo_status = status_actual
                        yield f"data: {json.dumps(datos)}\n\n"
                    if status_actual in ("completada", "error"):
                        break
                await asyncio.sleep(0.5)
        finally:
            await redis.aclose()

    return StreamingResponse(
        generar_eventos(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    )

@app.get("/tareas")
async def listar_tareas():
    redis = await get_redis()
    keys = await redis.keys("tarea:*")
    tareas = []
    for key in keys:
        datos_raw = await redis.get(key)
        if datos_raw:
            tareas.append(json.loads(datos_raw))
    await redis.aclose()
    return sorted(tareas, key=lambda t: t["id"])

app.mount("/", StaticFiles(directory="/app/static", html=True), name="static")