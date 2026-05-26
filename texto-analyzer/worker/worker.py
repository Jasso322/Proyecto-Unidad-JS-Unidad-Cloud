import json
import os
import socket
import time
from collections import Counter
import sys
sys.stdout.reconfigure(line_buffering=True)

import redis

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = 6379
WORKER_ID = socket.gethostname()

print(f"[{WORKER_ID}] Worker iniciado. Conectando a Redis en {REDIS_HOST}:{REDIS_PORT}...")

def conectar_redis():
    while True:
        try:
            cliente = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            cliente.ping()
            print(f"[{WORKER_ID}] ✓ Conectado a Redis")
            return cliente
        except redis.ConnectionError:
            print(f"[{WORKER_ID}] Redis no disponible, reintentando en 2s...")
            time.sleep(2)

def analizar_texto(texto: str) -> dict:
    texto_limpio = texto.strip()
    palabras_raw = texto_limpio.split()

    total_palabras = len(palabras_raw)
    total_caracteres = len(texto_limpio)
    total_oraciones = texto_limpio.count('.') + texto_limpio.count('!') + texto_limpio.count('?')

    PALABRAS_VACIAS = {
        "el","la","los","las","un","una","unos","unas","de","del","en","con",
        "por","para","que","se","y","o","a","es","son","the","an","is","are",
        "and","or","of","to","in","it","this"
    }
    palabras_normalizadas = [p.lower().strip(".,;:!?\"'()") for p in palabras_raw]
    palabras_utiles = [p for p in palabras_normalizadas if p and p not in PALABRAS_VACIAS]
    contador = Counter(palabras_utiles)
    top_palabras = contador.most_common(5)

    palabras_clave = [palabra for palabra, freq in contador.items() if freq >= 2]

    oraciones = [s.strip() for s in texto_limpio.replace('!','.').replace('?','.').split('.') if s.strip()]
    resumen = oraciones[0] if oraciones else texto_limpio[:100]

    palabras_es = {"el","la","de","en","que","es","son","con","del"}
    palabras_en = {"the","is","are","and","or","of","to","in","it"}
    score_es = sum(1 for p in palabras_normalizadas if p in palabras_es)
    score_en = sum(1 for p in palabras_normalizadas if p in palabras_en)
    idioma = "Español" if score_es >= score_en else "Inglés"

    return {
        "worker_id": WORKER_ID,
        "total_palabras": total_palabras,
        "total_caracteres": total_caracteres,
        "total_oraciones": max(total_oraciones, 1),
        "top_palabras": [{"palabra": p, "frecuencia": f} for p, f in top_palabras],
        "palabras_clave": palabras_clave[:10],
        "resumen": resumen[:200],
        "idioma_detectado": idioma,
    }

def main():
    r = conectar_redis()
    print(f"[{WORKER_ID}] Esperando tareas en 'cola:tareas'...")

    while True:
        try:
            resultado = r.blpop("cola:tareas", timeout=0)
            if resultado is None:
                continue

            _, tarea_raw = resultado
            tarea = json.loads(tarea_raw)
            tarea_id = tarea["id"]
            print(f"[{WORKER_ID}] 📥 Tarea recibida: {tarea_id}")

            tarea["status"] = "en_proceso"
            tarea["worker"] = WORKER_ID
            r.set(f"tarea:{tarea_id}", json.dumps(tarea))
            time.sleep(10.5)

            try:
                resultado_analisis = analizar_texto(tarea["texto"])
                tarea["status"] = "completada"
                tarea["resultado"] = resultado_analisis
                r.set(f"tarea:{tarea_id}", json.dumps(tarea))
                print(f"[{WORKER_ID}] ✅ Completada: {tarea_id}")
            except Exception as e:
                tarea["status"] = "error"
                tarea["resultado"] = {"error": str(e)}
                r.set(f"tarea:{tarea_id}", json.dumps(tarea))
                print(f"[{WORKER_ID}] ❌ Error: {e}")

        except redis.ConnectionError:
            print(f"[{WORKER_ID}] Conexión perdida. Reconectando...")
            time.sleep(2)
            r = conectar_redis()

if __name__ == "__main__":
    main()