from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from psycopg2.extras import RealDictCursor

app = FastAPI(title="SafetyGraph API", version="1.0.0")

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

def get_db():
    return psycopg2.connect(host="localhost", port=5434, database="safetygraph", user="safetygraph", password="SafetyGraph2026!")

@app.get("/")
def root():
    return {"message": "SafetyGraph API", "docs": "/docs"}

@app.get("/api/status")
def status():
    try:
        conn = get_db()
        conn.close()
        return {"status": "ok", "postgres": True}
    except:
        return {"status": "degraded", "postgres": False}

@app.get("/api/osha/sectors")
def get_osha_sectors():
    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM osha_sectors ORDER BY injuries DESC LIMIT 20")
    results = cur.fetchall()
    conn.close()
    return [dict(r) for r in results]

@app.get("/api/osha/top-dangerous")
def get_top_dangerous():
    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT naics_code, sector_name, injuries, deaths FROM osha_sectors WHERE deaths > 0 ORDER BY deaths DESC LIMIT 10")
    results = cur.fetchall()
    conn.close()
    return [dict(r) for r in results]
