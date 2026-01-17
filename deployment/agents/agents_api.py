from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import json, urllib.request

app = FastAPI(title="AgenticX5 API", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

AGENTS = {
    "MJ1": {"name": "Multi_Jurisdictional", "serie": "T", "precision": 0.95},
    "AN6": {"name": "Incident_Trend_Analyzer", "serie": "AN", "precision": 0.93},
    "R5": {"name": "Ergonomic_Risk", "serie": "R", "precision": 0.92},
    "SC23": {"name": "Fall_Analysis", "serie": "SC", "precision": 0.92},
    "A6": {"name": "Predictive_Model", "serie": "A", "precision": 0.947},
    "Sentinelle": {"name": "Sentinel_24_7", "serie": "L", "precision": 0.96},
}

def call_api(endpoint):
    try:
        with urllib.request.urlopen(f"http://localhost:8000{endpoint}", timeout=10) as r:
            return json.loads(r.read().decode())
    except: return {}

@app.get("/")
def root():
    return {"name": "AgenticX5 API", "agents": len(AGENTS), "port": 8002}

@app.get("/agents")
def list_agents():
    return {"total": len(AGENTS), "agents": AGENTS}

@app.get("/orchestrate")
def orchestrate():
    stats = call_api("/stats")
    sectors = call_api("/sectors").get("sectors", [])
    top5 = sorted(sectors, key=lambda x: x.get("total_incidents", 0), reverse=True)[:5]
    return {
        "agents_executed": list(AGENTS.keys()),
        "total_incidents": stats.get("total_incidents", 0),
        "jurisdictions": stats.get("jurisdictions", {}),
        "top_sectors": [{"name": s["sector"][:35], "incidents": s["total_incidents"]} for s in top5],
        "alerts": [{"type": "CRITIQUE", "sector": s["sector"][:30]} for s in top5 if s["total_incidents"] > 50000]
    }

@app.get("/dashboard/data")
def dashboard_data():
    stats = call_api("/stats")
    sectors = call_api("/sectors").get("sectors", [])
    return {
        "kpis": {"total_incidents": stats.get("total_incidents", 0), "sectors_count": len(sectors), "agents_active": len(AGENTS)},
        "jurisdictions": stats.get("jurisdictions", {}),
        "top_sectors": [{"name": s["sector"][:30], "incidents": s["total_incidents"], "tms_rate": s.get("tms_rate",0)} for s in sorted(sectors, key=lambda x: x.get("total_incidents", 0), reverse=True)[:10]]
    }

if __name__ == "__main__":
    import uvicorn
    print("\n" + "=" * 50)
    print("  AgenticX5 API - http://localhost:8002")
    print("  Docs: http://localhost:8002/docs")
    print("=" * 50)
    uvicorn.run(app, host="0.0.0.0", port=8002)
