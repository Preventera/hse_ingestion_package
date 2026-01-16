"""
SAFETYGRAPH API - FastAPI
=========================
API REST pour requetes dynamiques SafetyGraph
Endpoints: CNESST, OSHA, Harmonisation, Predictions
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Dict
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime

# ============================================
# CONFIGURATION
# ============================================

DB_CONFIG = {
    "host": "localhost",
    "port": 5434,
    "database": "safetygraph",
    "user": "safetygraph",
    "password": "SafetyGraph2026!"
}

# ============================================
# FASTAPI APP
# ============================================

app = FastAPI(
    title="SafetyGraph API",
    description="API REST pour donnees HSE harmonisees (CNESST, OSHA, EU-OSHA)",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================
# MODELS PYDANTIC
# ============================================

class CNESSTRecord(BaseModel):
    id: int
    annee: int
    scian: str
    genre_accident: str
    nature_lesion: str
    nombre_lesions: int
    jours_perdus: int

class OSHARecord(BaseModel):
    id: int
    year: int
    naics: str
    industry_description: str
    event_code: str
    total_cases: int
    total_deaths: int
    days_away: int

class SectorStats(BaseModel):
    sector: str
    total_cases: int
    total_days_lost: int
    avg_severity: float
    source: str

class HarmonizedData(BaseModel):
    cnesst_total: int
    osha_total: int
    sectors_count: int
    top_risks: List[Dict]

class RiskPrediction(BaseModel):
    sector: str
    risk_level: str
    score: float
    factors: List[str]

# ============================================
# DATABASE CONNECTION
# ============================================

def get_db_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection error: {str(e)}")

# ============================================
# ENDPOINTS
# ============================================

@app.get("/", tags=["Health"])
def root():
    """Health check endpoint"""
    return {
        "status": "online",
        "service": "SafetyGraph API",
        "version": "1.0.0",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/api/v1/stats", tags=["Statistics"])
def get_global_stats():
    """Statistiques globales SafetyGraph"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # CNESST stats
    cur.execute("SELECT COUNT(*) as count, SUM(nombre_lesions) as total FROM cnesst_lesions")
    cnesst = cur.fetchone()
    
    # OSHA stats
    cur.execute("SELECT COUNT(*) as count, SUM(total_cases) as total, SUM(total_deaths) as deaths FROM osha_injuries")
    osha = cur.fetchone()
    
    conn.close()
    
    return {
        "cnesst": {
            "records": cnesst["count"],
            "total_lesions": int(cnesst["total"] or 0)
        },
        "osha": {
            "records": osha["count"],
            "total_cases": int(osha["total"] or 0),
            "total_deaths": int(osha["deaths"] or 0)
        },
        "combined_incidents": int((cnesst["total"] or 0) + (osha["total"] or 0))
    }

# --- CNESST ENDPOINTS ---

@app.get("/api/v1/cnesst", response_model=List[Dict], tags=["CNESST"])
def get_cnesst_data(
    year: Optional[int] = Query(None, description="Filter by year"),
    scian: Optional[str] = Query(None, description="Filter by SCIAN code"),
    limit: int = Query(100, le=1000)
):
    """Recuperer les donnees CNESST"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    query = "SELECT * FROM cnesst_lesions WHERE 1=1"
    params = []
    
    if year:
        query += " AND annee = %s"
        params.append(year)
    if scian:
        query += " AND scian = %s"
        params.append(scian)
    
    query += f" ORDER BY annee DESC, nombre_lesions DESC LIMIT {limit}"
    
    cur.execute(query, params)
    results = cur.fetchall()
    conn.close()
    
    return [dict(r) for r in results]

@app.get("/api/v1/cnesst/sectors", tags=["CNESST"])
def get_cnesst_by_sector():
    """Agregation CNESST par secteur SCIAN"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    cur.execute("""
        SELECT scian, 
               SUM(nombre_lesions) as total_lesions,
               SUM(jours_perdus) as total_jours,
               ROUND(AVG(jours_perdus::numeric / NULLIF(nombre_lesions, 0)), 2) as gravite_moyenne
        FROM cnesst_lesions
        GROUP BY scian
        ORDER BY total_lesions DESC
    """)
    
    results = cur.fetchall()
    conn.close()
    
    return [dict(r) for r in results]

@app.get("/api/v1/cnesst/trends", tags=["CNESST"])
def get_cnesst_trends():
    """Tendances annuelles CNESST"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    cur.execute("""
        SELECT annee,
               SUM(nombre_lesions) as total_lesions,
               SUM(jours_perdus) as total_jours
        FROM cnesst_lesions
        GROUP BY annee
        ORDER BY annee
    """)
    
    results = cur.fetchall()
    conn.close()
    
    return [dict(r) for r in results]

# --- OSHA ENDPOINTS ---

@app.get("/api/v1/osha", response_model=List[Dict], tags=["OSHA"])
def get_osha_data(
    year: Optional[int] = Query(None, description="Filter by year"),
    naics: Optional[str] = Query(None, description="Filter by NAICS code"),
    limit: int = Query(100, le=1000)
):
    """Recuperer les donnees OSHA"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    query = "SELECT * FROM osha_injuries WHERE 1=1"
    params = []
    
    if year:
        query += " AND year = %s"
        params.append(year)
    if naics:
        query += " AND naics = %s"
        params.append(naics)
    
    query += f" ORDER BY year DESC, total_cases DESC LIMIT {limit}"
    
    cur.execute(query, params)
    results = cur.fetchall()
    conn.close()
    
    return [dict(r) for r in results]

@app.get("/api/v1/osha/sectors", tags=["OSHA"])
def get_osha_by_sector():
    """Agregation OSHA par secteur NAICS"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    cur.execute("""
        SELECT naics, industry_description,
               SUM(total_cases) as total_cases,
               SUM(total_deaths) as total_deaths,
               SUM(days_away) as total_days_away,
               ROUND(SUM(total_deaths)::numeric / NULLIF(SUM(total_cases), 0) * 1000, 2) as fatality_rate
        FROM osha_injuries
        GROUP BY naics, industry_description
        ORDER BY total_cases DESC
    """)
    
    results = cur.fetchall()
    conn.close()
    
    return [dict(r) for r in results]

# --- HARMONIZATION ENDPOINTS ---

@app.get("/api/v1/harmonized/compare", tags=["Harmonization"])
def compare_jurisdictions(sector: str = Query(..., description="Sector code (e.g., 23, 62, 221122)")):
    """Comparer CNESST vs OSHA pour un secteur"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # CNESST
    cur.execute("""
        SELECT SUM(nombre_lesions) as cases, SUM(jours_perdus) as days
        FROM cnesst_lesions WHERE scian = %s
    """, [sector])
    cnesst = cur.fetchone()
    
    # OSHA (mapping SCIAN -> NAICS)
    naics_mapping = {
        "221122": "2211",
        "23": "23",
        "62": "62",
        "31-33": "31-33",
        "48-49": "48-49",
        "44-45": "44-45",
        "21": "21",
        "11": "11"
    }
    naics = naics_mapping.get(sector, sector)
    
    cur.execute("""
        SELECT SUM(total_cases) as cases, SUM(total_deaths) as deaths, SUM(days_away) as days
        FROM osha_injuries WHERE naics = %s
    """, [naics])
    osha = cur.fetchone()
    
    conn.close()
    
    return {
        "sector": sector,
        "cnesst": {
            "jurisdiction": "Quebec",
            "cases": int(cnesst["cases"] or 0),
            "days_lost": int(cnesst["days"] or 0)
        },
        "osha": {
            "jurisdiction": "USA",
            "cases": int(osha["cases"] or 0),
            "deaths": int(osha["deaths"] or 0),
            "days_lost": int(osha["days"] or 0)
        },
        "ratio_usa_qc": round((osha["cases"] or 0) / max(1, cnesst["cases"] or 1), 2)
    }

@app.get("/api/v1/harmonized/electric", tags=["Harmonization"])
def get_electric_sector_analysis():
    """Analyse specifique secteur electrique (221122 / 2211)"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # CNESST 221122
    cur.execute("""
        SELECT annee, genre_accident, SUM(nombre_lesions) as cases, SUM(jours_perdus) as days
        FROM cnesst_lesions WHERE scian = '221122'
        GROUP BY annee, genre_accident
        ORDER BY annee, cases DESC
    """)
    cnesst = cur.fetchall()
    
    # OSHA 2211
    cur.execute("""
        SELECT year, event_code, event_description, SUM(total_cases) as cases, SUM(total_deaths) as deaths
        FROM osha_injuries WHERE naics = '2211'
        GROUP BY year, event_code, event_description
        ORDER BY year, cases DESC
    """)
    osha = cur.fetchall()
    
    conn.close()
    
    return {
        "sector": "Electric Power Distribution",
        "scian": "221122",
        "naics": "2211",
        "cnesst_data": [dict(r) for r in cnesst],
        "osha_data": [dict(r) for r in osha],
        "top_risks": [
            {"code": "51/610", "name": "Contact electrique", "severity": "CRITICAL"},
            {"code": "42/410", "name": "Chute hauteur", "severity": "CRITICAL"},
            {"code": "40/050", "name": "Arc flash/Brulure", "severity": "HIGH"}
        ]
    }

# --- RISK PREDICTION ENDPOINTS ---

@app.get("/api/v1/predict/risk", response_model=RiskPrediction, tags=["Predictions"])
def predict_sector_risk(sector: str = Query(..., description="SCIAN sector code")):
    """Prediction de risque pour un secteur"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    cur.execute("""
        SELECT SUM(nombre_lesions) as cases, SUM(jours_perdus) as days
        FROM cnesst_lesions WHERE scian = %s
    """, [sector])
    data = cur.fetchone()
    conn.close()
    
    cases = int(data["cases"] or 0)
    days = int(data["days"] or 0)
    
    # Simple risk scoring
    severity = days / max(1, cases)
    
    if cases > 50000 or severity > 8:
        risk_level = "CRITICAL"
        score = 90 + min(10, severity)
    elif cases > 20000 or severity > 6:
        risk_level = "HIGH"
        score = 70 + min(20, severity * 2)
    elif cases > 5000 or severity > 4:
        risk_level = "MEDIUM"
        score = 40 + min(30, severity * 3)
    else:
        risk_level = "LOW"
        score = 10 + min(30, severity * 4)
    
    factors = []
    if severity > 6:
        factors.append("High severity per incident")
    if cases > 10000:
        factors.append("High incident volume")
    if sector in ["23", "21", "221122"]:
        factors.append("Inherently hazardous industry")
    
    return RiskPrediction(
        sector=sector,
        risk_level=risk_level,
        score=round(score, 2),
        factors=factors if factors else ["Standard risk profile"]
    )

# ============================================
# MAIN
# ============================================

if __name__ == "__main__":
    import uvicorn
    print("=" * 50)
    print("SAFETYGRAPH API")
    print("=" * 50)
    print("Starting server...")
    print("Docs: http://localhost:8000/docs")
    print("=" * 50)
    uvicorn.run(app, host="0.0.0.0", port=8000)
