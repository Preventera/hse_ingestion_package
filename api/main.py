#!/usr/bin/env python3
"""
============================================================================
SAFETWIN X5 - API REST FastAPI
============================================================================
API pour exposer les données HSE (PostgreSQL + Neo4j)

Endpoints:
- /health          - Status de l'API
- /stats           - Statistiques globales
- /incidents       - Données incidents
- /sectors         - Secteurs et analyses
- /predictions     - Prédictions ML
- /neo4j           - Requêtes Knowledge Graph

Version: 1.0.0
Date: 2026-01-14
============================================================================
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime
import logging

from sqlalchemy import create_engine, text
from neo4j import GraphDatabase
import pandas as pd

# Configuration
POSTGRES_URL = "postgresql://postgres:postgres@localhost:5432/safety_graph"
NEO4J_URI = "bolt://localhost:7687"

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# FastAPI App
app = FastAPI(
    title="SafeTwin X5 API",
    description="API REST pour accéder aux données HSE (2.85M+ incidents)",
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

# Connexions
pg_engine = create_engine(POSTGRES_URL)
neo4j_driver = None

def get_neo4j():
    global neo4j_driver
    if neo4j_driver is None:
        neo4j_driver = GraphDatabase.driver(NEO4J_URI)
    return neo4j_driver


# ============================================================================
# MODÈLES PYDANTIC
# ============================================================================

class HealthResponse(BaseModel):
    status: str
    timestamp: str
    postgres: bool
    neo4j: bool
    version: str

class StatsResponse(BaseModel):
    total_incidents: int
    jurisdictions: Dict[str, int]
    years_covered: List[int]
    last_update: str

class SectorStats(BaseModel):
    sector: str
    total_incidents: int
    tms_count: int
    tms_rate: float
    psy_count: int
    psy_rate: float

class PredictionRequest(BaseModel):
    sector: str
    employees: int = 100
    jurisdiction: str = "QC"

class PredictionResponse(BaseModel):
    sector: str
    risk_score: int
    probability_30_days: float
    top_risks: List[str]
    recommendation: str


# ============================================================================
# ENDPOINTS - HEALTH & STATS
# ============================================================================

@app.get("/", tags=["Info"])
async def root():
    """Page d'accueil de l'API"""
    return {
        "name": "SafeTwin X5 API",
        "version": "1.0.0",
        "docs": "/docs",
        "endpoints": ["/health", "/stats", "/incidents", "/sectors", "/predictions", "/neo4j"]
    }

@app.get("/health", response_model=HealthResponse, tags=["Info"])
async def health_check():
    """Vérifier l'état de l'API et des connexions"""
    pg_ok = False
    neo4j_ok = False
    
    # Test PostgreSQL
    try:
        with pg_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        pg_ok = True
    except Exception as e:
        logger.error(f"PostgreSQL error: {e}")
    
    # Test Neo4j
    try:
        driver = get_neo4j()
        driver.verify_connectivity()
        neo4j_ok = True
    except Exception as e:
        logger.error(f"Neo4j error: {e}")
    
    return HealthResponse(
        status="healthy" if (pg_ok and neo4j_ok) else "degraded",
        timestamp=datetime.now().isoformat(),
        postgres=pg_ok,
        neo4j=neo4j_ok,
        version="1.0.0"
    )

@app.get("/stats", response_model=StatsResponse, tags=["Info"])
async def get_stats():
    """Statistiques globales de la base de données"""
    stats = {"total_incidents": 0, "jurisdictions": {}, "years_covered": []}
    
    # CNESST
    try:
        df = pd.read_sql("SELECT COUNT(*) as cnt FROM cnesst_lesions_quebec", pg_engine)
        stats["jurisdictions"]["QC"] = int(df['cnt'].iloc[0])
        stats["total_incidents"] += stats["jurisdictions"]["QC"]
    except:
        pass
    
    # OSHA
    try:
        df = pd.read_sql("SELECT COUNT(*) as cnt FROM osha_injuries_raw", pg_engine)
        stats["jurisdictions"]["USA"] = int(df['cnt'].iloc[0])
        stats["total_incidents"] += stats["jurisdictions"]["USA"]
    except:
        pass
    
    # Eurostat
    try:
        df = pd.read_sql("SELECT COUNT(*) as cnt FROM eurostat_esaw", pg_engine)
        stats["jurisdictions"]["EU27"] = int(df['cnt'].iloc[0])
        stats["total_incidents"] += stats["jurisdictions"]["EU27"]
    except:
        pass
    
    # Years
    try:
        df = pd.read_sql('SELECT DISTINCT "ANNEE" FROM cnesst_lesions_quebec ORDER BY "ANNEE"', pg_engine)
        stats["years_covered"] = df['ANNEE'].tolist()
    except:
        stats["years_covered"] = list(range(2017, 2024))
    
    return StatsResponse(
        total_incidents=stats["total_incidents"],
        jurisdictions=stats["jurisdictions"],
        years_covered=stats["years_covered"],
        last_update=datetime.now().isoformat()
    )


# ============================================================================
# ENDPOINTS - INCIDENTS
# ============================================================================

@app.get("/incidents/cnesst", tags=["Incidents"])
async def get_cnesst_incidents(
    sector: Optional[str] = None,
    year: Optional[int] = None,
    limit: int = Query(default=100, le=1000)
):
    """Récupérer les incidents CNESST (Québec)"""
    query = 'SELECT * FROM cnesst_lesions_quebec WHERE 1=1'
    
    if sector:
        query += f" AND \"SECTEUR_SCIAN\" ILIKE '%{sector}%'"
    if year:
        query += f" AND \"ANNEE\" = {year}"
    
    query += f" LIMIT {limit}"
    
    try:
        df = pd.read_sql(query, pg_engine)
        return {"count": len(df), "data": df.to_dict(orient='records')}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/incidents/osha", tags=["Incidents"])
async def get_osha_incidents(
    state: Optional[str] = None,
    limit: int = Query(default=100, le=1000)
):
    """Récupérer les incidents OSHA (USA)"""
    query = 'SELECT * FROM osha_injuries_raw WHERE 1=1'
    
    if state:
        query += f" AND state ILIKE '%{state}%'"
    
    query += f" LIMIT {limit}"
    
    try:
        df = pd.read_sql(query, pg_engine)
        return {"count": len(df), "data": df.to_dict(orient='records')}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/incidents/eurostat", tags=["Incidents"])
async def get_eurostat_incidents(
    country: Optional[str] = None,
    limit: int = Query(default=100, le=1000)
):
    """Récupérer les incidents Eurostat (EU)"""
    query = 'SELECT * FROM eurostat_esaw WHERE 1=1'
    
    if country:
        query += f" AND country_code ILIKE '%{country}%'"
    
    query += f" LIMIT {limit}"
    
    try:
        df = pd.read_sql(query, pg_engine)
        return {"count": len(df), "data": df.to_dict(orient='records')}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# ENDPOINTS - SECTORS
# ============================================================================

@app.get("/sectors", tags=["Secteurs"])
async def get_sectors():
    """Liste des secteurs avec statistiques"""
    query = """
    SELECT 
        "SECTEUR_SCIAN" as sector,
        COUNT(*) as total_incidents,
        SUM(CASE WHEN "IND_LESION_TMS" = 'OUI' THEN 1 ELSE 0 END) as tms_count,
        SUM(CASE WHEN "IND_LESION_PSY" = 'OUI' THEN 1 ELSE 0 END) as psy_count
    FROM cnesst_lesions_quebec
    WHERE "SECTEUR_SCIAN" IS NOT NULL
    GROUP BY "SECTEUR_SCIAN"
    ORDER BY total_incidents DESC
    """
    
    try:
        df = pd.read_sql(query, pg_engine)
        df['tms_rate'] = (df['tms_count'] / df['total_incidents'] * 100).round(2)
        df['psy_rate'] = (df['psy_count'] / df['total_incidents'] * 100).round(2)
        return {"count": len(df), "sectors": df.to_dict(orient='records')}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/sectors/{sector_name}", tags=["Secteurs"])
async def get_sector_detail(sector_name: str):
    """Détails d'un secteur spécifique"""
    query = f"""
    SELECT 
        "ANNEE" as year,
        COUNT(*) as incidents,
        SUM(CASE WHEN "IND_LESION_TMS" = 'OUI' THEN 1 ELSE 0 END) as tms,
        SUM(CASE WHEN "IND_LESION_PSY" = 'OUI' THEN 1 ELSE 0 END) as psy
    FROM cnesst_lesions_quebec
    WHERE "SECTEUR_SCIAN" ILIKE '%{sector_name}%'
    GROUP BY "ANNEE"
    ORDER BY "ANNEE"
    """
    
    try:
        df = pd.read_sql(query, pg_engine)
        if df.empty:
            raise HTTPException(status_code=404, detail="Secteur non trouvé")
        
        return {
            "sector": sector_name,
            "total_incidents": int(df['incidents'].sum()),
            "yearly_data": df.to_dict(orient='records')
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# ENDPOINTS - PREDICTIONS
# ============================================================================

@app.post("/predictions", response_model=PredictionResponse, tags=["Prédictions"])
async def predict_risk(request: PredictionRequest):
    """Prédire le risque pour un secteur donné"""
    
    # Récupérer données historiques
    query = f"""
    SELECT 
        COUNT(*) as total,
        SUM(CASE WHEN "IND_LESION_TMS" = 'OUI' THEN 1 ELSE 0 END) as tms,
        SUM(CASE WHEN "IND_LESION_PSY" = 'OUI' THEN 1 ELSE 0 END) as psy,
        SUM(CASE WHEN "IND_LESION_MACHINE" = 'OUI' THEN 1 ELSE 0 END) as machine
    FROM cnesst_lesions_quebec
    WHERE "SECTEUR_SCIAN" ILIKE '%{request.sector}%'
    """
    
    try:
        df = pd.read_sql(query, pg_engine)
        
        if df['total'].iloc[0] == 0:
            raise HTTPException(status_code=404, detail="Secteur non trouvé")
        
        total = int(df['total'].iloc[0])
        tms_rate = df['tms'].iloc[0] / total * 100
        psy_rate = df['psy'].iloc[0] / total * 100
        machine_rate = df['machine'].iloc[0] / total * 100
        
        # Calcul score de risque (simplifié)
        base_risk = min(100, int((total / 10000) * 50 + tms_rate + psy_rate))
        risk_score = min(100, base_risk + (request.employees // 50))
        
        # Probabilité 30 jours
        incidents_per_year = total / 7  # 7 ans de données
        monthly_rate = incidents_per_year / 12
        prob_30_days = min(0.99, monthly_rate / 1000 * request.employees)
        
        # Top risques
        risks = []
        if tms_rate > 20:
            risks.append(f"TMS ({tms_rate:.1f}%)")
        if psy_rate > 5:
            risks.append(f"Psychologique ({psy_rate:.1f}%)")
        if machine_rate > 10:
            risks.append(f"Machines ({machine_rate:.1f}%)")
        if not risks:
            risks = ["Risque modéré"]
        
        # Recommandation
        if risk_score > 70:
            rec = "Action immédiate requise - Formation SST et audit recommandés"
        elif risk_score > 40:
            rec = "Surveillance accrue recommandée - Réviser les procédures"
        else:
            rec = "Maintenir les mesures préventives actuelles"
        
        return PredictionResponse(
            sector=request.sector,
            risk_score=risk_score,
            probability_30_days=round(prob_30_days, 2),
            top_risks=risks,
            recommendation=rec
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# ENDPOINTS - NEO4J
# ============================================================================

@app.get("/neo4j/stats", tags=["Neo4j"])
async def get_neo4j_stats():
    """Statistiques du Knowledge Graph Neo4j"""
    try:
        driver = get_neo4j()
        with driver.session() as session:
            # Compter nœuds
            result = session.run("MATCH (n) RETURN count(n) as count")
            nodes = result.single()["count"]
            
            # Compter relations
            result = session.run("MATCH ()-[r]->() RETURN count(r) as count")
            rels = result.single()["count"]
            
            # Labels
            result = session.run("CALL db.labels() YIELD label RETURN collect(label) as labels")
            labels = result.single()["labels"]
            
        return {
            "total_nodes": nodes,
            "total_relationships": rels,
            "labels": labels
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/neo4j/jurisdictions", tags=["Neo4j"])
async def get_neo4j_jurisdictions():
    """Récupérer les juridictions depuis Neo4j"""
    try:
        driver = get_neo4j()
        with driver.session() as session:
            result = session.run("""
                MATCH (j:Jurisdiction)
                OPTIONAL MATCH (j)<-[:IN_JURISDICTION]-(s)
                RETURN j.code as code, j.name as name, j.flag as flag, count(s) as entities
            """)
            jurisdictions = [dict(record) for record in result]
        
        return {"jurisdictions": jurisdictions}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/neo4j/sectors", tags=["Neo4j"])
async def get_neo4j_sectors(jurisdiction: Optional[str] = None):
    """Récupérer les secteurs depuis Neo4j"""
    try:
        driver = get_neo4j()
        with driver.session() as session:
            query = """
                MATCH (s:Sector)
                WHERE s.jurisdiction IS NOT NULL
            """
            if jurisdiction:
                query += f" AND s.jurisdiction = '{jurisdiction}'"
            
            query += " RETURN s.code as code, s.name as name, s.jurisdiction as jurisdiction LIMIT 50"
            
            result = session.run(query)
            sectors = [dict(record) for record in result]
        
        return {"count": len(sectors), "sectors": sectors}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# STARTUP & SHUTDOWN
# ============================================================================

@app.on_event("startup")
async def startup():
    logger.info("🚀 SafeTwin X5 API starting...")
    logger.info(f"📊 PostgreSQL: {POSTGRES_URL}")
    logger.info(f"🧠 Neo4j: {NEO4J_URI}")

@app.on_event("shutdown")
async def shutdown():
    global neo4j_driver
    if neo4j_driver:
        neo4j_driver.close()
    logger.info("👋 SafeTwin X5 API stopped")


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
