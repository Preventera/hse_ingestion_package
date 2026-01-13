"""
PostgreSQL Loader - Safety Graph Integration
=============================================
Chargement des données Gold HSE vers PostgreSQL Safety Graph

Auteur: Mario Genest - GenAISafety / Preventera
Version: 1.0.0
"""

import os
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
from sqlalchemy import create_engine, text, MetaData, Table, Column
from sqlalchemy import Integer, String, Float, DateTime, Text, Date, Numeric
from sqlalchemy.dialects.postgresql import insert

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SafetyGraphLoader")


class SafetyGraphLoader:
    """
    Chargeur pour PostgreSQL Safety Graph
    
    Usage:
        loader = SafetyGraphLoader()
        loader.create_tables()
        loader.load_gold_data("data/gold/hse_incidents_global.parquet")
    """
    
    def __init__(self, connection_string: str = None):
        """
        Initialiser le chargeur
        
        Args:
            connection_string: URL de connexion PostgreSQL
                               Si None, utilise les variables d'environnement
        """
        if connection_string is None:
            connection_string = (
                f"postgresql://{os.getenv('POSTGRES_USER', 'agenticx5')}:"
                f"{os.getenv('POSTGRES_PASSWORD', 'password')}@"
                f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
                f"{os.getenv('POSTGRES_PORT', '5432')}/"
                f"{os.getenv('POSTGRES_DB', 'safety_graph')}"
            )
        
        self.engine = create_engine(connection_string, echo=False)
        self.metadata = MetaData()
        
        logger.info(f"✅ Connected to: {os.getenv('POSTGRES_HOST', 'localhost')}/{os.getenv('POSTGRES_DB', 'safety_graph')}")
    
    def create_tables(self):
        """Créer toutes les tables HSE"""
        
        ddl_statements = [
            # === Table principale des incidents ===
            """
            CREATE TABLE IF NOT EXISTS hse_incidents_global (
                id SERIAL PRIMARY KEY,
                incident_id VARCHAR(100),
                source VARCHAR(200) NOT NULL,
                jurisdiction VARCHAR(50) NOT NULL,
                incident_date DATE,
                year INTEGER,
                month INTEGER,
                
                -- Classification industrielle
                industry_code VARCHAR(20),
                industry_code_system VARCHAR(20),  -- NAICS, NACE, NAF, SCIAN
                industry_name TEXT,
                
                -- Établissement
                establishment_id VARCHAR(100),
                establishment_name TEXT,
                establishment_size VARCHAR(50),
                
                -- Type d'incident
                incident_type VARCHAR(50),  -- fatal, non-fatal, illness
                severity VARCHAR(50),
                nature_of_injury TEXT,
                body_part VARCHAR(100),
                event_type VARCHAR(200),
                
                -- Impact
                days_lost NUMERIC,
                hospitalized BOOLEAN DEFAULT FALSE,
                amputation BOOLEAN DEFAULT FALSE,
                
                -- Travailleur
                worker_age NUMERIC,
                worker_gender VARCHAR(20),
                worker_employment_status VARCHAR(50),
                worker_tenure_months INTEGER,
                
                -- Localisation
                address TEXT,
                city VARCHAR(100),
                state_province VARCHAR(100),
                postal_code VARCHAR(20),
                country VARCHAR(100),
                latitude NUMERIC(10, 6),
                longitude NUMERIC(10, 6),
                
                -- Narratif
                narrative TEXT,
                
                -- Métadonnées
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                ingestion_batch_id VARCHAR(50),
                
                -- Contraintes
                CONSTRAINT unique_incident_source UNIQUE (incident_id, source)
            )
            """,
            
            # === Index pour performance ===
            "CREATE INDEX IF NOT EXISTS idx_incidents_jurisdiction ON hse_incidents_global(jurisdiction)",
            "CREATE INDEX IF NOT EXISTS idx_incidents_year ON hse_incidents_global(year)",
            "CREATE INDEX IF NOT EXISTS idx_incidents_industry ON hse_incidents_global(industry_code)",
            "CREATE INDEX IF NOT EXISTS idx_incidents_type ON hse_incidents_global(incident_type)",
            "CREATE INDEX IF NOT EXISTS idx_incidents_severity ON hse_incidents_global(severity)",
            "CREATE INDEX IF NOT EXISTS idx_incidents_source ON hse_incidents_global(source)",
            "CREATE INDEX IF NOT EXISTS idx_incidents_date ON hse_incidents_global(incident_date)",
            
            # === Table des sources de données ===
            """
            CREATE TABLE IF NOT EXISTS hse_data_sources (
                id SERIAL PRIMARY KEY,
                source_key VARCHAR(100) UNIQUE NOT NULL,
                source_name VARCHAR(200),
                source_type VARCHAR(50),
                jurisdiction VARCHAR(50),
                url TEXT,
                format VARCHAR(20),
                update_frequency VARCHAR(50),
                priority INTEGER DEFAULT 1,
                
                -- Statistiques d'ingestion
                last_ingestion TIMESTAMP,
                rows_ingested INTEGER DEFAULT 0,
                total_rows INTEGER DEFAULT 0,
                status VARCHAR(20) DEFAULT 'pending',
                error_message TEXT,
                
                -- Métadonnées
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            
            # === Table des batches d'ingestion ===
            """
            CREATE TABLE IF NOT EXISTS hse_ingestion_batches (
                id SERIAL PRIMARY KEY,
                batch_id VARCHAR(50) UNIQUE NOT NULL,
                started_at TIMESTAMP NOT NULL,
                completed_at TIMESTAMP,
                status VARCHAR(20) DEFAULT 'running',
                
                sources_processed INTEGER DEFAULT 0,
                sources_successful INTEGER DEFAULT 0,
                sources_failed INTEGER DEFAULT 0,
                total_rows_ingested INTEGER DEFAULT 0,
                
                error_log TEXT,
                report_json JSONB,
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            
            # === Table de mapping des codes industriels ===
            """
            CREATE TABLE IF NOT EXISTS hse_industry_mapping (
                id SERIAL PRIMARY KEY,
                naics_code VARCHAR(10),
                nace_code VARCHAR(10),
                naf_code VARCHAR(10),
                scian_code VARCHAR(10),
                isic_code VARCHAR(10),
                
                industry_name_en TEXT,
                industry_name_fr TEXT,
                
                risk_level VARCHAR(20),  -- low, medium, high, very_high
                typical_hazards TEXT[],
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            
            # === Vue pour les statistiques globales ===
            """
            CREATE OR REPLACE VIEW hse_global_stats AS
            SELECT 
                jurisdiction,
                year,
                incident_type,
                COUNT(*) as incident_count,
                SUM(CASE WHEN incident_type = 'fatal' THEN 1 ELSE 0 END) as fatalities,
                AVG(days_lost) as avg_days_lost,
                COUNT(DISTINCT industry_code) as industries_affected
            FROM hse_incidents_global
            GROUP BY jurisdiction, year, incident_type
            ORDER BY jurisdiction, year DESC, incident_type
            """,
            
            # === Vue pour le dashboard ===
            """
            CREATE OR REPLACE VIEW hse_dashboard_summary AS
            SELECT 
                jurisdiction,
                COUNT(*) as total_incidents,
                SUM(CASE WHEN incident_type = 'fatal' THEN 1 ELSE 0 END) as total_fatalities,
                COUNT(DISTINCT year) as years_covered,
                MIN(year) as first_year,
                MAX(year) as last_year,
                COUNT(DISTINCT source) as data_sources,
                MAX(updated_at) as last_update
            FROM hse_incidents_global
            GROUP BY jurisdiction
            ORDER BY total_incidents DESC
            """
        ]
        
        with self.engine.connect() as conn:
            for ddl in ddl_statements:
                try:
                    conn.execute(text(ddl))
                    conn.commit()
                except Exception as e:
                    logger.warning(f"DDL warning: {e}")
        
        logger.info("✅ Tables créées/vérifiées avec succès")
    
    def load_gold_data(
        self, 
        parquet_path: str, 
        if_exists: str = "append",
        batch_size: int = 10000
    ) -> int:
        """
        Charger les données Gold Parquet dans PostgreSQL
        
        Args:
            parquet_path: Chemin vers le fichier Parquet
            if_exists: 'append', 'replace', ou 'fail'
            batch_size: Taille des lots pour l'insertion
            
        Returns:
            Nombre de lignes chargées
        """
        logger.info(f"📥 Loading: {parquet_path}")
        
        df = pd.read_parquet(parquet_path)
        
        # Renommer les colonnes si nécessaire
        column_mapping = {
            'date': 'incident_date',
            'worker_age': 'worker_age',
            'worker_gender': 'worker_gender'
        }
        df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
        
        # Ajouter batch_id
        batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        df['ingestion_batch_id'] = batch_id
        df['updated_at'] = datetime.now()
        
        # Colonnes à garder (celles qui existent dans la table)
        valid_columns = [
            'incident_id', 'source', 'jurisdiction', 'incident_date', 'year',
            'industry_code', 'industry_code_system', 'industry_name',
            'establishment_size', 'incident_type', 'severity', 'nature_of_injury',
            'body_part', 'event_type', 'days_lost', 'worker_age', 'worker_gender',
            'worker_employment_status', 'narrative', 'latitude', 'longitude',
            'created_at', 'updated_at', 'ingestion_batch_id'
        ]
        
        df_to_load = df[[c for c in valid_columns if c in df.columns]]
        
        # Charger par lots
        total_rows = 0
        for i in range(0, len(df_to_load), batch_size):
            batch = df_to_load.iloc[i:i+batch_size]
            
            batch.to_sql(
                'hse_incidents_global',
                self.engine,
                if_exists='append' if i > 0 or if_exists == 'append' else if_exists,
                index=False,
                method='multi'
            )
            
            total_rows += len(batch)
            logger.info(f"   ✓ Batch {i//batch_size + 1}: {len(batch)} rows ({total_rows}/{len(df_to_load)} total)")
        
        # Enregistrer le batch
        self._log_batch(batch_id, len(df_to_load))
        
        logger.info(f"✅ Chargé {total_rows:,} rows dans hse_incidents_global")
        return total_rows
    
    def _log_batch(self, batch_id: str, rows: int):
        """Enregistrer le batch d'ingestion"""
        with self.engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO hse_ingestion_batches 
                (batch_id, started_at, completed_at, status, total_rows_ingested)
                VALUES (:batch_id, :started, :completed, 'success', :rows)
            """), {
                "batch_id": batch_id,
                "started": datetime.now(),
                "completed": datetime.now(),
                "rows": rows
            })
            conn.commit()
    
    def update_source_metadata(
        self, 
        source_key: str, 
        source_name: str,
        jurisdiction: str,
        rows: int,
        status: str = "success",
        error: str = None
    ):
        """Mettre à jour les métadonnées de source"""
        with self.engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO hse_data_sources 
                (source_key, source_name, jurisdiction, last_ingestion, rows_ingested, status, error_message, updated_at)
                VALUES (:key, :name, :jurisdiction, CURRENT_TIMESTAMP, :rows, :status, :error, CURRENT_TIMESTAMP)
                ON CONFLICT (source_key) DO UPDATE SET
                    source_name = :name,
                    jurisdiction = :jurisdiction,
                    last_ingestion = CURRENT_TIMESTAMP,
                    rows_ingested = hse_data_sources.rows_ingested + :rows,
                    total_rows = hse_data_sources.total_rows + :rows,
                    status = :status,
                    error_message = :error,
                    updated_at = CURRENT_TIMESTAMP
            """), {
                "key": source_key,
                "name": source_name,
                "jurisdiction": jurisdiction,
                "rows": rows,
                "status": status,
                "error": error
            })
            conn.commit()
        
        logger.info(f"✅ Source metadata updated: {source_key}")
    
    def get_summary(self) -> pd.DataFrame:
        """Obtenir un résumé des données par juridiction"""
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT * FROM hse_dashboard_summary
            """))
            return pd.DataFrame(result.fetchall(), columns=result.keys())
    
    def get_yearly_stats(self, jurisdiction: str = None) -> pd.DataFrame:
        """Obtenir les statistiques annuelles"""
        query = """
            SELECT 
                jurisdiction,
                year,
                COUNT(*) as incidents,
                SUM(CASE WHEN incident_type = 'fatal' THEN 1 ELSE 0 END) as fatalities,
                ROUND(AVG(days_lost)::numeric, 1) as avg_days_lost
            FROM hse_incidents_global
        """
        
        if jurisdiction:
            query += f" WHERE jurisdiction = '{jurisdiction}'"
        
        query += """
            GROUP BY jurisdiction, year
            ORDER BY jurisdiction, year DESC
        """
        
        with self.engine.connect() as conn:
            result = conn.execute(text(query))
            return pd.DataFrame(result.fetchall(), columns=result.keys())
    
    def get_sources_status(self) -> pd.DataFrame:
        """Obtenir le statut de toutes les sources"""
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    source_key,
                    source_name,
                    jurisdiction,
                    last_ingestion,
                    rows_ingested,
                    total_rows,
                    status
                FROM hse_data_sources
                ORDER BY last_ingestion DESC NULLS LAST
            """))
            return pd.DataFrame(result.fetchall(), columns=result.keys())
    
    def query(self, sql: str) -> pd.DataFrame:
        """Exécuter une requête SQL arbitraire"""
        with self.engine.connect() as conn:
            result = conn.execute(text(sql))
            return pd.DataFrame(result.fetchall(), columns=result.keys())
    
    def health_check(self) -> Dict:
        """Vérifier la santé de la connexion et des données"""
        try:
            with self.engine.connect() as conn:
                # Test connexion
                conn.execute(text("SELECT 1"))
                
                # Compter les incidents
                result = conn.execute(text("""
                    SELECT 
                        COUNT(*) as total_incidents,
                        COUNT(DISTINCT jurisdiction) as jurisdictions,
                        COUNT(DISTINCT source) as sources,
                        MIN(incident_date) as oldest_incident,
                        MAX(incident_date) as newest_incident,
                        MAX(updated_at) as last_update
                    FROM hse_incidents_global
                """))
                
                row = result.fetchone()
                
                return {
                    "status": "healthy",
                    "database": os.getenv('POSTGRES_DB', 'safety_graph'),
                    "total_incidents": row[0] if row else 0,
                    "jurisdictions": row[1] if row else 0,
                    "sources": row[2] if row else 0,
                    "oldest_incident": str(row[3]) if row and row[3] else None,
                    "newest_incident": str(row[4]) if row and row[4] else None,
                    "last_update": str(row[5]) if row and row[5] else None,
                    "checked_at": datetime.now().isoformat()
                }
                
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "checked_at": datetime.now().isoformat()
            }


# ============================================================
# CLI
# ============================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Safety Graph PostgreSQL Loader")
    parser.add_argument("--create-tables", action="store_true", help="Créer les tables")
    parser.add_argument("--load", type=str, help="Charger un fichier Parquet")
    parser.add_argument("--summary", action="store_true", help="Afficher le résumé")
    parser.add_argument("--health", action="store_true", help="Health check")
    parser.add_argument("--query", type=str, help="Exécuter une requête SQL")
    
    args = parser.parse_args()
    
    loader = SafetyGraphLoader()
    
    if args.create_tables:
        loader.create_tables()
    
    if args.load:
        loader.load_gold_data(args.load)
    
    if args.summary:
        print("\n📊 Safety Graph Summary:")
        print(loader.get_summary().to_string())
    
    if args.health:
        import json
        print("\n🏥 Health Check:")
        print(json.dumps(loader.health_check(), indent=2))
    
    if args.query:
        print("\n📋 Query Results:")
        print(loader.query(args.query).to_string())
