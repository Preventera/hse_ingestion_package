#!/usr/bin/env python3
"""
============================================================================
SAFETWIN X5 - NEO4J KNOWLEDGE GRAPH LOADER
============================================================================
Transfert des données PostgreSQL (2.85M records) vers Neo4j SafetyGraph.
Génère des triplets RDF et alimente le Knowledge Graph.

Version: 1.0.0
Date: 2026-01-13
Neo4j: 5.26.8 Enterprise (localhost:7687)
============================================================================
"""

import os
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from dataclasses import dataclass
import json

from neo4j import GraphDatabase
from sqlalchemy import create_engine
import pandas as pd

# Configuration logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
POSTGRES_URL = "postgresql://postgres:postgres@localhost:5432/safety_graph"
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = ""  # Auth disabled


@dataclass
class TransferStats:
    """Statistiques de transfert"""
    source: str
    records_read: int
    nodes_created: int
    relationships_created: int
    duration_seconds: float
    errors: int


class Neo4jKGLoader:
    """
    Loader pour transférer les données HSE de PostgreSQL vers Neo4j.
    """
    
    def __init__(self):
        self.pg_engine = create_engine(POSTGRES_URL)
        self.neo4j_driver = None
        logger.info("🧠 Neo4j KG Loader initialisé")
        
    def connect_neo4j(self):
        """Connexion à Neo4j"""
        try:
            self.neo4j_driver = GraphDatabase.driver(
                NEO4J_URI,
                auth=(NEO4J_USER, NEO4J_PASSWORD) if NEO4J_PASSWORD else None
            )
            self.neo4j_driver.verify_connectivity()
            logger.info(f"✅ Connecté à Neo4j: {NEO4J_URI}")
            return True
        except Exception as e:
            logger.error(f"❌ Erreur connexion Neo4j: {e}")
            return False
    
    def close(self):
        """Fermer les connexions"""
        if self.neo4j_driver:
            self.neo4j_driver.close()
            logger.info("🔌 Connexion Neo4j fermée")
    
    def setup_constraints(self):
        """Créer les contraintes et index Neo4j"""
        logger.info("🔧 Création des contraintes et index...")
        
        constraints = [
            "CREATE CONSTRAINT sector_code IF NOT EXISTS FOR (s:Sector) REQUIRE s.code IS UNIQUE",
            "CREATE CONSTRAINT jurisdiction_code IF NOT EXISTS FOR (j:Jurisdiction) REQUIRE j.code IS UNIQUE",
            "CREATE CONSTRAINT lesion_type_code IF NOT EXISTS FOR (l:LesionType) REQUIRE l.code IS UNIQUE",
            "CREATE CONSTRAINT year_value IF NOT EXISTS FOR (y:Year) REQUIRE y.value IS UNIQUE",
            "CREATE CONSTRAINT stats_id IF NOT EXISTS FOR (st:IncidentStats) REQUIRE st.id IS UNIQUE",
            "CREATE INDEX sector_name IF NOT EXISTS FOR (s:Sector) ON (s.name)",
        ]
        
        with self.neo4j_driver.session() as session:
            for constraint in constraints:
                try:
                    session.run(constraint)
                except Exception as e:
                    logger.warning(f"  ⚠️ {e}")
        
        logger.info("✅ Contraintes créées")
    
    def create_jurisdictions(self):
        """Créer les nœuds Juridiction"""
        logger.info("🌍 Création des juridictions...")
        
        jurisdictions = [
            {"code": "USA", "name": "États-Unis", "flag": "🇺🇸", "source": "OSHA"},
            {"code": "EU27", "name": "Union Européenne", "flag": "🇪🇺", "source": "Eurostat"},
            {"code": "QC", "name": "Québec", "flag": "🇨🇦", "source": "CNESST"},
        ]
        
        query = """
        UNWIND $jurisdictions AS j
        MERGE (jurisdiction:Jurisdiction {code: j.code})
        SET jurisdiction.name = j.name,
            jurisdiction.flag = j.flag,
            jurisdiction.source = j.source,
            jurisdiction.updated_at = datetime()
        RETURN count(jurisdiction) as count
        """
        
        with self.neo4j_driver.session() as session:
            result = session.run(query, jurisdictions=jurisdictions)
            count = result.single()["count"]
            logger.info(f"  ✅ {count} juridictions créées")
        
        return len(jurisdictions)
    
    def create_years(self):
        """Créer les nœuds Année"""
        logger.info("📅 Création des années...")
        
        years = list(range(2010, 2024))
        
        query = """
        UNWIND $years AS y
        MERGE (year:Year {value: y})
        SET year.updated_at = datetime()
        RETURN count(year) as count
        """
        
        with self.neo4j_driver.session() as session:
            result = session.run(query, years=years)
            count = result.single()["count"]
            logger.info(f"  ✅ {count} années créées")
        
        return len(years)
    
    def transfer_cnesst_sectors(self) -> TransferStats:
        """Transférer les secteurs SCIAN depuis CNESST"""
        logger.info("🏭 Transfert des secteurs CNESST...")
        start_time = datetime.now()
        errors = 0
        nodes_created = 0
        rels_created = 0
        
        pg_query = """
        SELECT 
            "SECTEUR_SCIAN" as secteur,
            "ANNEE" as annee,
            COUNT(*) as total,
            SUM(CASE WHEN "IND_LESION_TMS" = 'OUI' THEN 1 ELSE 0 END) as tms,
            SUM(CASE WHEN "IND_LESION_PSY" = 'OUI' THEN 1 ELSE 0 END) as psy,
            SUM(CASE WHEN "IND_LESION_MACHINE" = 'OUI' THEN 1 ELSE 0 END) as machine
        FROM cnesst_lesions_quebec
        WHERE "SECTEUR_SCIAN" IS NOT NULL
        GROUP BY "SECTEUR_SCIAN", "ANNEE"
        ORDER BY total DESC
        """
        
        df = pd.read_sql(pg_query, self.pg_engine)
        logger.info(f"  📊 {len(df)} combinaisons secteur/année")
        
        # Créer les secteurs
        sectors = df['secteur'].unique()
        sector_query = """
        UNWIND $sectors AS s
        MERGE (sector:Sector {code: s})
        SET sector.name = s,
            sector.classification = 'SCIAN',
            sector.jurisdiction = 'QC',
            sector.updated_at = datetime()
        RETURN count(sector) as count
        """
        
        with self.neo4j_driver.session() as session:
            result = session.run(sector_query, sectors=list(sectors))
            nodes_created = result.single()["count"]
            logger.info(f"  ✅ {nodes_created} secteurs créés")
        
        # Créer les stats avec relations
        stats_query = """
        UNWIND $stats AS st
        MERGE (stats:IncidentStats {id: st.id})
        SET stats.total = st.total,
            stats.tms = st.tms,
            stats.tms_rate = st.tms_rate,
            stats.psy = st.psy,
            stats.machine = st.machine,
            stats.updated_at = datetime()
        WITH stats, st
        MATCH (sector:Sector {code: st.secteur})
        MERGE (sector)-[:HAS_STATS]->(stats)
        WITH stats, st
        MATCH (year:Year {value: st.annee})
        MERGE (stats)-[:IN_YEAR]->(year)
        WITH stats
        MATCH (jurisdiction:Jurisdiction {code: 'QC'})
        MERGE (stats)-[:IN_JURISDICTION]->(jurisdiction)
        RETURN count(stats) as count
        """
        
        stats_data = []
        for _, row in df.iterrows():
            total = int(row['total'])
            stats_data.append({
                "id": f"QC-{row['secteur'][:20]}-{int(row['annee'])}",
                "secteur": row['secteur'],
                "annee": int(row['annee']),
                "total": total,
                "tms": int(row['tms']),
                "tms_rate": round(row['tms'] / total * 100, 2) if total > 0 else 0,
                "psy": int(row['psy']),
                "machine": int(row['machine'])
            })
        
        batch_size = 500
        for i in range(0, len(stats_data), batch_size):
            batch = stats_data[i:i+batch_size]
            try:
                with self.neo4j_driver.session() as session:
                    result = session.run(stats_query, stats=batch)
                    rels_created += result.single()["count"] * 3
            except Exception as e:
                logger.error(f"  ❌ Erreur batch: {e}")
                errors += 1
        
        duration = (datetime.now() - start_time).total_seconds()
        return TransferStats("CNESST", len(df), nodes_created + len(stats_data), rels_created, duration, errors)
    
    def transfer_osha_summary(self) -> TransferStats:
        """Transférer résumé OSHA"""
        logger.info("🇺🇸 Transfert résumé OSHA...")
        start_time = datetime.now()
        
        pg_query = """
        SELECT state, COUNT(*) as total
        FROM osha_injuries_raw
        WHERE state IS NOT NULL
        GROUP BY state
        ORDER BY total DESC
        LIMIT 50
        """
        
        try:
            df = pd.read_sql(pg_query, self.pg_engine)
        except Exception as e:
            logger.error(f"  ❌ {e}")
            return TransferStats("OSHA", 0, 0, 0, 0, 1)
        
        states_query = """
        UNWIND $states AS st
        MERGE (state:State:Sector {code: st.code})
        SET state.name = st.name,
            state.total_incidents = st.total,
            state.jurisdiction = 'USA',
            state.updated_at = datetime()
        WITH state
        MATCH (jurisdiction:Jurisdiction {code: 'USA'})
        MERGE (state)-[:IN_JURISDICTION]->(jurisdiction)
        RETURN count(state) as count
        """
        
        states_data = [{"code": f"USA-{row['state']}", "name": row['state'], "total": int(row['total'])} 
                       for _, row in df.iterrows()]
        
        with self.neo4j_driver.session() as session:
            result = session.run(states_query, states=states_data)
            nodes_created = result.single()["count"]
        
        duration = (datetime.now() - start_time).total_seconds()
        return TransferStats("OSHA", len(df), nodes_created, nodes_created, duration, 0)
    
    def transfer_eurostat_summary(self) -> TransferStats:
        """Transférer résumé Eurostat"""
        logger.info("🇪🇺 Transfert résumé Eurostat...")
        start_time = datetime.now()
        
        pg_query = """
        SELECT country_code as country, country_name, COUNT(*) as total, SUM(CAST(incident_count AS INTEGER)) as incidents
        FROM eurostat_esaw
        WHERE country_code IS NOT NULL
        GROUP BY country_code, country_name
        ORDER BY total DESC
        """
        
        try:
            df = pd.read_sql(pg_query, self.pg_engine)
        except Exception as e:
            logger.error(f"  ❌ {e}")
            return TransferStats("Eurostat", 0, 0, 0, 0, 1)
        
        countries_query = """
        UNWIND $countries AS c
        MERGE (country:Country:Sector {code: c.code})
        SET country.name = c.name,
            country.total_records = c.total,
            country.jurisdiction = 'EU27',
            country.updated_at = datetime()
        WITH country
        MATCH (jurisdiction:Jurisdiction {code: 'EU27'})
        MERGE (country)-[:IN_JURISDICTION]->(jurisdiction)
        RETURN count(country) as count
        """
        
        countries_data = [{"code": f"EU-{row['country']}", "name": row['country'], "total": int(row['total'])} 
                          for _, row in df.iterrows()]
        
        with self.neo4j_driver.session() as session:
            result = session.run(countries_query, countries=countries_data)
            nodes_created = result.single()["count"]
        
        duration = (datetime.now() - start_time).total_seconds()
        return TransferStats("Eurostat", len(df), nodes_created, nodes_created, duration, 0)
    
    def get_kg_stats(self) -> Dict[str, Any]:
        """Obtenir les statistiques du Knowledge Graph"""
        query = """
        MATCH (n)
        WITH labels(n) AS labels, count(n) AS count
        UNWIND labels AS label
        RETURN label, sum(count) AS total
        ORDER BY total DESC
        """
        
        stats = {"nodes": {}, "total_nodes": 0, "total_relationships": 0}
        
        with self.neo4j_driver.session() as session:
            result = session.run(query)
            for record in result:
                stats["nodes"][record["label"]] = record["total"]
                stats["total_nodes"] += record["total"]
            
            rel_result = session.run("MATCH ()-[r]->() RETURN count(r) as count")
            stats["total_relationships"] = rel_result.single()["count"]
        
        return stats
    
    def run_full_transfer(self) -> Dict[str, Any]:
        """Exécuter le transfert complet"""
        logger.info("\n" + "="*60)
        logger.info("🚀 TRANSFERT PostgreSQL → Neo4j")
        logger.info("="*60)
        
        start_time = datetime.now()
        results = {"transfers": [], "stats_before": None, "stats_after": None}
        
        if not self.connect_neo4j():
            return {"error": "Connexion Neo4j impossible"}
        
        results["stats_before"] = self.get_kg_stats()
        
        self.setup_constraints()
        self.create_jurisdictions()
        self.create_years()
        
        results["transfers"].append(self.transfer_cnesst_sectors().__dict__)
        results["transfers"].append(self.transfer_osha_summary().__dict__)
        results["transfers"].append(self.transfer_eurostat_summary().__dict__)
        
        results["stats_after"] = self.get_kg_stats()
        results["duration"] = (datetime.now() - start_time).total_seconds()
        
        logger.info("\n" + "="*60)
        logger.info("✅ TRANSFERT TERMINÉ")
        logger.info(f"📊 Nœuds: {results['stats_after']['total_nodes']}")
        logger.info(f"🔗 Relations: {results['stats_after']['total_relationships']}")
        logger.info("="*60)
        
        return results


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Neo4j KG Loader")
    parser.add_argument("--transfer", "-t", action="store_true", help="Lancer transfert")
    parser.add_argument("--stats", "-s", action="store_true", help="Stats KG")
    parser.add_argument("--test", action="store_true", help="Test connexion")
    
    args = parser.parse_args()
    loader = Neo4jKGLoader()
    
    if args.test:
        if loader.connect_neo4j():
            print("✅ Connexion Neo4j OK!")
            stats = loader.get_kg_stats()
            print(f"📊 Nœuds: {stats['total_nodes']}")
        loader.close()
    elif args.transfer:
        results = loader.run_full_transfer()
        print(json.dumps(results, indent=2, default=str))
        loader.close()
    elif args.stats:
        if loader.connect_neo4j():
            stats = loader.get_kg_stats()
            print(f"\n📊 STATS NEO4J")
            print(f"Nœuds: {stats['total_nodes']}")
            print(f"Relations: {stats['total_relationships']}")
            for label, count in stats['nodes'].items():
                print(f"  • {label}: {count}")
        loader.close()
    else:
        parser.print_help()
