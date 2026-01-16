#!/usr/bin/env python3
"""
================================================================================
SAFETY GRAPH INTEGRATION PIPELINE
================================================================================
Script principal d'intégration entre hse_ingestion_package et l'architecture
SafetyGraph (schéma, concordances, Knowledge Graph).

Ce script orchestre:
1. Extraction via hse_data_ingestion.py (existant)
2. Harmonisation via hse_harmonizer.py (nouveau)
3. Chargement via neo4j_loader.py (nouveau) et postgresql_loader.py (existant)

Usage:
    # Mode interactif
    python integration_pipeline.py
    
    # Ligne de commande
    python integration_pipeline.py --source osha --output neo4j
    python integration_pipeline.py --source cnesst --output postgresql
    python integration_pipeline.py --all --validate

© 2026 AgenticX5 — GenAISafety / Preventera
================================================================================
"""

import os
import sys
import json
import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any

# Configuration logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("IntegrationPipeline")

# Imports locaux
try:
    from hse_harmonizer import HSEHarmonizer, convert_osha_to_unified, convert_esaw_to_unified, convert_cnesst_to_unified
    HARMONIZER_AVAILABLE = True
except ImportError:
    HARMONIZER_AVAILABLE = False
    logger.warning("hse_harmonizer.py non trouvé dans le chemin")

try:
    from neo4j_loader import SafetyGraphLoader, get_loader
    NEO4J_LOADER_AVAILABLE = True
except ImportError:
    NEO4J_LOADER_AVAILABLE = False
    logger.warning("neo4j_loader.py non trouvé dans le chemin")

# Tentative d'import du module d'ingestion existant
try:
    from hse_data_ingestion import HSEDataIngestion
    INGESTION_AVAILABLE = True
except ImportError:
    INGESTION_AVAILABLE = False
    logger.warning("hse_data_ingestion.py non trouvé - certaines sources ne seront pas disponibles")

try:
    from postgresql_loader import PostgreSQLLoader
    POSTGRESQL_AVAILABLE = True
except ImportError:
    POSTGRESQL_AVAILABLE = False
    logger.warning("postgresql_loader.py non trouvé")


# =============================================================================
# CONFIGURATION
# =============================================================================

DEFAULT_CONFIG = {
    "neo4j": {
        "uri": os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        "user": os.getenv("NEO4J_USER", "neo4j"),
        "password": os.getenv("NEO4J_PASSWORD", "password"),
        "database": os.getenv("NEO4J_DATABASE", "neo4j")
    },
    "postgresql": {
        "host": os.getenv("PG_HOST", "localhost"),
        "port": int(os.getenv("PG_PORT", "5432")),
        "database": os.getenv("PG_DATABASE", "safetygraph"),
        "user": os.getenv("PG_USER", "postgres"),
        "password": os.getenv("PG_PASSWORD", "")
    },
    "paths": {
        "schema": os.getenv("SCHEMA_PATH", "01_metadata_schema.json"),
        "output_dir": os.getenv("OUTPUT_DIR", "./output"),
        "data_dir": os.getenv("DATA_DIR", "./data")
    }
}


# =============================================================================
# CLASSE PIPELINE
# =============================================================================

class IntegrationPipeline:
    """
    Pipeline d'intégration complet pour SafetyGraph.
    """
    
    def __init__(self, config: Dict = None):
        """
        Initialise le pipeline.
        
        Args:
            config: Configuration personnalisée (optionnel)
        """
        self.config = config or DEFAULT_CONFIG
        
        # Initialiser les composants
        self.harmonizer = None
        self.neo4j_loader = None
        self.pg_loader = None
        self.ingestion = None
        
        # Statistiques globales
        self.stats = {
            "pipeline_start": None,
            "pipeline_end": None,
            "sources_processed": [],
            "total_extracted": 0,
            "total_harmonized": 0,
            "total_loaded_neo4j": 0,
            "total_loaded_pg": 0,
            "errors": []
        }
        
        self._init_components()
    
    def _init_components(self):
        """Initialise les composants disponibles"""
        # Harmonizer
        if HARMONIZER_AVAILABLE:
            schema_path = self.config["paths"].get("schema")
            if schema_path and Path(schema_path).exists():
                self.harmonizer = HSEHarmonizer(schema_path)
            else:
                self.harmonizer = HSEHarmonizer()
            logger.info("✅ Harmonizer initialisé")
        else:
            logger.warning("❌ Harmonizer non disponible")
        
        # Ingestion (module existant)
        if INGESTION_AVAILABLE:
            self.ingestion = HSEDataIngestion()
            logger.info("✅ Module d'ingestion initialisé")
        else:
            logger.warning("❌ Module d'ingestion non disponible")
    
    def connect_neo4j(self, mock: bool = False) -> bool:
        """
        Établit la connexion Neo4j.
        
        Args:
            mock: Utiliser le mode simulation
        """
        if not NEO4J_LOADER_AVAILABLE:
            logger.error("Neo4j loader non disponible")
            return False
        
        try:
            cfg = self.config["neo4j"]
            self.neo4j_loader = get_loader(
                uri=cfg["uri"],
                user=cfg["user"],
                password=cfg["password"],
                mock=mock
            )
            logger.info("✅ Connexion Neo4j établie")
            return True
        except Exception as e:
            logger.error(f"❌ Erreur connexion Neo4j: {e}")
            self.stats["errors"].append(f"Neo4j: {e}")
            return False
    
    def connect_postgresql(self) -> bool:
        """Établit la connexion PostgreSQL"""
        if not POSTGRESQL_AVAILABLE:
            logger.error("PostgreSQL loader non disponible")
            return False
        
        try:
            cfg = self.config["postgresql"]
            self.pg_loader = PostgreSQLLoader(
                host=cfg["host"],
                port=cfg["port"],
                database=cfg["database"],
                user=cfg["user"],
                password=cfg["password"]
            )
            logger.info("✅ Connexion PostgreSQL établie")
            return True
        except Exception as e:
            logger.error(f"❌ Erreur connexion PostgreSQL: {e}")
            self.stats["errors"].append(f"PostgreSQL: {e}")
            return False
    
    # -------------------------------------------------------------------------
    # Extraction des données
    # -------------------------------------------------------------------------
    
    def extract_from_source(self, source: str) -> List[Dict]:
        """
        Extrait les données d'une source.
        
        Args:
            source: Nom de la source (osha, esaw, cnesst, kaggle, etc.)
            
        Returns:
            Liste des enregistrements bruts
        """
        records = []
        
        if self.ingestion:
            try:
                # Utiliser le module d'ingestion existant
                df = self.ingestion.fetch_data(source)
                if df is not None and not df.empty:
                    records = df.to_dict('records')
                    logger.info(f"✅ Extrait {len(records)} enregistrements de {source}")
            except Exception as e:
                logger.error(f"❌ Erreur extraction {source}: {e}")
                self.stats["errors"].append(f"Extract {source}: {e}")
        else:
            logger.warning(f"Module d'ingestion non disponible pour {source}")
        
        return records
    
    def extract_from_file(self, filepath: str) -> List[Dict]:
        """
        Extrait les données d'un fichier local (CSV, JSON).
        
        Args:
            filepath: Chemin vers le fichier
            
        Returns:
            Liste des enregistrements
        """
        records = []
        path = Path(filepath)
        
        if not path.exists():
            logger.error(f"Fichier non trouvé: {filepath}")
            return records
        
        try:
            if path.suffix.lower() == '.json':
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    records = data if isinstance(data, list) else [data]
            
            elif path.suffix.lower() == '.csv':
                import pandas as pd
                df = pd.read_csv(path)
                records = df.to_dict('records')
            
            logger.info(f"✅ Extrait {len(records)} enregistrements de {filepath}")
            
        except Exception as e:
            logger.error(f"❌ Erreur lecture fichier: {e}")
            self.stats["errors"].append(f"File {filepath}: {e}")
        
        return records
    
    # -------------------------------------------------------------------------
    # Harmonisation
    # -------------------------------------------------------------------------
    
    def harmonize_records(self, records: List[Dict], source: str = None) -> List[Dict]:
        """
        Harmonise les enregistrements vers le format unifié.
        
        Args:
            records: Enregistrements bruts
            source: Source des données pour détection du format
            
        Returns:
            Enregistrements harmonisés
        """
        if not self.harmonizer:
            logger.error("Harmonizer non disponible")
            return []
        
        harmonized = self.harmonizer.transform_batch(records, source)
        self.stats["total_harmonized"] += len(harmonized)
        
        logger.info(f"✅ Harmonisé {len(harmonized)}/{len(records)} enregistrements")
        return harmonized
    
    # -------------------------------------------------------------------------
    # Chargement
    # -------------------------------------------------------------------------
    
    def load_to_neo4j(self, records: List[Dict]) -> int:
        """
        Charge les enregistrements harmonisés dans Neo4j.
        
        Args:
            records: Enregistrements au format unifié
            
        Returns:
            Nombre d'enregistrements chargés
        """
        if not self.neo4j_loader:
            logger.error("Neo4j loader non connecté")
            return 0
        
        count = self.neo4j_loader.load_unified_records(records)
        self.stats["total_loaded_neo4j"] += count
        
        logger.info(f"✅ Chargé {count} enregistrements dans Neo4j")
        return count
    
    def load_to_postgresql(self, records: List[Dict], table: str = "unified_risks") -> int:
        """
        Charge les enregistrements harmonisés dans PostgreSQL.
        
        Args:
            records: Enregistrements au format unifié
            table: Nom de la table cible
            
        Returns:
            Nombre d'enregistrements chargés
        """
        if not self.pg_loader:
            logger.error("PostgreSQL loader non connecté")
            return 0
        
        try:
            count = self.pg_loader.load_records(records, table)
            self.stats["total_loaded_pg"] += count
            logger.info(f"✅ Chargé {count} enregistrements dans PostgreSQL")
            return count
        except Exception as e:
            logger.error(f"❌ Erreur chargement PostgreSQL: {e}")
            self.stats["errors"].append(f"PG load: {e}")
            return 0
    
    def save_to_json(self, records: List[Dict], filename: str) -> str:
        """
        Sauvegarde les enregistrements harmonisés en JSON.
        
        Args:
            records: Enregistrements
            filename: Nom du fichier de sortie
            
        Returns:
            Chemin du fichier créé
        """
        output_dir = Path(self.config["paths"]["output_dir"])
        output_dir.mkdir(parents=True, exist_ok=True)
        
        filepath = output_dir / filename
        
        # Nettoyer les données pour sérialisation JSON
        clean_records = []
        for r in records:
            clean = {k: v for k, v in r.items() if k != "_source_record"}
            clean_records.append(clean)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(clean_records, f, ensure_ascii=False, indent=2, default=str)
        
        logger.info(f"✅ Sauvegardé {len(records)} enregistrements dans {filepath}")
        return str(filepath)
    
    # -------------------------------------------------------------------------
    # Pipeline complet
    # -------------------------------------------------------------------------
    
    def run_pipeline(
        self,
        source: str,
        output: str = "json",
        validate: bool = False
    ) -> Dict:
        """
        Exécute le pipeline complet pour une source.
        
        Args:
            source: Source de données (osha, esaw, cnesst, ou chemin fichier)
            output: Destination (json, neo4j, postgresql, all)
            validate: Valider contre le JSON Schema
            
        Returns:
            Résultats du pipeline
        """
        self.stats["pipeline_start"] = datetime.now().isoformat()
        results = {
            "source": source,
            "extracted": 0,
            "harmonized": 0,
            "loaded": 0,
            "output_files": []
        }
        
        logger.info(f"\n{'='*60}")
        logger.info(f"🚀 PIPELINE: {source} → {output}")
        logger.info(f"{'='*60}")
        
        # 1. Extraction
        if Path(source).exists():
            records = self.extract_from_file(source)
            source_type = self._detect_source_type(records)
        else:
            records = self.extract_from_source(source)
            source_type = source
        
        results["extracted"] = len(records)
        self.stats["total_extracted"] += len(records)
        
        if not records:
            logger.warning("Aucun enregistrement extrait")
            return results
        
        # 2. Harmonisation
        harmonized = self.harmonize_records(records, source_type)
        results["harmonized"] = len(harmonized)
        
        if not harmonized:
            logger.warning("Aucun enregistrement harmonisé")
            return results
        
        # 3. Validation (optionnelle)
        if validate and self.harmonizer:
            valid_count = 0
            for record in harmonized:
                is_valid, errors = self.harmonizer.validate_record(record)
                if is_valid:
                    valid_count += 1
            logger.info(f"✅ Validation: {valid_count}/{len(harmonized)} valides")
        
        # 4. Chargement selon destination
        if output in ["json", "all"]:
            filename = f"harmonized_{source_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            filepath = self.save_to_json(harmonized, filename)
            results["output_files"].append(filepath)
        
        if output in ["neo4j", "all"]:
            if self.neo4j_loader or self.connect_neo4j(mock=False):
                loaded = self.load_to_neo4j(harmonized)
                results["loaded"] += loaded
        
        if output in ["postgresql", "pg", "all"]:
            if self.pg_loader or self.connect_postgresql():
                loaded = self.load_to_postgresql(harmonized)
                results["loaded"] += loaded
        
        self.stats["sources_processed"].append(source)
        self.stats["pipeline_end"] = datetime.now().isoformat()
        
        return results
    
    def _detect_source_type(self, records: List[Dict]) -> str:
        """Détecte le type de source basé sur les champs"""
        if not records:
            return "unknown"
        
        sample = records[0]
        
        if "event_type" in sample or "EventType" in sample or "ActivityNr" in sample:
            return "osha"
        elif "deviation" in sample or "DEVIATION" in sample:
            return "esaw"
        elif "genre_accident" in sample or "GenreAccident" in sample:
            return "cnesst"
        else:
            return "generic"
    
    # -------------------------------------------------------------------------
    # Rapports et statistiques
    # -------------------------------------------------------------------------
    
    def print_summary(self):
        """Affiche le résumé du pipeline"""
        print("\n" + "="*70)
        print("📊 RÉSUMÉ DU PIPELINE D'INTÉGRATION")
        print("="*70)
        
        print(f"\n⏱️  Période: {self.stats['pipeline_start']} → {self.stats['pipeline_end']}")
        
        print(f"\n📥 EXTRACTION:")
        print(f"   Sources traitées: {', '.join(self.stats['sources_processed']) or 'Aucune'}")
        print(f"   Total extrait:    {self.stats['total_extracted']}")
        
        print(f"\n🔄 HARMONISATION:")
        print(f"   Total harmonisé:  {self.stats['total_harmonized']}")
        if self.harmonizer:
            h_stats = self.harmonizer.get_stats()
            print(f"   Par domaine:")
            for domain, count in sorted(h_stats.get("by_domain", {}).items(), key=lambda x: -x[1])[:5]:
                print(f"     - {domain}: {count}")
        
        print(f"\n📤 CHARGEMENT:")
        print(f"   Neo4j:       {self.stats['total_loaded_neo4j']}")
        print(f"   PostgreSQL:  {self.stats['total_loaded_pg']}")
        
        if self.stats["errors"]:
            print(f"\n❌ ERREURS ({len(self.stats['errors'])}):")
            for err in self.stats["errors"][:5]:
                print(f"   - {err}")
        
        print("\n" + "="*70)
    
    def close(self):
        """Ferme les connexions"""
        if self.neo4j_loader:
            self.neo4j_loader.close()
        if self.pg_loader:
            self.pg_loader.close()
        logger.info("Connexions fermées")


# =============================================================================
# INTERFACE CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="SafetyGraph Integration Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples:
  python integration_pipeline.py --source osha --output json
  python integration_pipeline.py --source data/cnesst_export.csv --output neo4j
  python integration_pipeline.py --all --output all --validate
  python integration_pipeline.py --interactive
        """
    )
    
    parser.add_argument(
        "--source", "-s",
        help="Source de données (osha, esaw, cnesst, ou chemin fichier)"
    )
    parser.add_argument(
        "--output", "-o",
        choices=["json", "neo4j", "postgresql", "pg", "all"],
        default="json",
        help="Destination des données harmonisées"
    )
    parser.add_argument(
        "--validate", "-v",
        action="store_true",
        help="Valider contre le JSON Schema"
    )
    parser.add_argument(
        "--all", "-a",
        action="store_true",
        help="Traiter toutes les sources disponibles"
    )
    parser.add_argument(
        "--interactive", "-i",
        action="store_true",
        help="Mode interactif"
    )
    parser.add_argument(
        "--mock",
        action="store_true",
        help="Utiliser les mocks (pas de connexion réelle)"
    )
    
    args = parser.parse_args()
    
    # Mode interactif
    if args.interactive or (not args.source and not args.all):
        run_interactive()
        return
    
    # Initialiser le pipeline
    pipeline = IntegrationPipeline()
    
    # Connexions si nécessaires
    if args.output in ["neo4j", "all"]:
        pipeline.connect_neo4j(mock=args.mock)
    if args.output in ["postgresql", "pg", "all"]:
        pipeline.connect_postgresql()
    
    # Exécution
    if args.all:
        sources = ["osha", "esaw", "cnesst"]
        for src in sources:
            try:
                pipeline.run_pipeline(src, args.output, args.validate)
            except Exception as e:
                logger.error(f"Erreur source {src}: {e}")
    elif args.source:
        pipeline.run_pipeline(args.source, args.output, args.validate)
    
    # Résumé
    pipeline.print_summary()
    pipeline.close()


def run_interactive():
    """Mode interactif avec menu"""
    print("\n" + "="*60)
    print("🔄 SAFETYGRAPH INTEGRATION PIPELINE")
    print("="*60)
    
    pipeline = IntegrationPipeline()
    
    while True:
        print("\n📋 MENU PRINCIPAL:")
        print("  1. Extraire et harmoniser une source")
        print("  2. Charger un fichier local")
        print("  3. Connecter Neo4j")
        print("  4. Connecter PostgreSQL")
        print("  5. Voir les statistiques")
        print("  6. Test rapide (mock)")
        print("  0. Quitter")
        
        choice = input("\nChoix: ").strip()
        
        if choice == "0":
            break
        
        elif choice == "1":
            print("\nSources disponibles: osha, esaw, cnesst, kaggle_osha, kaggle_industrial")
            source = input("Source: ").strip()
            output = input("Sortie (json/neo4j/postgresql): ").strip() or "json"
            pipeline.run_pipeline(source, output)
        
        elif choice == "2":
            filepath = input("Chemin du fichier: ").strip()
            output = input("Sortie (json/neo4j/postgresql): ").strip() or "json"
            pipeline.run_pipeline(filepath, output)
        
        elif choice == "3":
            pipeline.connect_neo4j()
        
        elif choice == "4":
            pipeline.connect_postgresql()
        
        elif choice == "5":
            pipeline.print_summary()
            if pipeline.harmonizer:
                pipeline.harmonizer.print_stats()
        
        elif choice == "6":
            # Test rapide avec données fictives
            print("\n🧪 Test rapide avec données simulées...")
            pipeline.connect_neo4j(mock=True)
            
            test_data = [
                {"event_type": "42", "naics_code": "23821", "id": "TEST001"},
                {"event_type": "51", "naics_code": "23821", "id": "TEST002"},
                {"genre_accident": "31", "scian": "23", "id": "TEST003"},
            ]
            
            harmonized = pipeline.harmonize_records(test_data)
            pipeline.load_to_neo4j(harmonized)
            pipeline.save_to_json(harmonized, "test_output.json")
            
            print("✅ Test terminé")
    
    pipeline.print_summary()
    pipeline.close()
    print("\n👋 Au revoir!")


# =============================================================================
# POINT D'ENTRÉE
# =============================================================================

if __name__ == "__main__":
    main()
