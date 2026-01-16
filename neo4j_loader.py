"""
================================================================================
NEO4J KNOWLEDGE GRAPH LOADER - SafetyGraph
================================================================================
Ce module charge les données HSE harmonisées dans Neo4j pour construire
le Knowledge Graph des risques professionnels et relations normatives.

Fonctionnalités:
- Création/mise à jour des nœuds Risque à partir des données harmonisées
- Liaison automatique aux domaines, secteurs, juridictions existants
- Gestion des relations normatives (normes applicables)
- Requêtes de recommandation et concordance

Prérequis:
    pip install neo4j

Usage:
    from neo4j_loader import SafetyGraphLoader
    
    loader = SafetyGraphLoader("bolt://localhost:7687", "neo4j", "password")
    loader.load_unified_records(harmonized_records)
    loader.close()

© 2026 AgenticX5 — GenAISafety / Preventera
================================================================================
"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

# Configuration logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SafetyGraphLoader")

# Tentative d'import Neo4j
try:
    from neo4j import GraphDatabase
    NEO4J_AVAILABLE = True
except ImportError:
    NEO4J_AVAILABLE = False
    logger.warning("Neo4j driver non installé. Installer avec: pip install neo4j")


# =============================================================================
# CLASSE PRINCIPALE
# =============================================================================

class SafetyGraphLoader:
    """
    Chargeur de données pour le Knowledge Graph SafetyGraph (Neo4j).
    """
    
    def __init__(self, uri: str, user: str, password: str, database: str = "neo4j"):
        """
        Initialise la connexion Neo4j.
        
        Args:
            uri: URI de connexion (ex: bolt://localhost:7687)
            user: Nom d'utilisateur
            password: Mot de passe
            database: Nom de la base de données
        """
        if not NEO4J_AVAILABLE:
            raise ImportError("Neo4j driver non disponible. Installer: pip install neo4j")
        
        self.uri = uri
        self.database = database
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        
        # Statistiques
        self.stats = {
            "nodes_created": 0,
            "nodes_updated": 0,
            "relationships_created": 0,
            "errors": 0
        }
        
        logger.info(f"Connexion Neo4j établie: {uri}")
    
    def close(self):
        """Ferme la connexion"""
        if self.driver:
            self.driver.close()
            logger.info("Connexion Neo4j fermée")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    # -------------------------------------------------------------------------
    # Vérification et initialisation du schéma
    # -------------------------------------------------------------------------
    
    def verify_schema(self) -> bool:
        """Vérifie que le schéma de base existe dans Neo4j"""
        with self.driver.session(database=self.database) as session:
            result = session.run("""
                MATCH (d:DomaineRisque)
                RETURN count(d) as domain_count
            """)
            record = result.single()
            count = record["domain_count"] if record else 0
            
            if count == 0:
                logger.warning("Schéma de base non trouvé. Exécuter d'abord le script Cypher d'architecture.")
                return False
            
            logger.info(f"Schéma vérifié: {count} domaines de risque trouvés")
            return True
    
    def initialize_base_schema(self) -> None:
        """
        Initialise le schéma de base si nécessaire.
        Crée les domaines de risque, juridictions de base.
        """
        with self.driver.session(database=self.database) as session:
            # Créer les domaines de risque
            domains = [
                ("ELEC", "Électricité", "Electrical", "⚡", "#FFA500", 5),
                ("CHUTE", "Chutes", "Falls", "🪜", "#FF0000", 5),
                ("INCENDIE", "Incendie/Explosion", "Fire/Explosion", "🔥", "#FF4500", 5),
                ("MACHINE", "Machines", "Machinery", "⚙️", "#808080", 4),
                ("ERGO", "Ergonomie", "Ergonomics", "🦴", "#9370DB", 3),
                ("CHIMIQUE", "Chimique", "Chemical", "☣️", "#32CD32", 4),
                ("GAZ", "Gaz", "Gas", "💨", "#87CEEB", 5),
                ("CONFINE", "Espaces clos", "Confined Spaces", "🚪", "#4169E1", 5),
                ("THERMIQUE", "Thermique", "Thermal", "🌡️", "#DC143C", 3),
                ("RPS", "Psychosociaux", "Psychosocial", "🧠", "#9932CC", 3),
                ("BIOLOGIQUE", "Biologique", "Biological", "🦠", "#228B22", 4),
                ("BRUIT", "Bruit", "Noise", "🔊", "#FF6347", 3),
                ("VEHICULE", "Véhicules", "Vehicles", "🚗", "#4682B4", 5),
                ("AUTRE", "Autre", "Other", "❓", "#A9A9A9", 2),
            ]
            
            for code, nom_fr, nom_en, icone, couleur, niveau in domains:
                session.run("""
                    MERGE (d:DomaineRisque {code: $code})
                    SET d.nom_fr = $nom_fr,
                        d.nom_en = $nom_en,
                        d.icone = $icone,
                        d.couleur = $couleur,
                        d.niveau_danger = $niveau
                """, code=code, nom_fr=nom_fr, nom_en=nom_en, icone=icone, couleur=couleur, niveau=niveau)
            
            # Créer les juridictions de base
            jurisdictions = [
                ("US", "États-Unis", "United States", "FEDERAL"),
                ("CA", "Canada", "Canada", "FEDERAL"),
                ("QC", "Québec", "Quebec", "STATE_PROVINCIAL"),
                ("ON", "Ontario", "Ontario", "STATE_PROVINCIAL"),
                ("EU", "Union Européenne", "European Union", "SUPRANATIONAL"),
                ("FR", "France", "France", "NATIONAL"),
                ("DE", "Allemagne", "Germany", "NATIONAL"),
                ("GB", "Royaume-Uni", "United Kingdom", "NATIONAL"),
                ("INT", "International", "International", "INTERNATIONAL"),
            ]
            
            for code, nom_fr, nom_en, type_j in jurisdictions:
                session.run("""
                    MERGE (j:Juridiction {code: $code})
                    SET j.nom_fr = $nom_fr,
                        j.nom_en = $nom_en,
                        j.type = $type
                """, code=code, nom_fr=nom_fr, nom_en=nom_en, type=type_j)
            
            logger.info("Schéma de base initialisé")
    
    # -------------------------------------------------------------------------
    # Chargement des enregistrements unifiés
    # -------------------------------------------------------------------------
    
    def load_unified_record(self, record: Dict) -> bool:
        """
        Charge un enregistrement unifié dans Neo4j.
        
        Args:
            record: Enregistrement au format unifié SafeTwin
            
        Returns:
            True si succès, False sinon
        """
        try:
            risk_id = record.get("risk_id")
            risk_code = record.get("risk_code", "")
            
            if not risk_id:
                logger.error("risk_id manquant dans l'enregistrement")
                self.stats["errors"] += 1
                return False
            
            # Extraire les informations clés
            title_fr = record.get("title", {}).get("fr", "")
            title_en = record.get("title", {}).get("en", "")
            
            hazard = record.get("hazard_classification", {})
            unified = hazard.get("unified", {})
            domain_code = unified.get("domain_code", "AUTRE")
            
            jurisdiction = record.get("jurisdiction", {})
            primary_country = jurisdiction.get("primary_country", "XX")
            subdivision = jurisdiction.get("subdivision", {})
            subdivision_code = subdivision.get("code", "").split("-")[-1] if subdivision else ""
            
            industry = record.get("industry_classification", {})
            scian_code = industry.get("scian", {}).get("code", "")
            nace_code = industry.get("nace", {}).get("code", "")
            
            provenance = record.get("data_provenance", {})
            source_system = provenance.get("source_system", "MANUAL_ENTRY")
            
            stats = record.get("incident_statistics", {})
            total_incidents = stats.get("total_incidents", 0)
            
            metadata = record.get("metadata", {})
            tags = metadata.get("tags", [])
            
            with self.driver.session(database=self.database) as session:
                # Créer ou mettre à jour le nœud Risque
                result = session.run("""
                    MERGE (r:Risque {id: $risk_id})
                    ON CREATE SET
                        r.code = $risk_code,
                        r.titre_fr = $title_fr,
                        r.titre_en = $title_en,
                        r.domain_code = $domain_code,
                        r.source_system = $source_system,
                        r.total_incidents = $total_incidents,
                        r.scian = $scian_code,
                        r.nace = $nace_code,
                        r.tags = $tags,
                        r.created_at = datetime(),
                        r.statut = 'ACTIVE'
                    ON MATCH SET
                        r.titre_fr = $title_fr,
                        r.titre_en = $title_en,
                        r.total_incidents = $total_incidents,
                        r.updated_at = datetime()
                    RETURN r.id as id, r.code as code
                """, 
                    risk_id=risk_id,
                    risk_code=risk_code,
                    title_fr=title_fr,
                    title_en=title_en,
                    domain_code=domain_code,
                    source_system=source_system,
                    total_incidents=total_incidents,
                    scian_code=scian_code,
                    nace_code=nace_code,
                    tags=tags
                )
                
                rec = result.single()
                if rec:
                    self.stats["nodes_created"] += 1
                
                # Créer relation vers DomaineRisque
                session.run("""
                    MATCH (r:Risque {id: $risk_id})
                    MATCH (d:DomaineRisque {code: $domain_code})
                    MERGE (r)-[:APPARTIENT_A]->(d)
                """, risk_id=risk_id, domain_code=domain_code)
                self.stats["relationships_created"] += 1
                
                # Créer relation vers Juridiction principale
                session.run("""
                    MATCH (r:Risque {id: $risk_id})
                    MATCH (j:Juridiction {code: $jurisdiction_code})
                    MERGE (r)-[:LOCALISE_DANS]->(j)
                """, risk_id=risk_id, jurisdiction_code=primary_country)
                self.stats["relationships_created"] += 1
                
                # Si subdivision (ex: Québec), créer aussi la relation
                if subdivision_code:
                    session.run("""
                        MATCH (r:Risque {id: $risk_id})
                        MATCH (j:Juridiction {code: $subdivision_code})
                        MERGE (r)-[:LOCALISE_DANS]->(j)
                    """, risk_id=risk_id, subdivision_code=subdivision_code)
                    self.stats["relationships_created"] += 1
                
                # Créer/lier le secteur si SCIAN présent
                if scian_code:
                    session.run("""
                        MERGE (s:Secteur {code: $scian_code})
                        ON CREATE SET s.type = 'SCIAN'
                        WITH s
                        MATCH (r:Risque {id: $risk_id})
                        MERGE (r)-[:DANS_SECTEUR]->(s)
                    """, risk_id=risk_id, scian_code=scian_code)
                    self.stats["relationships_created"] += 1
            
            return True
            
        except Exception as e:
            logger.error(f"Erreur chargement Neo4j: {e}")
            self.stats["errors"] += 1
            return False
    
    def load_unified_records(self, records: List[Dict], batch_size: int = 100) -> int:
        """
        Charge une liste d'enregistrements unifiés.
        
        Args:
            records: Liste d'enregistrements au format unifié
            batch_size: Taille des lots pour le commit
            
        Returns:
            Nombre d'enregistrements chargés avec succès
        """
        success_count = 0
        total = len(records)
        
        for i, record in enumerate(records):
            if self.load_unified_record(record):
                success_count += 1
            
            if (i + 1) % batch_size == 0:
                logger.info(f"Progression: {i+1}/{total} ({success_count} réussis)")
        
        logger.info(f"Chargement terminé: {success_count}/{total} enregistrements")
        return success_count
    
    # -------------------------------------------------------------------------
    # Liaison aux normes applicables
    # -------------------------------------------------------------------------
    
    def link_risk_to_standards(self, risk_id: str, standard_codes: List[str]) -> int:
        """
        Lie un risque aux normes applicables.
        
        Args:
            risk_id: ID du risque
            standard_codes: Liste des codes de normes (ex: ['CSA-Z462', 'NFPA-70E'])
            
        Returns:
            Nombre de relations créées
        """
        created = 0
        with self.driver.session(database=self.database) as session:
            for code in standard_codes:
                result = session.run("""
                    MATCH (r:Risque {id: $risk_id})
                    MATCH (n:Norme) WHERE n.code CONTAINS $code OR n.id CONTAINS $code
                    MERGE (r)-[rel:REGLEMENTE_PAR]->(n)
                    RETURN count(rel) as count
                """, risk_id=risk_id, code=code)
                
                record = result.single()
                if record and record["count"] > 0:
                    created += 1
                    self.stats["relationships_created"] += 1
        
        return created
    
    def auto_link_standards_by_domain(self) -> int:
        """
        Lie automatiquement les risques aux normes basé sur leur domaine.
        
        Returns:
            Nombre de relations créées
        """
        with self.driver.session(database=self.database) as session:
            result = session.run("""
                MATCH (r:Risque)-[:APPARTIENT_A]->(d:DomaineRisque)
                MATCH (n:Norme)-[:COUVRE_DOMAINE]->(d)
                WHERE NOT (r)-[:REGLEMENTE_PAR]->(n)
                CREATE (r)-[:REGLEMENTE_PAR {auto_linked: true}]->(n)
                RETURN count(*) as created
            """)
            
            record = result.single()
            created = record["created"] if record else 0
            self.stats["relationships_created"] += created
            logger.info(f"Auto-liaison normes: {created} relations créées")
            return created
    
    # -------------------------------------------------------------------------
    # Requêtes utilitaires
    # -------------------------------------------------------------------------
    
    def get_risks_by_domain(self, domain_code: str) -> List[Dict]:
        """Récupère tous les risques d'un domaine"""
        with self.driver.session(database=self.database) as session:
            result = session.run("""
                MATCH (r:Risque)-[:APPARTIENT_A]->(d:DomaineRisque {code: $domain_code})
                RETURN r.id as id, r.code as code, r.titre_fr as titre, 
                       r.total_incidents as incidents, r.source_system as source
                ORDER BY r.total_incidents DESC
                LIMIT 100
            """, domain_code=domain_code)
            
            return [dict(record) for record in result]
    
    def get_standards_for_risk(self, risk_id: str) -> List[Dict]:
        """Récupère les normes applicables à un risque"""
        with self.driver.session(database=self.database) as session:
            result = session.run("""
                MATCH (r:Risque {id: $risk_id})-[:REGLEMENTE_PAR]->(n:Norme)
                OPTIONAL MATCH (n)-[:APPLICABLE_DANS]->(j:Juridiction)
                RETURN n.code as code, n.titre_fr as titre, n.estObligatoire as obligatoire,
                       collect(DISTINCT j.code) as juridictions
            """, risk_id=risk_id)
            
            return [dict(record) for record in result]
    
    def get_concordance_path(self, norm1_code: str, norm2_code: str) -> List[Dict]:
        """Trouve le chemin de concordance entre deux normes"""
        with self.driver.session(database=self.database) as session:
            result = session.run("""
                MATCH path = shortestPath(
                    (n1:Norme)-[*..5]-(n2:Norme)
                )
                WHERE n1.code CONTAINS $code1 AND n2.code CONTAINS $code2
                RETURN [node in nodes(path) | node.code] as path_codes,
                       length(path) as path_length
            """, code1=norm1_code, code2=norm2_code)
            
            return [dict(record) for record in result]
    
    def get_recommendations_by_sector_jurisdiction(
        self, 
        scian_code: str, 
        jurisdiction_code: str
    ) -> List[Dict]:
        """
        Recommandations normatives basées sur secteur et juridiction.
        """
        with self.driver.session(database=self.database) as session:
            result = session.run("""
                MATCH (s:Secteur {code: $scian})-[:EXPOSE_A]->(d:DomaineRisque)
                MATCH (n:Norme)-[:COUVRE_DOMAINE]->(d)
                MATCH (n)-[:APPLICABLE_DANS]->(j:Juridiction)
                WHERE j.code = $jurisdiction OR 
                      (j.code = 'INT') OR
                      ($jurisdiction IN ['QC', 'ON'] AND j.code = 'CA')
                RETURN DISTINCT 
                    n.code as norme_code,
                    n.titre_fr as norme_titre,
                    n.estObligatoire as obligatoire,
                    d.code as domaine_code,
                    d.nom_fr as domaine_nom,
                    j.code as juridiction
                ORDER BY n.estObligatoire DESC, d.niveau_danger DESC
            """, scian=scian_code, jurisdiction=jurisdiction_code)
            
            return [dict(record) for record in result]
    
    # -------------------------------------------------------------------------
    # Statistiques et monitoring
    # -------------------------------------------------------------------------
    
    def get_graph_stats(self) -> Dict:
        """Récupère les statistiques du graphe"""
        with self.driver.session(database=self.database) as session:
            result = session.run("""
                MATCH (r:Risque) WITH count(r) as risques
                MATCH (n:Norme) WITH risques, count(n) as normes
                MATCH (d:DomaineRisque) WITH risques, normes, count(d) as domaines
                MATCH (j:Juridiction) WITH risques, normes, domaines, count(j) as juridictions
                MATCH (s:Secteur) WITH risques, normes, domaines, juridictions, count(s) as secteurs
                MATCH ()-[rel]->() WITH risques, normes, domaines, juridictions, secteurs, count(rel) as relations
                RETURN risques, normes, domaines, juridictions, secteurs, relations
            """)
            
            record = result.single()
            return dict(record) if record else {}
    
    def get_stats(self) -> Dict:
        """Retourne les statistiques de chargement"""
        return self.stats.copy()
    
    def print_stats(self) -> None:
        """Affiche les statistiques"""
        print("\n" + "="*60)
        print("📊 STATISTIQUES CHARGEMENT NEO4J")
        print("="*60)
        print(f"  Nœuds créés:       {self.stats['nodes_created']}")
        print(f"  Nœuds mis à jour:  {self.stats['nodes_updated']}")
        print(f"  Relations créées:  {self.stats['relationships_created']}")
        print(f"  Erreurs:           {self.stats['errors']}")
        
        graph_stats = self.get_graph_stats()
        if graph_stats:
            print("\n  État du graphe:")
            print(f"    - Risques:      {graph_stats.get('risques', 0)}")
            print(f"    - Normes:       {graph_stats.get('normes', 0)}")
            print(f"    - Domaines:     {graph_stats.get('domaines', 0)}")
            print(f"    - Juridictions: {graph_stats.get('juridictions', 0)}")
            print(f"    - Secteurs:     {graph_stats.get('secteurs', 0)}")
            print(f"    - Relations:    {graph_stats.get('relations', 0)}")
        
        print("="*60 + "\n")


# =============================================================================
# CLASSE MOCK POUR TESTS SANS NEO4J
# =============================================================================

class SafetyGraphLoaderMock:
    """
    Version mock du loader pour tests sans Neo4j installé.
    Stocke les données en mémoire.
    """
    
    def __init__(self, *args, **kwargs):
        self.nodes = []
        self.relationships = []
        self.stats = {
            "nodes_created": 0,
            "nodes_updated": 0,
            "relationships_created": 0,
            "errors": 0
        }
        logger.info("SafetyGraphLoaderMock initialisé (mode simulation)")
    
    def close(self):
        pass
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        pass
    
    def verify_schema(self) -> bool:
        return True
    
    def initialize_base_schema(self) -> None:
        logger.info("[MOCK] Schéma initialisé")
    
    def load_unified_record(self, record: Dict) -> bool:
        self.nodes.append(record)
        self.stats["nodes_created"] += 1
        return True
    
    def load_unified_records(self, records: List[Dict], batch_size: int = 100) -> int:
        for r in records:
            self.load_unified_record(r)
        return len(records)
    
    def get_stats(self) -> Dict:
        return self.stats
    
    def print_stats(self) -> None:
        print(f"[MOCK] Stats: {self.stats}")
        print(f"[MOCK] Nodes en mémoire: {len(self.nodes)}")


# =============================================================================
# FACTORY FUNCTION
# =============================================================================

def get_loader(uri: str = None, user: str = None, password: str = None, mock: bool = False):
    """
    Factory pour obtenir le loader approprié.
    
    Args:
        uri, user, password: Paramètres de connexion Neo4j
        mock: Si True, retourne le mock même si Neo4j est disponible
        
    Returns:
        SafetyGraphLoader ou SafetyGraphLoaderMock
    """
    if mock or not NEO4J_AVAILABLE:
        return SafetyGraphLoaderMock()
    
    if not all([uri, user, password]):
        logger.warning("Paramètres Neo4j manquants, utilisation du mock")
        return SafetyGraphLoaderMock()
    
    return SafetyGraphLoader(uri, user, password)


# =============================================================================
# POINT D'ENTRÉE CLI
# =============================================================================

if __name__ == "__main__":
    print("="*60)
    print("🕸️ NEO4J SAFETY GRAPH LOADER - Test")
    print("="*60)
    
    # Utiliser le mock pour le test
    loader = get_loader(mock=True)
    
    # Données de test
    test_records = [
        {
            "risk_id": "test-001",
            "risk_code": "ELEC-US-00000001",
            "title": {"fr": "Électrocution", "en": "Electrocution"},
            "hazard_classification": {
                "unified": {"domain_code": "ELEC"}
            },
            "jurisdiction": {"primary_country": "US"},
            "industry_classification": {"scian": {"code": "23821"}},
            "incident_statistics": {"total_incidents": 150},
            "data_provenance": {"source_system": "OSHA_INSPECTION"},
            "metadata": {"tags": ["osha", "electrical"]}
        },
        {
            "risk_id": "test-002",
            "risk_code": "CHUTE-QC-00000001",
            "title": {"fr": "Chute de hauteur", "en": "Fall from height"},
            "hazard_classification": {
                "unified": {"domain_code": "CHUTE"}
            },
            "jurisdiction": {
                "primary_country": "CA",
                "subdivision": {"code": "CA-QC"}
            },
            "industry_classification": {"scian": {"code": "23"}},
            "incident_statistics": {"total_incidents": 245},
            "data_provenance": {"source_system": "CNESST"},
            "metadata": {"tags": ["cnesst", "falls"]}
        }
    ]
    
    # Charger les données
    loaded = loader.load_unified_records(test_records)
    print(f"\n✅ {loaded} enregistrements chargés")
    
    loader.print_stats()
    
    print("\n💡 Pour utiliser avec Neo4j réel:")
    print('   loader = SafetyGraphLoader("bolt://localhost:7687", "neo4j", "password")')
    print("   loader.load_unified_records(harmonized_data)")
