"""
================================================================================
HSE DATA HARMONIZER - Module d'Intégration Architecture ↔ Ingestion
================================================================================
Ce module fait le pont entre:
- hse_ingestion_package (extraction des données brutes)
- Architecture SafetyGraph (schéma, concordances, Knowledge Graph)

Il permet de:
1. Transformer les données sources vers le format unifié SafeTwin
2. Valider les enregistrements contre le JSON Schema
3. Enrichir avec les codes de concordance multi-juridictionnels
4. Préparer les données pour Neo4j Knowledge Graph

Usage:
    from hse_harmonizer import HSEHarmonizer
    
    harmonizer = HSEHarmonizer()
    unified_record = harmonizer.transform_osha_record(osha_data)
    harmonizer.validate_record(unified_record)
    
© 2026 AgenticX5 — GenAISafety / Preventera
================================================================================
"""

import json
import uuid
import hashlib
from datetime import datetime, date
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, field, asdict
from enum import Enum
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("HSEHarmonizer")


# =============================================================================
# SECTION 1: ÉNUMÉRATIONS ET CONSTANTES
# =============================================================================

class SourceSystem(Enum):
    """Systèmes sources supportés"""
    OSHA_INSPECTION = "OSHA_INSPECTION"
    OSHA_SEVERE_INJURY = "OSHA_SEVERE_INJURY"
    BLS_CFOI = "BLS_CFOI"
    BLS_SOII = "BLS_SOII"
    EUROSTAT_ESAW = "EUROSTAT_ESAW"
    ILOSTAT = "ILOSTAT"
    CNESST = "CNESST"
    DARES = "DARES"
    CARSAT = "CARSAT"
    KAGGLE = "KAGGLE"
    MANUAL_ENTRY = "MANUAL_ENTRY"
    API_IMPORT = "API_IMPORT"


class RiskLevel(Enum):
    """Niveaux de risque"""
    NEGLIGIBLE = "NEGLIGIBLE"
    LOW = "LOW"
    MODERATE = "MODERATE"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class UnifiedDomain(Enum):
    """Domaines de risque unifiés SafeTwinX5"""
    ELEC = ("ELEC", "Électricité", "Electrical")
    CHUTE = ("CHUTE", "Chutes", "Falls")
    INCENDIE = ("INCENDIE", "Incendie/Explosion", "Fire/Explosion")
    MACHINE = ("MACHINE", "Machines", "Machinery")
    ERGO = ("ERGO", "Ergonomie", "Ergonomics")
    CHIMIQUE = ("CHIMIQUE", "Chimique", "Chemical")
    GAZ = ("GAZ", "Gaz", "Gas")
    CONFINE = ("CONFINE", "Espaces clos", "Confined Spaces")
    THERMIQUE = ("THERMIQUE", "Thermique", "Thermal")
    RPS = ("RPS", "Psychosociaux", "Psychosocial")
    BIOLOGIQUE = ("BIOLOGIQUE", "Biologique", "Biological")
    BRUIT = ("BRUIT", "Bruit", "Noise")
    VIBRATION = ("VIBRATION", "Vibrations", "Vibration")
    RAYONNEMENT = ("RAYONNEMENT", "Rayonnements", "Radiation")
    VEHICULE = ("VEHICULE", "Véhicules", "Vehicles")
    AUTRE = ("AUTRE", "Autre", "Other")
    
    def __init__(self, code, label_fr, label_en):
        self.code = code
        self.label_fr = label_fr
        self.label_en = label_en


# =============================================================================
# SECTION 2: TABLES DE CONCORDANCE (intégrées)
# =============================================================================

# Mapping Genre d'Accident: OSHA Event Code → Unified
OSHA_EVENT_TO_UNIFIED = {
    "42": {"code": "CHUTE-01", "domain": "CHUTE", "label_fr": "Chute de hauteur", "label_en": "Fall from height"},
    "421": {"code": "CHUTE-01", "domain": "CHUTE", "label_fr": "Chute de hauteur", "label_en": "Fall from height"},
    "422": {"code": "CHUTE-03", "domain": "CHUTE", "label_fr": "Chute dans escaliers", "label_en": "Fall on stairs"},
    "43": {"code": "CHUTE-02", "domain": "CHUTE", "label_fr": "Chute de plain-pied", "label_en": "Fall on same level"},
    "51": {"code": "ELEC-01", "domain": "ELEC", "label_fr": "Contact électrique", "label_en": "Electrical contact"},
    "511": {"code": "ELEC-01", "domain": "ELEC", "label_fr": "Contact électrique direct", "label_en": "Direct electrical contact"},
    "512": {"code": "ELEC-02", "domain": "ELEC", "label_fr": "Arc électrique", "label_en": "Arc flash"},
    "21": {"code": "MACH-01", "domain": "MACHINE", "label_fr": "Frappé par objet", "label_en": "Struck by object"},
    "22": {"code": "MACH-02", "domain": "MACHINE", "label_fr": "Heurté contre objet", "label_en": "Struck against object"},
    "23": {"code": "MACH-03", "domain": "MACHINE", "label_fr": "Coincement", "label_en": "Caught in machinery"},
    "232": {"code": "MACH-04", "domain": "MACHINE", "label_fr": "Écrasement", "label_en": "Crushing"},
    "71": {"code": "ERGO-01", "domain": "ERGO", "label_fr": "Effort excessif", "label_en": "Overexertion"},
    "72": {"code": "ERGO-02", "domain": "ERGO", "label_fr": "Mouvement répétitif", "label_en": "Repetitive motion"},
    "731": {"code": "ERGO-03", "domain": "ERGO", "label_fr": "Posture contraignante", "label_en": "Awkward posture"},
    "32": {"code": "FEU-01", "domain": "INCENDIE", "label_fr": "Incendie", "label_en": "Fire"},
    "321": {"code": "FEU-02", "domain": "INCENDIE", "label_fr": "Explosion", "label_en": "Explosion"},
    "53": {"code": "CHIM-01", "domain": "CHIMIQUE", "label_fr": "Exposition chimique", "label_en": "Chemical exposure"},
    "534": {"code": "CHIM-02", "domain": "CHIMIQUE", "label_fr": "Inhalation toxique", "label_en": "Toxic inhalation"},
    "31": {"code": "THERM-01", "domain": "THERMIQUE", "label_fr": "Contact chaud", "label_en": "Contact with hot object"},
    "55": {"code": "THERM-02", "domain": "THERMIQUE", "label_fr": "Stress thermique", "label_en": "Heat stress"},
    "26": {"code": "VEH-01", "domain": "VEHICULE", "label_fr": "Accident véhicule", "label_en": "Vehicle accident"},
    "542": {"code": "CONF-01", "domain": "CONFINE", "label_fr": "Asphyxie espace clos", "label_en": "Confined space asphyxiation"},
    "12": {"code": "RPS-01", "domain": "RPS", "label_fr": "Violence au travail", "label_en": "Workplace violence"},
}

# Mapping ESAW Deviation → Unified
ESAW_DEVIATION_TO_UNIFIED = {
    "51": {"code": "CHUTE-01", "domain": "CHUTE", "label_fr": "Chute de hauteur", "label_en": "Fall from height"},
    "52": {"code": "CHUTE-02", "domain": "CHUTE", "label_fr": "Chute de plain-pied", "label_en": "Fall on same level"},
    "61": {"code": "ELEC-01", "domain": "ELEC", "label_fr": "Contact électrique direct", "label_en": "Direct electrical contact"},
    "62": {"code": "ELEC-02", "domain": "ELEC", "label_fr": "Arc électrique", "label_en": "Arc flash"},
    "43": {"code": "MACH-01", "domain": "MACHINE", "label_fr": "Heurt par objet", "label_en": "Struck by object"},
    "44": {"code": "MACH-02", "domain": "MACHINE", "label_fr": "Heurt contre objet", "label_en": "Struck against object"},
    "41": {"code": "MACH-03", "domain": "MACHINE", "label_fr": "Happement machine", "label_en": "Caught in machinery"},
    "42": {"code": "MACH-04", "domain": "MACHINE", "label_fr": "Écrasement", "label_en": "Crushing"},
    "71": {"code": "ERGO-01", "domain": "ERGO", "label_fr": "Effort excessif", "label_en": "Overexertion"},
    "72": {"code": "ERGO-02", "domain": "ERGO", "label_fr": "Mouvement répétitif", "label_en": "Repetitive motion"},
    "73": {"code": "ERGO-03", "domain": "ERGO", "label_fr": "Posture contraignante", "label_en": "Awkward posture"},
    "21": {"code": "FEU-01", "domain": "INCENDIE", "label_fr": "Incendie", "label_en": "Fire"},
    "22": {"code": "FEU-02", "domain": "INCENDIE", "label_fr": "Explosion", "label_en": "Explosion"},
    "15": {"code": "CHIM-01", "domain": "CHIMIQUE", "label_fr": "Exposition substance", "label_en": "Substance exposure"},
    "63": {"code": "THERM-01", "domain": "THERMIQUE", "label_fr": "Contact chaud/froid", "label_en": "Contact with hot/cold"},
    "81": {"code": "THERM-02", "domain": "THERMIQUE", "label_fr": "Stress thermique", "label_en": "Thermal stress"},
    "11": {"code": "VEH-01", "domain": "VEHICULE", "label_fr": "Accident véhicule", "label_en": "Vehicle accident"},
    "82": {"code": "RPS-01", "domain": "RPS", "label_fr": "Violence", "label_en": "Violence"},
}

# Mapping CNESST Genre → Unified
CNESST_GENRE_TO_UNIFIED = {
    "31": {"code": "CHUTE-01", "domain": "CHUTE", "label_fr": "Chute niveau inférieur", "label_en": "Fall to lower level"},
    "32": {"code": "CHUTE-02", "domain": "CHUTE", "label_fr": "Chute même niveau", "label_en": "Fall on same level"},
    "51": {"code": "ELEC-01", "domain": "ELEC", "label_fr": "Contact électricité", "label_en": "Electrical contact"},
    "52": {"code": "ELEC-02", "domain": "ELEC", "label_fr": "Arc électrique", "label_en": "Arc flash"},
    "21": {"code": "MACH-01", "domain": "MACHINE", "label_fr": "Frappé par objet", "label_en": "Struck by object"},
    "22": {"code": "MACH-02", "domain": "MACHINE", "label_fr": "Heurt contre objet", "label_en": "Struck against object"},
    "23": {"code": "MACH-03", "domain": "MACHINE", "label_fr": "Happement", "label_en": "Caught in"},
    "24": {"code": "MACH-04", "domain": "MACHINE", "label_fr": "Écrasement", "label_en": "Crushing"},
    "11": {"code": "ERGO-01", "domain": "ERGO", "label_fr": "Effort en soulevant", "label_en": "Overexertion lifting"},
    "12": {"code": "ERGO-02", "domain": "ERGO", "label_fr": "Mouvement répétitif", "label_en": "Repetitive motion"},
    "13": {"code": "ERGO-03", "domain": "ERGO", "label_fr": "Posture contraignante", "label_en": "Awkward posture"},
    "61": {"code": "FEU-01", "domain": "INCENDIE", "label_fr": "Incendie", "label_en": "Fire"},
    "62": {"code": "FEU-02", "domain": "INCENDIE", "label_fr": "Explosion", "label_en": "Explosion"},
    "41": {"code": "CHIM-01", "domain": "CHIMIQUE", "label_fr": "Exposition nocive", "label_en": "Harmful exposure"},
    "42": {"code": "CHIM-02", "domain": "CHIMIQUE", "label_fr": "Inhalation", "label_en": "Inhalation"},
    "53": {"code": "THERM-01", "domain": "THERMIQUE", "label_fr": "Contact chaud", "label_en": "Contact with hot"},
    "44": {"code": "THERM-02", "domain": "THERMIQUE", "label_fr": "Chaleur/froid", "label_en": "Heat/cold"},
    "71": {"code": "VEH-01", "domain": "VEHICULE", "label_fr": "Accident véhicule", "label_en": "Vehicle accident"},
    "43": {"code": "CONF-01", "domain": "CONFINE", "label_fr": "Asphyxie", "label_en": "Asphyxiation"},
    "91": {"code": "RPS-01", "domain": "RPS", "label_fr": "Violence/agression", "label_en": "Violence/aggression"},
}

# Mapping Nature de lésion: OSHA → Unified
OSHA_NATURE_TO_UNIFIED = {
    "121": {"code": "NAT-01", "label_fr": "Fracture", "label_en": "Fracture"},
    "122": {"code": "NAT-05", "label_fr": "Amputation", "label_en": "Amputation"},
    "211": {"code": "NAT-02", "label_fr": "Entorse/Foulure", "label_en": "Sprain/Strain"},
    "212": {"code": "NAT-10", "label_fr": "TMS", "label_en": "MSD"},
    "131": {"code": "NAT-03", "label_fr": "Contusion", "label_en": "Contusion"},
    "111": {"code": "NAT-04", "label_fr": "Coupure", "label_en": "Cut/Laceration"},
    "321": {"code": "NAT-06", "label_fr": "Brûlure thermique", "label_en": "Thermal burn"},
    "322": {"code": "NAT-07", "label_fr": "Brûlure chimique", "label_en": "Chemical burn"},
    "323": {"code": "NAT-08", "label_fr": "Brûlure électrique", "label_en": "Electrical burn"},
    "331": {"code": "NAT-09", "label_fr": "Électrocution", "label_en": "Electrocution"},
    "341": {"code": "NAT-11", "label_fr": "Intoxication", "label_en": "Poisoning"},
    "342": {"code": "NAT-12", "label_fr": "Asphyxie", "label_en": "Asphyxiation"},
    "561": {"code": "NAT-13", "label_fr": "Stress post-traumatique", "label_en": "PTSD"},
    "999": {"code": "NAT-99", "label_fr": "Décès", "label_en": "Fatality"},
}

# Mapping Partie du corps: OSHA → Unified
OSHA_BODYPART_TO_UNIFIED = {
    "1": {"code": "BP-01", "label_fr": "Tête", "label_en": "Head"},
    "12": {"code": "BP-02", "label_fr": "Œil", "label_en": "Eye"},
    "2": {"code": "BP-03", "label_fr": "Cou", "label_en": "Neck"},
    "32": {"code": "BP-04", "label_fr": "Dos", "label_en": "Back"},
    "41": {"code": "BP-05", "label_fr": "Épaule", "label_en": "Shoulder"},
    "42": {"code": "BP-06", "label_fr": "Bras", "label_en": "Arm"},
    "44": {"code": "BP-07", "label_fr": "Main", "label_en": "Hand"},
    "45": {"code": "BP-08", "label_fr": "Doigt", "label_en": "Finger"},
    "31": {"code": "BP-09", "label_fr": "Poitrine", "label_en": "Chest"},
    "33": {"code": "BP-10", "label_fr": "Abdomen", "label_en": "Abdomen"},
    "51": {"code": "BP-11", "label_fr": "Bassin/Hanche", "label_en": "Pelvis/Hip"},
    "52": {"code": "BP-12", "label_fr": "Jambe", "label_en": "Leg"},
    "53": {"code": "BP-13", "label_fr": "Genou", "label_en": "Knee"},
    "54": {"code": "BP-14", "label_fr": "Pied", "label_en": "Foot"},
    "55": {"code": "BP-15", "label_fr": "Orteil", "label_en": "Toe"},
    "8": {"code": "BP-90", "label_fr": "Corps entier", "label_en": "Whole body"},
}

# Mapping SCIAN → NACE
SCIAN_TO_NACE = {
    "23": {"nace": "F", "isic": "F", "label": "Construction"},
    "236": {"nace": "F41", "isic": "41", "label": "Construction de bâtiments"},
    "238": {"nace": "F43", "isic": "43", "label": "Travaux spécialisés"},
    "23821": {"nace": "F4321", "isic": "4321", "label": "Travaux d'électricité"},
    "2211": {"nace": "D351", "isic": "351", "label": "Production d'électricité"},
    "221122": {"nace": "D3513", "isic": "3513", "label": "Distribution d'électricité"},
    "31-33": {"nace": "C", "isic": "C", "label": "Fabrication"},
    "332": {"nace": "C25", "isic": "25", "label": "Fabrication métallique"},
    "21": {"nace": "B", "isic": "B", "label": "Mines"},
    "11": {"nace": "A01", "isic": "01", "label": "Agriculture"},
    "484": {"nace": "H49", "isic": "49", "label": "Transport routier"},
    "62": {"nace": "Q86", "isic": "86", "label": "Santé"},
}


# =============================================================================
# SECTION 3: CLASSE PRINCIPALE HSEHarmonizer
# =============================================================================

class HSEHarmonizer:
    """
    Classe principale pour harmoniser les données HSE multi-sources
    vers le format unifié SafeTwinX5/AgenticX5.
    """
    
    def __init__(self, schema_path: str = None):
        """
        Initialise l'harmoniseur.
        
        Args:
            schema_path: Chemin vers le fichier JSON Schema (optionnel)
        """
        self.schema = None
        if schema_path:
            self.load_schema(schema_path)
        
        self.stats = {
            "records_processed": 0,
            "records_harmonized": 0,
            "records_failed": 0,
            "by_source": {},
            "by_domain": {}
        }
        
        logger.info("HSEHarmonizer initialisé")
    
    def load_schema(self, schema_path: str) -> None:
        """Charge le JSON Schema pour validation"""
        try:
            with open(schema_path, 'r', encoding='utf-8') as f:
                self.schema = json.load(f)
            logger.info(f"Schema chargé: {schema_path}")
        except Exception as e:
            logger.error(f"Erreur chargement schema: {e}")
    
    # -------------------------------------------------------------------------
    # Génération d'identifiants
    # -------------------------------------------------------------------------
    
    def generate_risk_id(self) -> str:
        """Génère un UUID v4 pour un nouveau risque"""
        return str(uuid.uuid4())
    
    def generate_risk_code(self, domain: str, jurisdiction: str, sequence: int = None) -> str:
        """
        Génère un code mnémonique pour un risque.
        Format: DOMAIN-JURISDICTION-SEQUENCE (ex: ELEC-QC-00001234)
        """
        if sequence is None:
            # Générer un hash unique basé sur timestamp
            hash_input = f"{domain}{jurisdiction}{datetime.now().isoformat()}"
            sequence = int(hashlib.md5(hash_input.encode()).hexdigest()[:8], 16) % 100000000
        
        return f"{domain[:4].upper()}-{jurisdiction.upper()}-{sequence:08d}"
    
    # -------------------------------------------------------------------------
    # Fonctions de conversion: OSHA
    # -------------------------------------------------------------------------
    
    def convert_osha_event(self, osha_code: str) -> Dict:
        """Convertit un code OSHA Event Type vers le format unifié"""
        # Essayer code exact, puis code parent (2 chiffres)
        unified = OSHA_EVENT_TO_UNIFIED.get(str(osha_code))
        if not unified and len(str(osha_code)) > 2:
            unified = OSHA_EVENT_TO_UNIFIED.get(str(osha_code)[:2])
        
        if unified:
            return unified.copy()
        
        return {
            "code": "AUTRE-99",
            "domain": "AUTRE",
            "label_fr": "Non classé",
            "label_en": "Unclassified"
        }
    
    def convert_osha_nature(self, osha_code: str) -> Dict:
        """Convertit un code OSHA Nature vers le format unifié"""
        unified = OSHA_NATURE_TO_UNIFIED.get(str(osha_code))
        if not unified and len(str(osha_code)) > 2:
            unified = OSHA_NATURE_TO_UNIFIED.get(str(osha_code)[:2] + "1")
        
        if unified:
            return unified.copy()
        
        return {"code": "NAT-99", "label_fr": "Non spécifié", "label_en": "Unspecified"}
    
    def convert_osha_bodypart(self, osha_code: str) -> Dict:
        """Convertit un code OSHA Body Part vers le format unifié"""
        unified = OSHA_BODYPART_TO_UNIFIED.get(str(osha_code))
        if not unified and len(str(osha_code)) > 1:
            unified = OSHA_BODYPART_TO_UNIFIED.get(str(osha_code)[0])
        
        if unified:
            return unified.copy()
        
        return {"code": "BP-99", "label_fr": "Non spécifié", "label_en": "Unspecified"}
    
    def transform_osha_record(self, record: Dict) -> Dict:
        """
        Transforme un enregistrement OSHA brut vers le format unifié.
        
        Args:
            record: Dictionnaire avec les champs OSHA bruts
            
        Returns:
            Dictionnaire au format unifié SafeTwin
        """
        self.stats["records_processed"] += 1
        self.stats["by_source"]["OSHA"] = self.stats["by_source"].get("OSHA", 0) + 1
        
        try:
            # Extraction des codes sources
            event_code = str(record.get("event_type", record.get("EventType", "")))
            nature_code = str(record.get("nature", record.get("Nature", "")))
            bodypart_code = str(record.get("body_part", record.get("PartOfBody", "")))
            naics_code = str(record.get("naics_code", record.get("NAICS", "")))
            
            # Conversion vers codes unifiés
            event_unified = self.convert_osha_event(event_code)
            nature_unified = self.convert_osha_nature(nature_code)
            bodypart_unified = self.convert_osha_bodypart(bodypart_code)
            
            # Conversion NAICS vers NACE
            industry_mapping = SCIAN_TO_NACE.get(naics_code, {})
            
            # Construction de l'enregistrement unifié
            unified = {
                "risk_id": self.generate_risk_id(),
                "risk_code": self.generate_risk_code(event_unified["domain"], "US"),
                "version": {"major": 1, "minor": 0, "patch": 0},
                "status": "ACTIVE",
                
                "title": {
                    "en": event_unified["label_en"],
                    "fr": event_unified["label_fr"]
                },
                
                "jurisdiction": {
                    "primary_country": "US",
                    "country_name": {"en": "United States", "fr": "États-Unis"},
                    "applicable_jurisdictions": ["US"]
                },
                
                "hazard_classification": {
                    "osha_usa": {
                        "event_type_code": event_code,
                        "event_type_description": record.get("event_description", ""),
                        "nature_code": nature_code,
                        "body_part_code": bodypart_code
                    },
                    "unified": {
                        "domain_code": event_unified["domain"],
                        "domain_label": {
                            "en": event_unified["label_en"],
                            "fr": event_unified["label_fr"]
                        }
                    }
                },
                
                "industry_classification": {
                    "scian": {
                        "code": naics_code,
                        "description_en": industry_mapping.get("label", "")
                    },
                    "nace": {
                        "code": industry_mapping.get("nace", ""),
                        "description": industry_mapping.get("label", "")
                    },
                    "isic": {
                        "code": industry_mapping.get("isic", ""),
                        "revision": 4
                    }
                },
                
                "data_provenance": {
                    "source_system": "OSHA_INSPECTION" if "inspection" in str(record.get("source", "")).lower() else "OSHA_SEVERE_INJURY",
                    "source_record_id": str(record.get("id", record.get("ActivityNr", ""))),
                    "extraction_date": datetime.now().isoformat(),
                    "validation_status": "PENDING"
                },
                
                "metadata": {
                    "created_at": datetime.now().isoformat(),
                    "schema_version": "1.0.0",
                    "tags": ["osha", "usa", event_unified["domain"].lower()]
                },
                
                # Données originales pour traçabilité
                "_source_record": record
            }
            
            # Stats par domaine
            domain = event_unified["domain"]
            self.stats["by_domain"][domain] = self.stats["by_domain"].get(domain, 0) + 1
            self.stats["records_harmonized"] += 1
            
            return unified
            
        except Exception as e:
            logger.error(f"Erreur transformation OSHA: {e}")
            self.stats["records_failed"] += 1
            return None
    
    # -------------------------------------------------------------------------
    # Fonctions de conversion: ESAW (Eurostat)
    # -------------------------------------------------------------------------
    
    def convert_esaw_deviation(self, esaw_code: str) -> Dict:
        """Convertit un code ESAW Deviation vers le format unifié"""
        unified = ESAW_DEVIATION_TO_UNIFIED.get(str(esaw_code))
        if unified:
            return unified.copy()
        return {"code": "AUTRE-99", "domain": "AUTRE", "label_fr": "Non classé", "label_en": "Unclassified"}
    
    def transform_esaw_record(self, record: Dict) -> Dict:
        """
        Transforme un enregistrement ESAW (Eurostat) vers le format unifié.
        """
        self.stats["records_processed"] += 1
        self.stats["by_source"]["ESAW"] = self.stats["by_source"].get("ESAW", 0) + 1
        
        try:
            deviation_code = str(record.get("deviation", record.get("DEVIATION", "")))
            country_code = str(record.get("geo", record.get("GEO", "EU")))[:2].upper()
            nace_code = str(record.get("nace_r2", record.get("NACE_R2", "")))
            
            deviation_unified = self.convert_esaw_deviation(deviation_code)
            
            unified = {
                "risk_id": self.generate_risk_id(),
                "risk_code": self.generate_risk_code(deviation_unified["domain"], country_code),
                "version": {"major": 1, "minor": 0, "patch": 0},
                "status": "ACTIVE",
                
                "title": {
                    "en": deviation_unified["label_en"],
                    "fr": deviation_unified["label_fr"]
                },
                
                "jurisdiction": {
                    "primary_country": country_code,
                    "applicable_jurisdictions": [country_code, "EU"]
                },
                
                "hazard_classification": {
                    "esaw": {
                        "deviation_code": deviation_code,
                        "deviation_description": {"en": deviation_unified["label_en"], "fr": deviation_unified["label_fr"]}
                    },
                    "unified": {
                        "domain_code": deviation_unified["domain"],
                        "domain_label": {"en": deviation_unified["label_en"], "fr": deviation_unified["label_fr"]}
                    }
                },
                
                "industry_classification": {
                    "nace": {"code": nace_code}
                },
                
                "incident_statistics": {
                    "total_incidents": int(record.get("OBS_VALUE", record.get("value", 0)) or 0),
                    "reporting_period": {
                        "start_date": f"{record.get('TIME_PERIOD', record.get('year', '2023'))}-01-01",
                        "end_date": f"{record.get('TIME_PERIOD', record.get('year', '2023'))}-12-31"
                    }
                },
                
                "data_provenance": {
                    "source_system": "EUROSTAT_ESAW",
                    "extraction_date": datetime.now().isoformat(),
                    "validation_status": "PENDING"
                },
                
                "metadata": {
                    "created_at": datetime.now().isoformat(),
                    "schema_version": "1.0.0",
                    "tags": ["esaw", "eurostat", country_code.lower(), deviation_unified["domain"].lower()]
                },
                
                "_source_record": record
            }
            
            domain = deviation_unified["domain"]
            self.stats["by_domain"][domain] = self.stats["by_domain"].get(domain, 0) + 1
            self.stats["records_harmonized"] += 1
            
            return unified
            
        except Exception as e:
            logger.error(f"Erreur transformation ESAW: {e}")
            self.stats["records_failed"] += 1
            return None
    
    # -------------------------------------------------------------------------
    # Fonctions de conversion: CNESST (Québec)
    # -------------------------------------------------------------------------
    
    def convert_cnesst_genre(self, cnesst_code: str) -> Dict:
        """Convertit un code CNESST Genre vers le format unifié"""
        unified = CNESST_GENRE_TO_UNIFIED.get(str(cnesst_code))
        if unified:
            return unified.copy()
        return {"code": "AUTRE-99", "domain": "AUTRE", "label_fr": "Non classé", "label_en": "Unclassified"}
    
    def transform_cnesst_record(self, record: Dict) -> Dict:
        """
        Transforme un enregistrement CNESST vers le format unifié.
        """
        self.stats["records_processed"] += 1
        self.stats["by_source"]["CNESST"] = self.stats["by_source"].get("CNESST", 0) + 1
        
        try:
            genre_code = str(record.get("genre_accident", record.get("GenreAccident", "")))
            scian_code = str(record.get("scian", record.get("SCIAN", "")))
            
            genre_unified = self.convert_cnesst_genre(genre_code)
            industry_mapping = SCIAN_TO_NACE.get(scian_code, {})
            
            unified = {
                "risk_id": self.generate_risk_id(),
                "risk_code": self.generate_risk_code(genre_unified["domain"], "QC"),
                "version": {"major": 1, "minor": 0, "patch": 0},
                "status": "ACTIVE",
                
                "title": {
                    "fr": genre_unified["label_fr"],
                    "en": genre_unified["label_en"]
                },
                
                "jurisdiction": {
                    "primary_country": "CA",
                    "subdivision": {
                        "code": "CA-QC",
                        "name": {"fr": "Québec", "en": "Quebec"},
                        "type": "STATE_PROVINCIAL"
                    },
                    "applicable_jurisdictions": ["CA", "QC"]
                },
                
                "hazard_classification": {
                    "cnesst": {
                        "genre_accident_code": genre_code,
                        "genre_accident_description": record.get("genre_description", ""),
                        "nature_lesion_code": str(record.get("nature_lesion", "")),
                        "siege_lesion_code": str(record.get("siege_lesion", "")),
                        "agent_causal_code": str(record.get("agent_causal", ""))
                    },
                    "unified": {
                        "domain_code": genre_unified["domain"],
                        "domain_label": {"fr": genre_unified["label_fr"], "en": genre_unified["label_en"]}
                    }
                },
                
                "industry_classification": {
                    "scian": {
                        "code": scian_code,
                        "description_fr": record.get("secteur", ""),
                        "description_en": industry_mapping.get("label", "")
                    },
                    "nace": {
                        "code": industry_mapping.get("nace", "")
                    }
                },
                
                "incident_statistics": {
                    "total_incidents": int(record.get("nombre_lesions", record.get("nb_lesions", 0)) or 0),
                    "total_lost_days": int(record.get("jours_perdus", 0) or 0)
                },
                
                "data_provenance": {
                    "source_system": "CNESST",
                    "extraction_date": datetime.now().isoformat(),
                    "validation_status": "PENDING"
                },
                
                "metadata": {
                    "created_at": datetime.now().isoformat(),
                    "schema_version": "1.0.0",
                    "tags": ["cnesst", "quebec", "canada", genre_unified["domain"].lower()]
                },
                
                "_source_record": record
            }
            
            domain = genre_unified["domain"]
            self.stats["by_domain"][domain] = self.stats["by_domain"].get(domain, 0) + 1
            self.stats["records_harmonized"] += 1
            
            return unified
            
        except Exception as e:
            logger.error(f"Erreur transformation CNESST: {e}")
            self.stats["records_failed"] += 1
            return None
    
    # -------------------------------------------------------------------------
    # Transformation générique (détection automatique de source)
    # -------------------------------------------------------------------------
    
    def transform_record(self, record: Dict, source: str = None) -> Dict:
        """
        Transforme un enregistrement en détectant automatiquement la source.
        
        Args:
            record: Données brutes
            source: Source explicite (optionnel) - 'osha', 'esaw', 'cnesst'
            
        Returns:
            Enregistrement au format unifié
        """
        if source:
            source = source.lower()
        else:
            # Détection automatique basée sur les champs présents
            if "event_type" in record or "EventType" in record or "ActivityNr" in record:
                source = "osha"
            elif "deviation" in record or "DEVIATION" in record or "nace_r2" in record:
                source = "esaw"
            elif "genre_accident" in record or "GenreAccident" in record or "scian" in record:
                source = "cnesst"
            else:
                logger.warning("Source non détectée, utilisation du format générique")
                source = "generic"
        
        if source == "osha":
            return self.transform_osha_record(record)
        elif source == "esaw":
            return self.transform_esaw_record(record)
        elif source == "cnesst":
            return self.transform_cnesst_record(record)
        else:
            # Format générique minimal
            return self._transform_generic_record(record)
    
    def _transform_generic_record(self, record: Dict) -> Dict:
        """Transformation générique pour sources non standard"""
        self.stats["records_processed"] += 1
        self.stats["by_source"]["GENERIC"] = self.stats["by_source"].get("GENERIC", 0) + 1
        
        return {
            "risk_id": self.generate_risk_id(),
            "risk_code": self.generate_risk_code("AUTRE", "XX"),
            "version": {"major": 1, "minor": 0, "patch": 0},
            "status": "DRAFT",
            "title": {"en": "Unclassified Record", "fr": "Enregistrement non classé"},
            "hazard_classification": {
                "unified": {"domain_code": "AUTRE", "domain_label": {"en": "Other", "fr": "Autre"}}
            },
            "data_provenance": {
                "source_system": "MANUAL_ENTRY",
                "extraction_date": datetime.now().isoformat(),
                "validation_status": "REVIEW_REQUIRED"
            },
            "metadata": {
                "created_at": datetime.now().isoformat(),
                "schema_version": "1.0.0"
            },
            "_source_record": record
        }
    
    # -------------------------------------------------------------------------
    # Transformation par lots (batch)
    # -------------------------------------------------------------------------
    
    def transform_batch(self, records: List[Dict], source: str = None) -> List[Dict]:
        """
        Transforme un lot d'enregistrements.
        
        Args:
            records: Liste d'enregistrements bruts
            source: Source des données
            
        Returns:
            Liste d'enregistrements unifiés (sans les échecs)
        """
        results = []
        for record in records:
            unified = self.transform_record(record, source)
            if unified:
                results.append(unified)
        
        logger.info(f"Batch transformé: {len(results)}/{len(records)} réussis")
        return results
    
    # -------------------------------------------------------------------------
    # Validation
    # -------------------------------------------------------------------------
    
    def validate_record(self, record: Dict) -> tuple:
        """
        Valide un enregistrement contre le JSON Schema.
        
        Returns:
            (is_valid: bool, errors: list)
        """
        if not self.schema:
            logger.warning("Aucun schema chargé, validation ignorée")
            return True, []
        
        try:
            import jsonschema
            jsonschema.validate(instance=record, schema=self.schema)
            return True, []
        except jsonschema.ValidationError as e:
            return False, [str(e.message)]
        except Exception as e:
            return False, [str(e)]
    
    # -------------------------------------------------------------------------
    # Statistiques et reporting
    # -------------------------------------------------------------------------
    
    def get_stats(self) -> Dict:
        """Retourne les statistiques de transformation"""
        return self.stats.copy()
    
    def print_stats(self) -> None:
        """Affiche les statistiques de transformation"""
        print("\n" + "="*60)
        print("📊 STATISTIQUES DE TRANSFORMATION HSE")
        print("="*60)
        print(f"  Total traités:     {self.stats['records_processed']}")
        print(f"  ✅ Harmonisés:     {self.stats['records_harmonized']}")
        print(f"  ❌ Échecs:         {self.stats['records_failed']}")
        print()
        print("  Par source:")
        for src, count in self.stats["by_source"].items():
            print(f"    - {src}: {count}")
        print()
        print("  Par domaine de risque:")
        for domain, count in sorted(self.stats["by_domain"].items(), key=lambda x: -x[1]):
            print(f"    - {domain}: {count}")
        print("="*60 + "\n")
    
    def reset_stats(self) -> None:
        """Réinitialise les statistiques"""
        self.stats = {
            "records_processed": 0,
            "records_harmonized": 0,
            "records_failed": 0,
            "by_source": {},
            "by_domain": {}
        }


# =============================================================================
# SECTION 4: FONCTIONS UTILITAIRES STANDALONE
# =============================================================================

def convert_osha_to_unified(osha_event_code: str) -> Dict:
    """Fonction standalone pour conversion rapide OSHA → Unified"""
    return OSHA_EVENT_TO_UNIFIED.get(str(osha_event_code), {
        "code": "AUTRE-99", "domain": "AUTRE", "label_fr": "Non classé", "label_en": "Unclassified"
    })

def convert_esaw_to_unified(esaw_deviation_code: str) -> Dict:
    """Fonction standalone pour conversion rapide ESAW → Unified"""
    return ESAW_DEVIATION_TO_UNIFIED.get(str(esaw_deviation_code), {
        "code": "AUTRE-99", "domain": "AUTRE", "label_fr": "Non classé", "label_en": "Unclassified"
    })

def convert_cnesst_to_unified(cnesst_genre_code: str) -> Dict:
    """Fonction standalone pour conversion rapide CNESST → Unified"""
    return CNESST_GENRE_TO_UNIFIED.get(str(cnesst_genre_code), {
        "code": "AUTRE-99", "domain": "AUTRE", "label_fr": "Non classé", "label_en": "Unclassified"
    })

def convert_scian_to_nace(scian_code: str) -> Dict:
    """Fonction standalone pour conversion SCIAN → NACE"""
    return SCIAN_TO_NACE.get(str(scian_code), {})


# =============================================================================
# POINT D'ENTRÉE CLI
# =============================================================================

if __name__ == "__main__":
    print("="*60)
    print("🔄 HSE DATA HARMONIZER - Test Module")
    print("="*60)
    
    # Créer l'harmoniseur
    harmonizer = HSEHarmonizer()
    
    # Test OSHA
    print("\n📋 Test transformation OSHA:")
    osha_record = {
        "ActivityNr": "123456789",
        "event_type": "42",
        "nature": "121",
        "body_part": "52",
        "naics_code": "23821"
    }
    unified_osha = harmonizer.transform_osha_record(osha_record)
    print(f"  Code unifié: {unified_osha['risk_code']}")
    print(f"  Domaine: {unified_osha['hazard_classification']['unified']['domain_code']}")
    
    # Test ESAW
    print("\n📋 Test transformation ESAW:")
    esaw_record = {
        "deviation": "51",
        "geo": "FR",
        "nace_r2": "F43",
        "OBS_VALUE": 1250,
        "TIME_PERIOD": "2023"
    }
    unified_esaw = harmonizer.transform_esaw_record(esaw_record)
    print(f"  Code unifié: {unified_esaw['risk_code']}")
    print(f"  Domaine: {unified_esaw['hazard_classification']['unified']['domain_code']}")
    
    # Test CNESST
    print("\n📋 Test transformation CNESST:")
    cnesst_record = {
        "genre_accident": "31",
        "scian": "23821",
        "nombre_lesions": 45
    }
    unified_cnesst = harmonizer.transform_cnesst_record(cnesst_record)
    print(f"  Code unifié: {unified_cnesst['risk_code']}")
    print(f"  Domaine: {unified_cnesst['hazard_classification']['unified']['domain_code']}")
    
    # Afficher stats
    harmonizer.print_stats()
    
    # Test conversions standalone
    print("\n📋 Test conversions standalone:")
    print(f"  OSHA 42 → {convert_osha_to_unified('42')}")
    print(f"  ESAW 51 → {convert_esaw_to_unified('51')}")
    print(f"  CNESST 31 → {convert_cnesst_to_unified('31')}")
    print(f"  SCIAN 23821 → NACE: {convert_scian_to_nace('23821')}")
