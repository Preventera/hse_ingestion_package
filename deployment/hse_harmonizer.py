"""
HSE HARMONIZER - SafetyGraph
============================
Module d'harmonisation multi-juridictionnelle
CNESST (Québec) ↔ OSHA (USA) ↔ EU-OSHA (Europe)

Standards: Dublin Core, DDI, ISO 11179, SCIAN/NAICS/NACE
"""

import json
from dataclasses import dataclass
from typing import Dict, List, Optional
from enum import Enum

# ============================================
# MAPPINGS DE CODES INTER-JURIDICTIONNELS
# ============================================

class JurisdictionCode(Enum):
    CNESST = "cnesst"
    OSHA = "osha"
    EU_OSHA = "eu_osha"
    HSE_UK = "hse_uk"

# Mapping SCIAN (Canada) ↔ NAICS (USA) ↔ NACE (Europe)
SECTOR_MAPPING = {
    "221122": {
        "scian": "221122",
        "naics": "221122",
        "nace": "D35.13",
        "name_fr": "Distribution d'électricité",
        "name_en": "Electric power distribution",
        "name_de": "Elektrizitätsverteilung"
    },
    "23": {
        "scian": "23",
        "naics": "23",
        "nace": "F41-43",
        "name_fr": "Construction",
        "name_en": "Construction",
        "name_de": "Baugewerbe"
    },
    "31-33": {
        "scian": "31-33",
        "naics": "31-33",
        "nace": "C10-33",
        "name_fr": "Fabrication",
        "name_en": "Manufacturing",
        "name_de": "Verarbeitendes Gewerbe"
    },
    "62": {
        "scian": "62",
        "naics": "62",
        "nace": "Q86-88",
        "name_fr": "Soins de santé et assistance sociale",
        "name_en": "Health care and social assistance",
        "name_de": "Gesundheits- und Sozialwesen"
    },
    "48-49": {
        "scian": "48-49",
        "naics": "48-49",
        "nace": "H49-53",
        "name_fr": "Transport et entreposage",
        "name_en": "Transportation and warehousing",
        "name_de": "Verkehr und Lagerei"
    },
    "21": {
        "scian": "21",
        "naics": "21",
        "nace": "B05-09",
        "name_fr": "Extraction minière",
        "name_en": "Mining and quarrying",
        "name_de": "Bergbau"
    },
    "11": {
        "scian": "11",
        "naics": "11",
        "nace": "A01-03",
        "name_fr": "Agriculture, foresterie",
        "name_en": "Agriculture, forestry",
        "name_de": "Land- und Forstwirtschaft"
    },
    "44-45": {
        "scian": "44-45",
        "naics": "44-45",
        "nace": "G47",
        "name_fr": "Commerce de détail",
        "name_en": "Retail trade",
        "name_de": "Einzelhandel"
    }
}

# Mapping Types de lésions CNESST ↔ OSHA ↔ ESAW
INJURY_TYPE_MAPPING = {
    # Code CNESST -> OSHA -> ESAW
    "10": {
        "cnesst_code": "10",
        "cnesst_name": "Entorse, foulure, déchirure",
        "osha_code": "021",
        "osha_name": "Sprains, strains, tears",
        "esaw_code": "010",
        "esaw_name": "Wounds and superficial injuries"
    },
    "20": {
        "cnesst_code": "20",
        "cnesst_name": "Contusion, écrasement",
        "osha_code": "030",
        "osha_name": "Bruises, contusions",
        "esaw_code": "020",
        "esaw_name": "Bone fractures"
    },
    "30": {
        "cnesst_code": "30",
        "cnesst_name": "Fracture",
        "osha_code": "011",
        "osha_name": "Fractures",
        "esaw_code": "020",
        "esaw_name": "Bone fractures"
    },
    "40": {
        "cnesst_code": "40",
        "cnesst_name": "Brûlure (thermique, électrique, chimique)",
        "osha_code": "050",
        "osha_name": "Burns",
        "esaw_code": "050",
        "esaw_name": "Burns, scalds and frostbites"
    },
    "50": {
        "cnesst_code": "50",
        "cnesst_name": "Coupure, lacération",
        "osha_code": "040",
        "osha_name": "Cuts, lacerations",
        "esaw_code": "010",
        "esaw_name": "Wounds and superficial injuries"
    }
}

# Mapping Agents causaux / Genre d'accident
CAUSATION_MAPPING = {
    "31": {
        "cnesst_code": "31",
        "cnesst_name": "Chute au même niveau",
        "osha_code": "411",
        "osha_name": "Fall on same level",
        "esaw_code": "30",
        "esaw_name": "Fall of persons - same level"
    },
    "42": {
        "cnesst_code": "42",
        "cnesst_name": "Chute à un niveau inférieur",
        "osha_code": "410",
        "osha_name": "Fall to lower level",
        "esaw_code": "31",
        "esaw_name": "Fall of persons - from height"
    },
    "50": {
        "cnesst_code": "50",
        "cnesst_name": "Frappé par un objet",
        "osha_code": "520",
        "osha_name": "Struck by object",
        "esaw_code": "40",
        "esaw_name": "Struck by moving object"
    },
    "51": {
        "cnesst_code": "51",
        "cnesst_name": "Contact avec courant électrique",
        "osha_code": "610",
        "osha_name": "Contact with electric current",
        "esaw_code": "70",
        "esaw_name": "Contact with electrical voltage"
    },
    "52": {
        "cnesst_code": "52",
        "cnesst_name": "Coincé dans ou entre",
        "osha_code": "530",
        "osha_name": "Caught in/compressed by",
        "esaw_code": "50",
        "esaw_name": "Trapped, crushed"
    },
    "60": {
        "cnesst_code": "60",
        "cnesst_name": "Accident de transport",
        "osha_code": "200",
        "osha_name": "Transportation accidents",
        "esaw_code": "80",
        "esaw_name": "Road traffic accidents"
    },
    "72": {
        "cnesst_code": "72",
        "cnesst_name": "Effort excessif (TMS)",
        "osha_code": "710",
        "osha_name": "Overexertion",
        "esaw_code": "60",
        "esaw_name": "Physical strain"
    }
}


@dataclass
class HarmonizedIncident:
    """Incident SST harmonisé multi-juridictionnel"""
    # Identifiants
    id: str
    source_jurisdiction: JurisdictionCode
    original_id: str
    
    # Secteur (harmonisé)
    sector_scian: str
    sector_naics: str
    sector_nace: str
    sector_name_fr: str
    sector_name_en: str
    
    # Type de lésion (harmonisé)
    injury_cnesst: str
    injury_osha: str
    injury_esaw: str
    injury_name_fr: str
    injury_name_en: str
    
    # Agent causal (harmonisé)
    causation_cnesst: str
    causation_osha: str
    causation_esaw: str
    causation_name_fr: str
    causation_name_en: str
    
    # Données quantitatives
    year: int
    count: int
    days_lost: int
    severity_level: int  # 1-5
    
    # Métadonnées Dublin Core
    dc_title: str
    dc_creator: str
    dc_date: str
    dc_coverage: str
    dc_rights: str


class HSEHarmonizer:
    """
    Harmoniseur de données HSE multi-juridictionnel
    Conforme aux standards: Dublin Core, DDI, ISO 11179
    """
    
    def __init__(self):
        self.sector_map = SECTOR_MAPPING
        self.injury_map = INJURY_TYPE_MAPPING
        self.causation_map = CAUSATION_MAPPING
        
    def harmonize_sector(self, code: str, source: str = "scian") -> Dict:
        """Harmonise un code secteur vers tous les systèmes"""
        if code in self.sector_map:
            return self.sector_map[code]
        # Recherche partielle
        for key, value in self.sector_map.items():
            if code.startswith(key) or key.startswith(code):
                return value
        return {"scian": code, "naics": code, "nace": "UNKNOWN", "name_fr": "Non classé", "name_en": "Unclassified"}
    
    def harmonize_injury(self, code: str, source: str = "cnesst") -> Dict:
        """Harmonise un type de lésion vers tous les systèmes"""
        if code in self.injury_map:
            return self.injury_map[code]
        return {"cnesst_code": code, "osha_code": "UNK", "esaw_code": "UNK"}
    
    def harmonize_causation(self, code: str, source: str = "cnesst") -> Dict:
        """Harmonise un agent causal vers tous les systèmes"""
        if code in self.causation_map:
            return self.causation_map[code]
        return {"cnesst_code": code, "osha_code": "UNK", "esaw_code": "UNK"}
    
    def harmonize_cnesst_record(self, record: Dict) -> HarmonizedIncident:
        """Transforme un enregistrement CNESST en incident harmonisé"""
        sector = self.harmonize_sector(str(record.get("scian", "")))
        injury = self.harmonize_injury(str(record.get("nature_lesion", "")))
        causation = self.harmonize_causation(str(record.get("genre_accident", "")))
        
        return HarmonizedIncident(
            id=f"CNESST-{record.get('id', 'UNK')}",
            source_jurisdiction=JurisdictionCode.CNESST,
            original_id=str(record.get("id", "")),
            sector_scian=sector.get("scian", ""),
            sector_naics=sector.get("naics", ""),
            sector_nace=sector.get("nace", ""),
            sector_name_fr=sector.get("name_fr", ""),
            sector_name_en=sector.get("name_en", ""),
            injury_cnesst=injury.get("cnesst_code", ""),
            injury_osha=injury.get("osha_code", ""),
            injury_esaw=injury.get("esaw_code", ""),
            injury_name_fr=injury.get("cnesst_name", ""),
            injury_name_en=injury.get("osha_name", ""),
            causation_cnesst=causation.get("cnesst_code", ""),
            causation_osha=causation.get("osha_code", ""),
            causation_esaw=causation.get("esaw_code", ""),
            causation_name_fr=causation.get("cnesst_name", ""),
            causation_name_en=causation.get("osha_name", ""),
            year=record.get("annee", 2023),
            count=record.get("nombre_lesions", 0),
            days_lost=record.get("jours_perdus", 0),
            severity_level=min(5, max(1, record.get("nombre_lesions", 0) // 1000 + 1)),
            dc_title=f"Incident SST - {sector.get('name_fr', '')} - {record.get('annee', '')}",
            dc_creator="CNESST",
            dc_date=str(record.get("annee", "")),
            dc_coverage="Québec, Canada",
            dc_rights="Open Data License - Gouvernement du Québec"
        )
    
    def generate_dublin_core_metadata(self, incident: HarmonizedIncident) -> Dict:
        """Génère les métadonnées Dublin Core pour un incident"""
        return {
            "dc:title": incident.dc_title,
            "dc:creator": incident.dc_creator,
            "dc:subject": [incident.sector_name_fr, incident.injury_name_fr],
            "dc:description": f"Incident SST dans le secteur {incident.sector_name_fr}",
            "dc:date": incident.dc_date,
            "dc:type": "Dataset",
            "dc:format": "application/json",
            "dc:identifier": incident.id,
            "dc:source": incident.source_jurisdiction.value,
            "dc:language": "fr-CA",
            "dc:coverage": incident.dc_coverage,
            "dc:rights": incident.dc_rights
        }
    
    def export_to_neo4j_cypher(self, incidents: List[HarmonizedIncident]) -> str:
        """Génère les requêtes Cypher pour Neo4j"""
        cypher = []
        for inc in incidents:
            cypher.append(f"""
MERGE (i:Incident {{id: '{inc.id}'}})
SET i.year = {inc.year},
    i.count = {inc.count},
    i.days_lost = {inc.days_lost},
    i.severity = {inc.severity_level},
    i.sector_scian = '{inc.sector_scian}',
    i.sector_nace = '{inc.sector_nace}',
    i.injury_cnesst = '{inc.injury_cnesst}',
    i.injury_osha = '{inc.injury_osha}'
WITH i
MATCH (s:Secteur {{scian: '{inc.sector_scian}'}})
MERGE (i)-[:DANS_SECTEUR]->(s);
""")
        return "\n".join(cypher)


# ============================================
# EXEMPLE D'UTILISATION
# ============================================

if __name__ == "__main__":
    # Créer l'harmoniseur
    harmonizer = HSEHarmonizer()
    
    # Exemple de données CNESST
    sample_records = [
        {"id": 1, "annee": 2023, "scian": "221122", "genre_accident": "51", "nature_lesion": "40", "nombre_lesions": 245, "jours_perdus": 3675},
        {"id": 2, "annee": 2023, "scian": "23", "genre_accident": "42", "nature_lesion": "30", "nombre_lesions": 3200, "jours_perdus": 25600},
        {"id": 3, "annee": 2023, "scian": "62", "genre_accident": "72", "nature_lesion": "10", "nombre_lesions": 6200, "jours_perdus": 24800},
    ]
    
    print("=" * 60)
    print("HSE HARMONIZER - SafetyGraph")
    print("=" * 60)
    
    harmonized = []
    for record in sample_records:
        incident = harmonizer.harmonize_cnesst_record(record)
        harmonized.append(incident)
        
        print(f"\n📊 Incident: {incident.id}")
        print(f"   Secteur: {incident.sector_scian} (SCIAN) | {incident.sector_nace} (NACE)")
        print(f"   Lésion:  {incident.injury_cnesst} (CNESST) → {incident.injury_osha} (OSHA)")
        print(f"   Cause:   {incident.causation_cnesst} (CNESST) → {incident.causation_osha} (OSHA)")
        print(f"   Stats:   {incident.count} lésions, {incident.days_lost} jours perdus")
    
    # Générer métadonnées Dublin Core
    print("\n" + "=" * 60)
    print("MÉTADONNÉES DUBLIN CORE")
    print("=" * 60)
    for inc in harmonized[:1]:
        metadata = harmonizer.generate_dublin_core_metadata(inc)
        print(json.dumps(metadata, indent=2, ensure_ascii=False))
    
    print("\n✅ Harmonisation terminée!")
    print(f"   {len(harmonized)} incidents harmonisés")
    print("   Standards: Dublin Core, DDI, ISO 11179")
    print("   Systèmes: CNESST ↔ OSHA ↔ EU-OSHA")
