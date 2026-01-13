"""
HSE Data Ingestion Pipeline - AgenticX5 / Safety Graph
=======================================================
Pipeline d'ingestion automatisé pour les données HSE internationales
Compatible avec: Kaggle, OSHA API, Eurostat SDMX, ILOSTAT, DARES, BLS

Auteur: Mario Genest - GenAISafety / Preventera
Version: 1.0.0
Date: 2026-01-12
"""

import os
import sys
import json
import logging
import requests
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import hashlib
import time

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s'
)
logger = logging.getLogger("HSEIngestion")


# ============================================================
# CONFIGURATION
# ============================================================

@dataclass
class SourceConfig:
    """Configuration d'une source de données"""
    name: str
    type: str  # api, file, sdmx, kaggle
    url: str
    format: str  # csv, json, xml, sdmx
    update_frequency: str  # daily, weekly, monthly, yearly
    jurisdiction: str
    priority: int = 1
    api_key_env: Optional[str] = None
    rate_limit: int = 100  # requests per minute
    enabled: bool = True
    metadata: Dict = field(default_factory=dict)


# Sources configurées
HSE_SOURCES = {
    # === KAGGLE ===
    "kaggle_osha_injuries": SourceConfig(
        name="OSHA Work-Related Injury Data 2016-2021",
        type="kaggle",
        url="robikscube/osha-injury-data-20162021",
        format="csv",
        update_frequency="static",
        jurisdiction="USA",
        priority=1,
        api_key_env="KAGGLE_KEY",
        metadata={
            "records": "1M+",
            "years": "2016-2021",
            "use_case": "ML training, benchmarking"
        }
    ),
    "kaggle_industrial_safety": SourceConfig(
        name="Industrial Safety & Health Analytics",
        type="kaggle",
        url="ihmstefanini/industrial-safety-and-health-analytics-database",
        format="csv",
        update_frequency="static",
        jurisdiction="International",
        priority=2,
        api_key_env="KAGGLE_KEY",
        metadata={
            "records": "~12K",
            "use_case": "Severity classification"
        }
    ),
    
    # === OSHA OFFICIAL ===
    "osha_inspections": SourceConfig(
        name="OSHA Inspection Data",
        type="api",
        url="https://enforcedata.dol.gov/views/data_catalogs.php",
        format="csv",
        update_frequency="weekly",
        jurisdiction="USA",
        priority=1,
        rate_limit=60,
        metadata={
            "records": "8M+",
            "years": "1972-present",
            "tables": ["osha_inspection", "osha_violation", "osha_accident"]
        }
    ),
    "osha_ita": SourceConfig(
        name="OSHA Injury Tracking Application (ITA)",
        type="api",
        url="https://www.osha.gov/Establishment-Specific-Injury-and-Illness-Data",
        format="csv",
        update_frequency="yearly",
        jurisdiction="USA",
        priority=1,
        metadata={
            "forms": ["300A", "300", "301"],
            "establishments": "430K+"
        }
    ),
    "osha_severe_injuries": SourceConfig(
        name="OSHA Severe Injury Reports",
        type="file",
        url="https://www.osha.gov/severeinjury/xml/severeinjury.xml",
        format="xml",
        update_frequency="daily",
        jurisdiction="USA",
        priority=2,
        metadata={
            "type": "hospitalizations, amputations, eye loss"
        }
    ),
    
    # === BLS ===
    "bls_cfoi": SourceConfig(
        name="BLS Census of Fatal Occupational Injuries",
        type="api",
        url="https://api.bls.gov/publicAPI/v2/timeseries/data/",
        format="json",
        update_frequency="yearly",
        jurisdiction="USA",
        priority=1,
        api_key_env="BLS_API_KEY",
        rate_limit=25,
        metadata={
            "series_id": "FWU00X4XXXXXXXX",
            "years": "1992-present"
        }
    ),
    "bls_soii": SourceConfig(
        name="BLS Survey of Occupational Injuries & Illnesses",
        type="api",
        url="https://api.bls.gov/publicAPI/v2/timeseries/data/",
        format="json",
        update_frequency="yearly",
        jurisdiction="USA",
        priority=1,
        api_key_env="BLS_API_KEY",
        metadata={
            "establishments": "200K+"
        }
    ),
    
    # === EUROSTAT ===
    "eurostat_esaw": SourceConfig(
        name="Eurostat ESAW - Accidents at Work",
        type="sdmx",
        url="https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/hsw_n2_01",
        format="sdmx",
        update_frequency="yearly",
        jurisdiction="EU-27",
        priority=1,
        rate_limit=30,
        metadata={
            "countries": 27,
            "classification": "NACE Rev.2"
        }
    ),
    "eurostat_esaw_fatal": SourceConfig(
        name="Eurostat ESAW - Fatal Accidents",
        type="sdmx",
        url="https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/hsw_n2_02",
        format="sdmx",
        update_frequency="yearly",
        jurisdiction="EU-27",
        priority=1
    ),
    
    # === ILOSTAT ===
    "ilostat_injuries": SourceConfig(
        name="ILOSTAT Occupational Injuries",
        type="sdmx",
        url="https://www.ilo.org/sdmx/rest/data/ILO,DF_INJ_FATL_SEX_ECO_NB",
        format="sdmx",
        update_frequency="yearly",
        jurisdiction="International",
        priority=1,
        metadata={
            "countries": "180+",
            "indicator": "SDG 8.8.1"
        }
    ),
    
    # === FRANCE ===
    "dares_at": SourceConfig(
        name="DARES Accidents du Travail France",
        type="file",
        url="https://dares.travail-emploi.gouv.fr/donnees/les-accidents-du-travail",
        format="xlsx",
        update_frequency="yearly",
        jurisdiction="France",
        priority=1,
        metadata={
            "sources": ["CNAM", "MSA", "CNRACL"],
            "records_annual": "668K"
        }
    ),
    "carsat_opendata": SourceConfig(
        name="CARSAT Open Data - Pays de la Loire",
        type="api",
        url="https://opendata.carsat-pl.fr/api/",
        format="json",
        update_frequency="yearly",
        jurisdiction="France",
        priority=2
    ),
    
    # === CANADA ===
    "cnesst_lesions": SourceConfig(
        name="CNESST Lésions Professionnelles Québec",
        type="file",
        url="https://www.donneesquebec.ca/recherche/dataset/lesions-professionnelles",
        format="csv",
        update_frequency="yearly",
        jurisdiction="Quebec",
        priority=1,
        metadata={
            "records": "793K+",
            "years": "2017-2023"
        }
    ),
}


# ============================================================
# CLASSE DE BASE - CONNECTEUR HSE
# ============================================================

class HSEDataConnector(ABC):
    """Classe de base abstraite pour tous les connecteurs HSE"""
    
    def __init__(self, config: SourceConfig, data_dir: str = "data"):
        self.config = config
        self.data_dir = Path(data_dir)
        self.raw_dir = self.data_dir / "bronze" / config.name.lower().replace(" ", "_")
        self.clean_dir = self.data_dir / "silver" / config.name.lower().replace(" ", "_")
        self.gold_dir = self.data_dir / "gold"
        
        # Créer les répertoires
        for d in [self.raw_dir, self.clean_dir, self.gold_dir]:
            d.mkdir(parents=True, exist_ok=True)
        
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "AgenticX5-HSE-Connector/1.0 (GenAISafety)"
        })
        
        self._cache: Dict = {}
        self._last_request = 0
    
    def _rate_limit(self):
        """Respecter le rate limiting"""
        min_interval = 60 / self.config.rate_limit
        elapsed = time.time() - self._last_request
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self._last_request = time.time()
    
    def _get_api_key(self) -> Optional[str]:
        """Récupérer la clé API depuis les variables d'environnement"""
        if self.config.api_key_env:
            return os.getenv(self.config.api_key_env)
        return None
    
    def _generate_hash(self, data: str) -> str:
        """Générer un hash pour le versioning des données"""
        return hashlib.md5(data.encode()).hexdigest()[:12]
    
    @abstractmethod
    def fetch(self, **kwargs) -> pd.DataFrame:
        """Télécharger les données brutes"""
        pass
    
    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transformer vers le schéma Silver"""
        pass
    
    def harmonize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Harmoniser vers le schéma Gold (commun à toutes les sources)"""
        # Schéma Gold standard AgenticX5
        gold_schema = {
            'incident_id': str,
            'source': str,
            'jurisdiction': str,
            'date': 'datetime64[ns]',
            'year': int,
            'industry_code': str,  # NAICS ou NACE
            'industry_code_system': str,
            'industry_name': str,
            'establishment_size': str,
            'incident_type': str,  # fatal, non-fatal, illness
            'severity': str,
            'nature_of_injury': str,
            'body_part': str,
            'event_type': str,
            'days_lost': float,
            'worker_age': float,
            'worker_gender': str,
            'worker_employment_status': str,
            'latitude': float,
            'longitude': float,
            'narrative': str,
            'created_at': 'datetime64[ns]',
            'updated_at': 'datetime64[ns]'
        }
        
        # Ajouter les colonnes manquantes
        for col, dtype in gold_schema.items():
            if col not in df.columns:
                if dtype == str:
                    df[col] = None
                elif dtype == float:
                    df[col] = np.nan
                elif dtype == int:
                    df[col] = 0
        
        df['source'] = self.config.name
        df['jurisdiction'] = self.config.jurisdiction
        df['updated_at'] = datetime.now()
        
        return df
    
    def save_bronze(self, df: pd.DataFrame, suffix: str = "") -> Path:
        """Sauvegarder en couche Bronze (brut)"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"bronze_{timestamp}{suffix}.parquet"
        filepath = self.raw_dir / filename
        df.to_parquet(filepath, index=False)
        logger.info(f"✅ Bronze saved: {filepath} ({len(df)} rows)")
        return filepath
    
    def save_silver(self, df: pd.DataFrame, suffix: str = "") -> Path:
        """Sauvegarder en couche Silver (nettoyé)"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"silver_{timestamp}{suffix}.parquet"
        filepath = self.clean_dir / filename
        df.to_parquet(filepath, index=False)
        logger.info(f"✅ Silver saved: {filepath} ({len(df)} rows)")
        return filepath
    
    def save_gold(self, df: pd.DataFrame, table_name: str = "incidents") -> Path:
        """Sauvegarder en couche Gold (harmonisé)"""
        timestamp = datetime.now().strftime("%Y%m%d")
        filename = f"gold_{table_name}_{self.config.jurisdiction.lower()}_{timestamp}.parquet"
        filepath = self.gold_dir / filename
        df.to_parquet(filepath, index=False)
        logger.info(f"✅ Gold saved: {filepath} ({len(df)} rows)")
        return filepath
    
    def run_pipeline(self, **kwargs) -> Dict:
        """Exécuter le pipeline complet Bronze → Silver → Gold"""
        logger.info(f"🚀 Starting pipeline: {self.config.name}")
        result = {
            "source": self.config.name,
            "status": "success",
            "started_at": datetime.now().isoformat(),
            "steps": {}
        }
        
        try:
            # Bronze: Fetch raw data
            logger.info("📥 Step 1/3: Fetching raw data (Bronze)...")
            df_bronze = self.fetch(**kwargs)
            bronze_path = self.save_bronze(df_bronze)
            result["steps"]["bronze"] = {
                "rows": len(df_bronze),
                "path": str(bronze_path)
            }
            
            # Silver: Transform
            logger.info("🔧 Step 2/3: Transforming data (Silver)...")
            df_silver = self.transform(df_bronze)
            silver_path = self.save_silver(df_silver)
            result["steps"]["silver"] = {
                "rows": len(df_silver),
                "path": str(silver_path)
            }
            
            # Gold: Harmonize
            logger.info("✨ Step 3/3: Harmonizing data (Gold)...")
            df_gold = self.harmonize(df_silver)
            gold_path = self.save_gold(df_gold)
            result["steps"]["gold"] = {
                "rows": len(df_gold),
                "path": str(gold_path)
            }
            
        except Exception as e:
            result["status"] = "failed"
            result["error"] = str(e)
            logger.error(f"❌ Pipeline failed: {e}")
        
        result["completed_at"] = datetime.now().isoformat()
        return result


# ============================================================
# CONNECTEUR KAGGLE
# ============================================================

class KaggleConnector(HSEDataConnector):
    """Connecteur pour les datasets Kaggle"""
    
    def fetch(self, **kwargs) -> pd.DataFrame:
        """Télécharger un dataset Kaggle"""
        try:
            import kaggle
            
            dataset_path = self.config.url
            download_dir = self.raw_dir / "download"
            download_dir.mkdir(exist_ok=True)
            
            logger.info(f"📥 Downloading Kaggle dataset: {dataset_path}")
            kaggle.api.dataset_download_files(
                dataset_path,
                path=str(download_dir),
                unzip=True
            )
            
            # Charger les fichiers CSV
            dfs = []
            for csv_file in download_dir.glob("*.csv"):
                logger.info(f"   Loading {csv_file.name}...")
                df = pd.read_csv(csv_file, low_memory=False)
                dfs.append(df)
            
            if dfs:
                combined = pd.concat(dfs, ignore_index=True)
                logger.info(f"✅ Loaded {len(combined)} records from Kaggle")
                return combined
            
            return pd.DataFrame()
            
        except ImportError:
            logger.error("❌ Kaggle package not installed. Run: pip install kaggle")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"❌ Kaggle download failed: {e}")
            return pd.DataFrame()
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transformer les données Kaggle OSHA"""
        if df.empty:
            return df
        
        # Mapping des colonnes OSHA vers schéma standard
        column_mapping = {
            'establishment_name': 'establishment_name',
            'ein': 'establishment_id',
            'naics_code': 'industry_code',
            'industry_description': 'industry_name',
            'annual_average_employees': 'establishment_size',
            'total_hours_worked': 'hours_worked',
            'total_deaths': 'fatalities',
            'total_dafw_cases': 'days_away_cases',
            'total_djtr_cases': 'restricted_duty_cases',
            'total_injuries': 'total_incidents',
            'year_filing_for': 'year'
        }
        
        df_transformed = df.rename(columns={
            k: v for k, v in column_mapping.items() if k in df.columns
        })
        
        # Ajouter le système de code industriel
        df_transformed['industry_code_system'] = 'NAICS'
        
        # Calculer le taux d'incidence si possible
        if 'total_incidents' in df_transformed.columns and 'hours_worked' in df_transformed.columns:
            df_transformed['incidence_rate'] = (
                df_transformed['total_incidents'] / df_transformed['hours_worked'] * 200000
            ).round(2)
        
        return df_transformed


# ============================================================
# CONNECTEUR OSHA API
# ============================================================

class OSHAConnector(HSEDataConnector):
    """Connecteur pour l'API OSHA (Department of Labor)"""
    
    BASE_URL = "https://enforcedata.dol.gov/views/data_catalogs.php"
    
    def fetch(self, table: str = "osha_inspection", **kwargs) -> pd.DataFrame:
        """Télécharger les données OSHA"""
        self._rate_limit()
        
        # URLs directes pour les tables OSHA
        direct_urls = {
            "osha_inspection": "https://enforcedata.dol.gov/views/data_catalog_request.php?dataset=osha_inspection",
            "osha_violation": "https://enforcedata.dol.gov/views/data_catalog_request.php?dataset=osha_violation",
            "osha_accident": "https://enforcedata.dol.gov/views/data_catalog_request.php?dataset=osha_accident",
            "osha_accident_injury": "https://enforcedata.dol.gov/views/data_catalog_request.php?dataset=osha_accident_injury"
        }
        
        url = direct_urls.get(table, direct_urls["osha_inspection"])
        
        try:
            logger.info(f"📥 Fetching OSHA {table}...")
            
            # Pour les gros fichiers, on télécharge en chunks
            response = self.session.get(url, stream=True, timeout=300)
            response.raise_for_status()
            
            # Sauvegarder temporairement
            temp_file = self.raw_dir / f"{table}_temp.csv"
            with open(temp_file, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            # Charger avec pandas
            df = pd.read_csv(temp_file, low_memory=False)
            logger.info(f"✅ Loaded {len(df)} OSHA {table} records")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ OSHA fetch failed: {e}")
            return pd.DataFrame()
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transformer les données OSHA"""
        if df.empty:
            return df
        
        # Mapping OSHA vers schéma standard
        column_mapping = {
            'activity_nr': 'inspection_id',
            'reporting_id': 'incident_id',
            'estab_name': 'establishment_name',
            'site_address': 'address',
            'site_city': 'city',
            'site_state': 'state',
            'site_zip': 'zip_code',
            'naics_code': 'industry_code',
            'sic_code': 'sic_code',
            'open_date': 'inspection_date',
            'case_mod_date': 'updated_date',
            'nr_in_estab': 'establishment_size',
            'insp_type': 'inspection_type',
            'safety_hlth': 'safety_health_focus',
            'violation_type_s': 'violation_type',
            'initial_penalty': 'penalty_amount',
            'nr_instances': 'violation_instances',
            'hazsub1': 'hazardous_substance',
            'summary_nr': 'accident_summary_id'
        }
        
        df_transformed = df.rename(columns={
            k: v for k, v in column_mapping.items() if k in df.columns
        })
        
        df_transformed['industry_code_system'] = 'NAICS'
        df_transformed['source_table'] = 'osha_inspection'
        
        return df_transformed


# ============================================================
# CONNECTEUR BLS API
# ============================================================

class BLSConnector(HSEDataConnector):
    """Connecteur pour l'API Bureau of Labor Statistics"""
    
    BASE_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
    
    # Series IDs pour les données SST
    SERIES_IDS = {
        "fatal_injuries_total": "FWU00X4XXXXXXXX1",
        "fatal_injuries_construction": "FWU00X4230XXXXX1",
        "fatal_injuries_manufacturing": "FWU00X431XXXXXX1",
        "nonfatal_rate_private": "ISU00000000000000100000"
    }
    
    def fetch(self, series_ids: List[str] = None, start_year: int = 2015, end_year: int = 2023, **kwargs) -> pd.DataFrame:
        """Télécharger les données BLS"""
        self._rate_limit()
        
        if series_ids is None:
            series_ids = list(self.SERIES_IDS.values())[:4]  # Limite de 4 séries par requête
        
        api_key = self._get_api_key()
        
        payload = {
            "seriesid": series_ids,
            "startyear": str(start_year),
            "endyear": str(end_year),
            "registrationkey": api_key if api_key else ""
        }
        
        try:
            logger.info(f"📥 Fetching BLS data ({start_year}-{end_year})...")
            response = self.session.post(self.BASE_URL, json=payload, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get("status") != "REQUEST_SUCCEEDED":
                logger.error(f"BLS API error: {data.get('message', 'Unknown error')}")
                return pd.DataFrame()
            
            # Parser les résultats
            records = []
            for series in data.get("Results", {}).get("series", []):
                series_id = series.get("seriesID")
                for item in series.get("data", []):
                    records.append({
                        "series_id": series_id,
                        "year": int(item.get("year", 0)),
                        "period": item.get("period", ""),
                        "value": float(item.get("value", 0)),
                        "footnotes": item.get("footnotes", [])
                    })
            
            df = pd.DataFrame(records)
            logger.info(f"✅ Loaded {len(df)} BLS data points")
            return df
            
        except Exception as e:
            logger.error(f"❌ BLS fetch failed: {e}")
            return pd.DataFrame()
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transformer les données BLS"""
        if df.empty:
            return df
        
        # Mapper les series IDs vers des noms lisibles
        series_names = {v: k for k, v in self.SERIES_IDS.items()}
        df['indicator'] = df['series_id'].map(series_names).fillna(df['series_id'])
        
        return df


# ============================================================
# CONNECTEUR EUROSTAT SDMX
# ============================================================

class EurostatConnector(HSEDataConnector):
    """Connecteur pour Eurostat via SDMX API"""
    
    BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data"
    
    # Datasets ESAW
    DATASETS = {
        "accidents_nace": "hsw_n2_01",  # Par activité économique
        "accidents_fatal": "hsw_n2_02",  # Accidents mortels
        "accidents_severity": "hsw_n2_03",  # Par sévérité
        "accidents_age": "hsw_n2_04"  # Par âge
    }
    
    def fetch(self, dataset: str = "accidents_nace", **kwargs) -> pd.DataFrame:
        """Télécharger les données Eurostat ESAW"""
        self._rate_limit()
        
        dataset_id = self.DATASETS.get(dataset, dataset)
        url = f"{self.BASE_URL}/{dataset_id}?format=JSON"
        
        try:
            logger.info(f"📥 Fetching Eurostat {dataset_id}...")
            response = self.session.get(url, timeout=120)
            response.raise_for_status()
            
            data = response.json()
            
            # Parser le format JSON-stat
            records = self._parse_jsonstat(data)
            df = pd.DataFrame(records)
            
            logger.info(f"✅ Loaded {len(df)} Eurostat records")
            return df
            
        except Exception as e:
            logger.error(f"❌ Eurostat fetch failed: {e}")
            return pd.DataFrame()
    
    def _parse_jsonstat(self, data: Dict) -> List[Dict]:
        """Parser le format JSON-stat d'Eurostat"""
        records = []
        
        try:
            dimensions = data.get("dimension", {})
            values = data.get("value", {})
            
            # Extraire les dimensions
            dim_keys = list(dimensions.keys())
            dim_sizes = [len(dimensions[d].get("category", {}).get("index", {})) for d in dim_keys]
            
            # Créer les combinaisons
            for idx, value in values.items():
                record = {"value": value}
                
                # Décoder l'index multi-dimensionnel
                idx_int = int(idx)
                for i, dim_key in enumerate(dim_keys):
                    categories = dimensions[dim_key].get("category", {})
                    labels = categories.get("label", {})
                    index = categories.get("index", {})
                    
                    # Calculer la position dans cette dimension
                    divisor = 1
                    for j in range(i + 1, len(dim_keys)):
                        divisor *= dim_sizes[j]
                    pos = (idx_int // divisor) % dim_sizes[i]
                    
                    # Trouver la clé correspondante
                    for key, position in index.items():
                        if position == pos:
                            record[dim_key] = key
                            record[f"{dim_key}_label"] = labels.get(key, key)
                            break
                
                records.append(record)
                
        except Exception as e:
            logger.warning(f"JSON-stat parsing issue: {e}")
        
        return records
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transformer les données Eurostat"""
        if df.empty:
            return df
        
        # Renommer les colonnes ESAW vers schéma standard
        column_mapping = {
            'geo': 'country_code',
            'geo_label': 'country_name',
            'nace_r2': 'industry_code',
            'nace_r2_label': 'industry_name',
            'severity': 'severity_code',
            'severity_label': 'severity',
            'sex': 'gender_code',
            'sex_label': 'worker_gender',
            'time': 'year',
            'value': 'incident_count'
        }
        
        df_transformed = df.rename(columns={
            k: v for k, v in column_mapping.items() if k in df.columns
        })
        
        df_transformed['industry_code_system'] = 'NACE_REV2'
        
        return df_transformed


# ============================================================
# CONNECTEUR ILOSTAT
# ============================================================

class ILOSTATConnector(HSEDataConnector):
    """Connecteur pour ILOSTAT (ILO Statistics)"""
    
    BASE_URL = "https://www.ilo.org/sdmx/rest/data"
    
    # Dataflows pour SST
    DATAFLOWS = {
        "fatal_injuries": "ILO,DF_INJ_FATL_SEX_ECO_NB",
        "nonfatal_injuries": "ILO,DF_INJ_NFTL_SEX_ECO_NB",
        "days_lost": "ILO,DF_INJ_DAYS_SEX_ECO_NB"
    }
    
    def fetch(self, dataflow: str = "fatal_injuries", **kwargs) -> pd.DataFrame:
        """Télécharger les données ILOSTAT"""
        self._rate_limit()
        
        dataflow_id = self.DATAFLOWS.get(dataflow, dataflow)
        url = f"{self.BASE_URL}/{dataflow_id}/all?format=csv"
        
        try:
            logger.info(f"📥 Fetching ILOSTAT {dataflow_id}...")
            response = self.session.get(url, timeout=180)
            response.raise_for_status()
            
            # ILOSTAT retourne du CSV
            from io import StringIO
            df = pd.read_csv(StringIO(response.text))
            
            logger.info(f"✅ Loaded {len(df)} ILOSTAT records")
            return df
            
        except Exception as e:
            logger.error(f"❌ ILOSTAT fetch failed: {e}")
            return pd.DataFrame()
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transformer les données ILOSTAT"""
        if df.empty:
            return df
        
        column_mapping = {
            'REF_AREA': 'country_code',
            'SEX': 'gender_code',
            'CLASSIF1': 'classification1',
            'TIME_PERIOD': 'year',
            'OBS_VALUE': 'value'
        }
        
        df_transformed = df.rename(columns={
            k: v for k, v in column_mapping.items() if k in df.columns
        })
        
        df_transformed['industry_code_system'] = 'ISIC'
        
        return df_transformed


# ============================================================
# CONNECTEUR DARES (FRANCE)
# ============================================================

class DARESConnector(HSEDataConnector):
    """Connecteur pour les données DARES (France)"""
    
    def fetch(self, **kwargs) -> pd.DataFrame:
        """Télécharger les données DARES"""
        # Les données DARES sont généralement en XLSX
        # URL directe vers le fichier Excel
        xlsx_url = kwargs.get('url', 'https://dares.travail-emploi.gouv.fr/sites/default/files/xlsx/accidents-travail.xlsx')
        
        try:
            logger.info(f"📥 Fetching DARES data...")
            response = self.session.get(xlsx_url, timeout=60)
            response.raise_for_status()
            
            # Sauvegarder temporairement
            temp_file = self.raw_dir / "dares_temp.xlsx"
            with open(temp_file, 'wb') as f:
                f.write(response.content)
            
            # Charger avec pandas
            df = pd.read_excel(temp_file, sheet_name=0)
            logger.info(f"✅ Loaded {len(df)} DARES records")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ DARES fetch failed: {e}")
            return pd.DataFrame()
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transformer les données DARES"""
        if df.empty:
            return df
        
        # Mapping colonnes DARES
        column_mapping = {
            'Secteur d\'activité': 'industry_name',
            'Code NAF': 'industry_code',
            'Année': 'year',
            'Nombre d\'AT': 'incident_count',
            'Nombre de décès': 'fatalities',
            'Nombre de jours d\'arrêt': 'days_lost',
            'Indice de fréquence': 'frequency_index',
            'Indice de gravité': 'severity_index'
        }
        
        df_transformed = df.rename(columns={
            k: v for k, v in column_mapping.items() if k in df.columns
        })
        
        df_transformed['industry_code_system'] = 'NAF'
        
        return df_transformed


# ============================================================
# ORCHESTRATEUR DE PIPELINE
# ============================================================

class HSEPipelineOrchestrator:
    """Orchestrateur pour exécuter les pipelines d'ingestion HSE"""
    
    CONNECTORS = {
        "kaggle": KaggleConnector,
        "osha": OSHAConnector,
        "bls": BLSConnector,
        "eurostat": EurostatConnector,
        "ilostat": ILOSTATConnector,
        "dares": DARESConnector
    }
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.results: List[Dict] = []
    
    def get_connector(self, source_key: str) -> Optional[HSEDataConnector]:
        """Obtenir le connecteur approprié pour une source"""
        if source_key not in HSE_SOURCES:
            logger.error(f"Unknown source: {source_key}")
            return None
        
        config = HSE_SOURCES[source_key]
        
        # Mapper le type vers la classe de connecteur
        type_mapping = {
            "kaggle": KaggleConnector,
            "api": OSHAConnector,  # Par défaut pour les APIs
            "sdmx": EurostatConnector,
            "file": DARESConnector
        }
        
        # Surcharges spécifiques
        if "osha" in source_key.lower():
            connector_class = OSHAConnector
        elif "bls" in source_key.lower():
            connector_class = BLSConnector
        elif "eurostat" in source_key.lower():
            connector_class = EurostatConnector
        elif "ilostat" in source_key.lower():
            connector_class = ILOSTATConnector
        elif "dares" in source_key.lower():
            connector_class = DARESConnector
        else:
            connector_class = type_mapping.get(config.type, HSEDataConnector)
        
        return connector_class(config, str(self.data_dir))
    
    def run_single(self, source_key: str, **kwargs) -> Dict:
        """Exécuter le pipeline pour une source"""
        connector = self.get_connector(source_key)
        if connector:
            result = connector.run_pipeline(**kwargs)
            self.results.append(result)
            return result
        return {"status": "failed", "error": f"Connector not found for {source_key}"}
    
    def run_all(self, priority_threshold: int = 3, **kwargs) -> List[Dict]:
        """Exécuter les pipelines pour toutes les sources activées"""
        results = []
        
        # Trier par priorité
        sorted_sources = sorted(
            HSE_SOURCES.items(),
            key=lambda x: x[1].priority
        )
        
        for source_key, config in sorted_sources:
            if not config.enabled:
                logger.info(f"⏭️ Skipping disabled source: {source_key}")
                continue
            
            if config.priority > priority_threshold:
                logger.info(f"⏭️ Skipping low-priority source: {source_key} (priority {config.priority})")
                continue
            
            logger.info(f"\n{'='*60}")
            logger.info(f"🔄 Processing: {config.name}")
            logger.info(f"{'='*60}")
            
            result = self.run_single(source_key, **kwargs)
            results.append(result)
            
            # Pause entre les sources
            time.sleep(2)
        
        self.results = results
        return results
    
    def generate_report(self) -> Dict:
        """Générer un rapport d'exécution"""
        report = {
            "execution_date": datetime.now().isoformat(),
            "total_sources": len(self.results),
            "successful": sum(1 for r in self.results if r.get("status") == "success"),
            "failed": sum(1 for r in self.results if r.get("status") == "failed"),
            "total_rows_ingested": sum(
                r.get("steps", {}).get("gold", {}).get("rows", 0) 
                for r in self.results
            ),
            "sources": self.results
        }
        
        # Sauvegarder le rapport
        report_path = self.data_dir / f"ingestion_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        logger.info(f"📊 Report saved: {report_path}")
        return report
    
    def merge_gold_tables(self, output_file: str = "hse_incidents_global.parquet") -> Path:
        """Fusionner tous les fichiers Gold en une table unique"""
        gold_files = list(self.data_dir.glob("gold/gold_*.parquet"))
        
        if not gold_files:
            logger.warning("No Gold files to merge")
            return None
        
        dfs = []
        for f in gold_files:
            try:
                df = pd.read_parquet(f)
                dfs.append(df)
                logger.info(f"   ✓ Loaded {f.name}: {len(df)} rows")
            except Exception as e:
                logger.error(f"   ✗ Failed to load {f.name}: {e}")
        
        if dfs:
            merged = pd.concat(dfs, ignore_index=True)
            output_path = self.data_dir / "gold" / output_file
            merged.to_parquet(output_path, index=False)
            logger.info(f"✅ Merged Gold table: {output_path} ({len(merged)} total rows)")
            return output_path
        
        return None


# ============================================================
# CLI & MAIN
# ============================================================

def main():
    """Point d'entrée principal"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="HSE Data Ingestion Pipeline - AgenticX5",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Lister les sources disponibles
  python hse_data_ingestion.py --list
  
  # Exécuter une source spécifique
  python hse_data_ingestion.py --source kaggle_osha_injuries
  
  # Exécuter toutes les sources priorité 1
  python hse_data_ingestion.py --all --priority 1
  
  # Fusionner les tables Gold
  python hse_data_ingestion.py --merge
        """
    )
    
    parser.add_argument("--list", action="store_true", help="Lister les sources disponibles")
    parser.add_argument("--source", type=str, help="Exécuter une source spécifique")
    parser.add_argument("--all", action="store_true", help="Exécuter toutes les sources")
    parser.add_argument("--priority", type=int, default=2, help="Seuil de priorité max (1-3)")
    parser.add_argument("--merge", action="store_true", help="Fusionner les tables Gold")
    parser.add_argument("--data-dir", type=str, default="data", help="Répertoire de données")
    parser.add_argument("--report", action="store_true", help="Générer un rapport")
    
    args = parser.parse_args()
    
    if args.list:
        print("\n📋 Sources HSE Disponibles:")
        print("=" * 80)
        for key, config in sorted(HSE_SOURCES.items(), key=lambda x: x[1].priority):
            status = "✅" if config.enabled else "❌"
            print(f"{status} [{config.priority}] {key}")
            print(f"      {config.name}")
            print(f"      Type: {config.type} | Juridiction: {config.jurisdiction}")
            print(f"      URL: {config.url[:60]}...")
            print()
        return
    
    orchestrator = HSEPipelineOrchestrator(args.data_dir)
    
    if args.source:
        print(f"\n🚀 Running pipeline for: {args.source}")
        result = orchestrator.run_single(args.source)
        print(json.dumps(result, indent=2, default=str))
    
    elif args.all:
        print(f"\n🚀 Running all pipelines (priority <= {args.priority})")
        results = orchestrator.run_all(priority_threshold=args.priority)
        
        if args.report:
            report = orchestrator.generate_report()
            print(f"\n📊 Execution Summary:")
            print(f"   Sources: {report['total_sources']}")
            print(f"   Success: {report['successful']}")
            print(f"   Failed:  {report['failed']}")
            print(f"   Total rows: {report['total_rows_ingested']:,}")
    
    if args.merge:
        print("\n🔗 Merging Gold tables...")
        orchestrator.merge_gold_tables()
    
    print("\n✅ Done!")


if __name__ == "__main__":
    main()
