# 🚀 Guide d'Exécution des Pipelines HSE - AgenticX5

## Architecture Multi-Projets

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        ÉCOSYSTÈME AGENTICX5                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                   │
│  │   ZERVE.AI   │    │  SAFETY      │    │   CLAUDE     │                   │
│  │  (Notebooks) │    │  GRAPH DB    │    │  PROJECTS    │                   │
│  └──────┬───────┘    └──────┬───────┘    └──────┬───────┘                   │
│         │                   │                   │                            │
│         └───────────────────┼───────────────────┘                            │
│                             │                                                │
│                    ┌────────▼────────┐                                       │
│                    │  HSE INGESTION  │                                       │
│                    │    PIPELINE     │                                       │
│                    └────────┬────────┘                                       │
│                             │                                                │
│         ┌───────────────────┼───────────────────┐                            │
│         │                   │                   │                            │
│  ┌──────▼──────┐    ┌───────▼──────┐    ┌──────▼──────┐                     │
│  │   BRONZE    │    │    SILVER    │    │    GOLD     │                     │
│  │  (Raw Data) │───▶│  (Cleaned)   │───▶│ (Harmonized)│                     │
│  └─────────────┘    └──────────────┘    └─────────────┘                     │
│                                                │                             │
│                    ┌───────────────────────────┼─────────────────────┐       │
│                    │                           │                     │       │
│             ┌──────▼──────┐            ┌───────▼──────┐      ┌───────▼─────┐│
│             │ PostgreSQL  │            │    Parquet   │      │   Neo4j     ││
│             │ Safety Graph│            │  Data Lake   │      │ Knowledge   ││
│             └─────────────┘            └──────────────┘      └─────────────┘│
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 📁 Structure des Fichiers

```
agenticx5-hse-ingestion/
│
├── 📄 hse_data_ingestion.py      # Module principal d'ingestion
├── 📄 config/
│   ├── sources.yaml              # Configuration des sources
│   ├── env.local.yaml            # Config environnement local
│   ├── env.zerve.yaml            # Config Zerve.ai
│   └── env.production.yaml       # Config production
│
├── 📄 connectors/
│   ├── kaggle_connector.py
│   ├── osha_connector.py
│   ├── eurostat_connector.py
│   ├── ilostat_connector.py
│   └── cnesst_connector.py
│
├── 📄 pipelines/
│   ├── bronze_pipeline.py
│   ├── silver_pipeline.py
│   └── gold_pipeline.py
│
├── 📄 integrations/
│   ├── postgresql_loader.py      # Chargement Safety Graph
│   ├── neo4j_loader.py           # Chargement Knowledge Graph
│   └── parquet_exporter.py       # Export Data Lake
│
├── 📄 schedulers/
│   ├── cron_scheduler.py
│   ├── airflow_dag.py
│   └── prefect_flow.py
│
└── 📄 data/
    ├── bronze/                   # Données brutes
    ├── silver/                   # Données nettoyées
    └── gold/                     # Données harmonisées
```

---

## 🔧 Installation

### Prérequis

```bash
# Python 3.10+
python --version

# Créer environnement virtuel
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
.\venv\Scripts\activate   # Windows
```

### Dépendances

```bash
# Installation des packages
pip install -r requirements.txt
```

**requirements.txt:**
```
pandas>=2.0.0
numpy>=1.24.0
requests>=2.28.0
pyarrow>=12.0.0
sqlalchemy>=2.0.0
psycopg2-binary>=2.9.0
kaggle>=1.5.0
python-dotenv>=1.0.0
pyyaml>=6.0
schedule>=1.2.0
neo4j>=5.0.0
openpyxl>=3.1.0
lxml>=4.9.0
tqdm>=4.65.0
```

### Variables d'Environnement

```bash
# .env file
# === API Keys ===
KAGGLE_USERNAME=your_kaggle_username
KAGGLE_KEY=your_kaggle_api_key
BLS_API_KEY=your_bls_api_key

# === Database ===
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=safety_graph
POSTGRES_USER=agenticx5
POSTGRES_PASSWORD=your_password

# === Neo4j ===
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_password

# === Paths ===
DATA_DIR=/path/to/data
LOG_DIR=/path/to/logs
```

---

## 🖥️ Exécution Locale (Développement)

### 1. Mode CLI Basique

```bash
# Lister toutes les sources disponibles
python hse_data_ingestion.py --list

# Exécuter une source spécifique
python hse_data_ingestion.py --source kaggle_osha_injuries

# Exécuter toutes les sources priorité 1 (critiques)
python hse_data_ingestion.py --all --priority 1

# Exécuter avec rapport
python hse_data_ingestion.py --all --priority 2 --report

# Fusionner les tables Gold
python hse_data_ingestion.py --merge
```

### 2. Mode Python Interactif

```python
from hse_data_ingestion import HSEPipelineOrchestrator, HSE_SOURCES

# Initialiser l'orchestrateur
orchestrator = HSEPipelineOrchestrator(data_dir="./data")

# Exécuter Kaggle OSHA
result = orchestrator.run_single("kaggle_osha_injuries")
print(f"Status: {result['status']}")
print(f"Rows ingested: {result['steps']['gold']['rows']}")

# Exécuter toutes les sources
results = orchestrator.run_all(priority_threshold=2)

# Générer rapport
report = orchestrator.generate_report()

# Fusionner Gold tables
merged_path = orchestrator.merge_gold_tables()
```

### 3. Script de Test Rapide

```python
# test_ingestion.py
"""
Test rapide d'ingestion HSE
"""
import os
os.environ['DATA_DIR'] = './test_data'

from hse_data_ingestion import (
    HSEPipelineOrchestrator,
    KaggleConnector,
    OSHAConnector,
    EurostatConnector,
    HSE_SOURCES
)

def test_single_source():
    """Tester une source individuelle"""
    config = HSE_SOURCES["kaggle_osha_injuries"]
    connector = KaggleConnector(config, "./test_data")
    
    # Test fetch
    df = connector.fetch()
    print(f"✅ Fetched: {len(df)} rows")
    
    # Test transform
    df_silver = connector.transform(df)
    print(f"✅ Transformed: {len(df_silver)} rows")
    
    # Test harmonize
    df_gold = connector.harmonize(df_silver)
    print(f"✅ Harmonized: {len(df_gold)} rows")
    print(f"   Columns: {list(df_gold.columns)}")

if __name__ == "__main__":
    test_single_source()
```

---

## 📊 Exécution dans Zerve.ai

### Configuration Zerve

Zerve.ai est votre plateforme Data Science préférée. Voici comment y intégrer les pipelines :

### 1. Notebook Zerve - Setup

```python
# Cell 1: Installation
!pip install kaggle pandas pyarrow requests openpyxl

# Cell 2: Configuration
import os
os.environ['KAGGLE_USERNAME'] = 'your_username'
os.environ['KAGGLE_KEY'] = 'your_key'
os.environ['DATA_DIR'] = '/workspace/data'

# Cell 3: Import du module
# Upload hse_data_ingestion.py dans Zerve
from hse_data_ingestion import HSEPipelineOrchestrator, HSE_SOURCES
```

### 2. Notebook Zerve - Exécution Interactive

```python
# Cell 4: Initialisation
orchestrator = HSEPipelineOrchestrator(data_dir="/workspace/data")

# Cell 5: Dashboard des sources
import pandas as pd

sources_df = pd.DataFrame([
    {
        "Source": key,
        "Nom": config.name,
        "Type": config.type,
        "Juridiction": config.jurisdiction,
        "Priorité": config.priority,
        "Activé": config.enabled
    }
    for key, config in HSE_SOURCES.items()
])

display(sources_df.sort_values("Priorité"))
```

### 3. Notebook Zerve - Pipeline Complet

```python
# Cell 6: Exécution Pipeline
from tqdm.notebook import tqdm

results = []
sources_to_run = [
    "kaggle_osha_injuries",
    "osha_inspections", 
    "eurostat_esaw",
    "dares_at"
]

for source in tqdm(sources_to_run, desc="Ingestion HSE"):
    result = orchestrator.run_single(source)
    results.append({
        "source": source,
        "status": result["status"],
        "rows": result.get("steps", {}).get("gold", {}).get("rows", 0)
    })

results_df = pd.DataFrame(results)
display(results_df)
```

### 4. Notebook Zerve - Visualisation

```python
# Cell 7: Visualisation des données Gold
import plotly.express as px

# Charger la table Gold fusionnée
gold_df = pd.read_parquet("/workspace/data/gold/hse_incidents_global.parquet")

# Distribution par juridiction
fig1 = px.pie(
    gold_df.groupby("jurisdiction").size().reset_index(name="count"),
    values="count",
    names="jurisdiction",
    title="Distribution des Incidents par Juridiction"
)
fig1.show()

# Tendances temporelles
if "year" in gold_df.columns:
    yearly = gold_df.groupby(["year", "jurisdiction"]).size().reset_index(name="incidents")
    fig2 = px.line(
        yearly,
        x="year",
        y="incidents",
        color="jurisdiction",
        title="Évolution des Incidents par Année"
    )
    fig2.show()
```

### 5. Template Zerve Complet

```python
"""
==========================================================
ZERVE NOTEBOOK: HSE Data Ingestion Pipeline
AgenticX5 / Safety Graph
==========================================================
"""

# %% [markdown]
# # 🔄 Pipeline d'Ingestion HSE
# 
# Ce notebook exécute le pipeline complet d'ingestion des données HSE 
# internationales vers Safety Graph.

# %% Setup
import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime

# Configuration
os.environ['DATA_DIR'] = '/workspace/agenticx5/data'
os.environ['KAGGLE_USERNAME'] = os.getenv('KAGGLE_USERNAME', '')
os.environ['KAGGLE_KEY'] = os.getenv('KAGGLE_KEY', '')

# Import du module (doit être uploadé dans Zerve)
sys.path.append('/workspace/agenticx5')
from hse_data_ingestion import HSEPipelineOrchestrator, HSE_SOURCES

print(f"✅ Setup completed at {datetime.now()}")
print(f"📂 Data directory: {os.environ['DATA_DIR']}")

# %% Initialisation
orchestrator = HSEPipelineOrchestrator(
    data_dir=os.environ['DATA_DIR']
)

# Afficher les sources
print("\n📋 Sources HSE Configurées:")
for key, config in sorted(HSE_SOURCES.items(), key=lambda x: x[1].priority):
    emoji = "✅" if config.enabled else "❌"
    print(f"  {emoji} [{config.priority}] {key}: {config.name}")

# %% Exécution Sélective
# Choisir les sources à exécuter
SOURCES_TO_RUN = [
    "kaggle_osha_injuries",    # Kaggle - 1M+ records USA
    "eurostat_esaw",           # Eurostat - 27 pays EU  
    "ilostat_injuries",        # ILOSTAT - 180+ pays
]

results = []
for source in SOURCES_TO_RUN:
    print(f"\n{'='*60}")
    print(f"🔄 Processing: {source}")
    print(f"{'='*60}")
    
    result = orchestrator.run_single(source)
    results.append(result)
    
    if result["status"] == "success":
        print(f"✅ Success: {result['steps']['gold']['rows']} rows")
    else:
        print(f"❌ Failed: {result.get('error', 'Unknown error')}")

# %% Rapport
report = orchestrator.generate_report()

print(f"\n{'='*60}")
print("📊 RAPPORT D'EXÉCUTION")
print(f"{'='*60}")
print(f"Date: {report['execution_date']}")
print(f"Sources traitées: {report['total_sources']}")
print(f"Succès: {report['successful']}")
print(f"Échecs: {report['failed']}")
print(f"Total rows: {report['total_rows_ingested']:,}")

# %% Fusion Gold
print("\n🔗 Fusion des tables Gold...")
merged_path = orchestrator.merge_gold_tables()

if merged_path:
    gold_df = pd.read_parquet(merged_path)
    print(f"✅ Table fusionnée: {len(gold_df):,} rows")
    print(f"📊 Colonnes: {list(gold_df.columns)}")
    display(gold_df.head(10))
```

---

## 🐘 Chargement dans PostgreSQL (Safety Graph)

### Script de Chargement

```python
# postgresql_loader.py
"""
Chargement des données Gold vers PostgreSQL Safety Graph
"""

import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
import os

class SafetyGraphLoader:
    """Chargeur pour PostgreSQL Safety Graph"""
    
    def __init__(self):
        self.engine = create_engine(
            f"postgresql://{os.getenv('POSTGRES_USER')}:"
            f"{os.getenv('POSTGRES_PASSWORD')}@"
            f"{os.getenv('POSTGRES_HOST')}:"
            f"{os.getenv('POSTGRES_PORT')}/"
            f"{os.getenv('POSTGRES_DB')}"
        )
    
    def create_tables(self):
        """Créer les tables si elles n'existent pas"""
        ddl = """
        -- Table principale des incidents HSE
        CREATE TABLE IF NOT EXISTS hse_incidents_global (
            id SERIAL PRIMARY KEY,
            incident_id VARCHAR(100),
            source VARCHAR(200),
            jurisdiction VARCHAR(50),
            incident_date DATE,
            year INTEGER,
            industry_code VARCHAR(20),
            industry_code_system VARCHAR(20),
            industry_name TEXT,
            establishment_size VARCHAR(50),
            incident_type VARCHAR(50),
            severity VARCHAR(50),
            nature_of_injury TEXT,
            body_part VARCHAR(100),
            event_type VARCHAR(200),
            days_lost NUMERIC,
            worker_age NUMERIC,
            worker_gender VARCHAR(20),
            narrative TEXT,
            latitude NUMERIC,
            longitude NUMERIC,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            -- Index pour les requêtes fréquentes
            CONSTRAINT unique_incident UNIQUE (incident_id, source)
        );
        
        -- Index
        CREATE INDEX IF NOT EXISTS idx_incidents_jurisdiction ON hse_incidents_global(jurisdiction);
        CREATE INDEX IF NOT EXISTS idx_incidents_year ON hse_incidents_global(year);
        CREATE INDEX IF NOT EXISTS idx_incidents_industry ON hse_incidents_global(industry_code);
        CREATE INDEX IF NOT EXISTS idx_incidents_type ON hse_incidents_global(incident_type);
        
        -- Table de métadonnées des sources
        CREATE TABLE IF NOT EXISTS hse_data_sources (
            id SERIAL PRIMARY KEY,
            source_key VARCHAR(100) UNIQUE,
            source_name VARCHAR(200),
            source_type VARCHAR(50),
            jurisdiction VARCHAR(50),
            url TEXT,
            last_ingestion TIMESTAMP,
            rows_ingested INTEGER,
            status VARCHAR(20)
        );
        """
        
        with self.engine.connect() as conn:
            for statement in ddl.split(';'):
                if statement.strip():
                    conn.execute(text(statement))
            conn.commit()
        
        print("✅ Tables créées/vérifiées")
    
    def load_gold_data(self, parquet_path: str, if_exists: str = "append"):
        """Charger les données Gold dans PostgreSQL"""
        df = pd.read_parquet(parquet_path)
        
        # Renommer les colonnes pour PostgreSQL
        column_mapping = {
            'date': 'incident_date'
        }
        df = df.rename(columns=column_mapping)
        
        # Charger
        df.to_sql(
            'hse_incidents_global',
            self.engine,
            if_exists=if_exists,
            index=False,
            method='multi',
            chunksize=10000
        )
        
        print(f"✅ Chargé {len(df):,} rows dans hse_incidents_global")
        return len(df)
    
    def update_source_metadata(self, source_key: str, source_name: str, 
                               jurisdiction: str, rows: int):
        """Mettre à jour les métadonnées de source"""
        with self.engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO hse_data_sources 
                (source_key, source_name, jurisdiction, last_ingestion, rows_ingested, status)
                VALUES (:key, :name, :jurisdiction, CURRENT_TIMESTAMP, :rows, 'success')
                ON CONFLICT (source_key) DO UPDATE SET
                    last_ingestion = CURRENT_TIMESTAMP,
                    rows_ingested = :rows,
                    status = 'success'
            """), {
                "key": source_key,
                "name": source_name,
                "jurisdiction": jurisdiction,
                "rows": rows
            })
            conn.commit()
    
    def query_summary(self):
        """Obtenir un résumé des données"""
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    jurisdiction,
                    COUNT(*) as total_incidents,
                    COUNT(DISTINCT year) as years_covered,
                    MIN(year) as first_year,
                    MAX(year) as last_year
                FROM hse_incidents_global
                GROUP BY jurisdiction
                ORDER BY total_incidents DESC
            """))
            
            return pd.DataFrame(result.fetchall(), columns=result.keys())


# Utilisation
if __name__ == "__main__":
    loader = SafetyGraphLoader()
    
    # Créer les tables
    loader.create_tables()
    
    # Charger les données Gold
    gold_path = "data/gold/hse_incidents_global.parquet"
    loader.load_gold_data(gold_path)
    
    # Afficher le résumé
    summary = loader.query_summary()
    print("\n📊 Résumé Safety Graph:")
    print(summary)
```

### Exécution du Chargement

```bash
# Terminal
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DB=safety_graph
export POSTGRES_USER=agenticx5
export POSTGRES_PASSWORD=your_password

python postgresql_loader.py
```

---

## ⏰ Automatisation (Scheduling)

### 1. Cron Job (Linux/Mac)

```bash
# Éditer crontab
crontab -e

# Ajouter les jobs
# Ingestion quotidienne OSHA Severe Injuries à 6h
0 6 * * * /path/to/venv/bin/python /path/to/hse_data_ingestion.py --source osha_severe_injuries >> /var/log/hse_ingestion.log 2>&1

# Ingestion hebdomadaire complète le dimanche à 2h
0 2 * * 0 /path/to/venv/bin/python /path/to/hse_data_ingestion.py --all --priority 2 --report >> /var/log/hse_ingestion.log 2>&1

# Fusion mensuelle le 1er à 3h
0 3 1 * * /path/to/venv/bin/python /path/to/hse_data_ingestion.py --merge >> /var/log/hse_ingestion.log 2>&1
```

### 2. Windows Task Scheduler

```powershell
# PowerShell - Créer une tâche planifiée
$action = New-ScheduledTaskAction -Execute "python.exe" -Argument "C:\AgenticX5\hse_data_ingestion.py --all --priority 2"
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Sunday -At 2am
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable

Register-ScheduledTask -TaskName "HSE_Ingestion_Weekly" -Action $action -Trigger $trigger -Settings $settings
```

### 3. Script Python avec Schedule

```python
# scheduler.py
"""
Scheduler pour l'ingestion HSE automatique
"""

import schedule
import time
import logging
from datetime import datetime
from hse_data_ingestion import HSEPipelineOrchestrator

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler('hse_scheduler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

orchestrator = HSEPipelineOrchestrator(data_dir="./data")


def daily_quick_ingestion():
    """Ingestion quotidienne rapide (sources temps réel)"""
    logger.info("🔄 Starting daily quick ingestion...")
    
    quick_sources = [
        "osha_severe_injuries",  # Mis à jour quotidiennement
    ]
    
    for source in quick_sources:
        try:
            result = orchestrator.run_single(source)
            logger.info(f"✅ {source}: {result['status']}")
        except Exception as e:
            logger.error(f"❌ {source}: {e}")


def weekly_full_ingestion():
    """Ingestion hebdomadaire complète"""
    logger.info("🔄 Starting weekly full ingestion...")
    
    try:
        results = orchestrator.run_all(priority_threshold=2)
        report = orchestrator.generate_report()
        
        logger.info(f"📊 Weekly Report:")
        logger.info(f"   Sources: {report['total_sources']}")
        logger.info(f"   Success: {report['successful']}")
        logger.info(f"   Rows: {report['total_rows_ingested']:,}")
        
    except Exception as e:
        logger.error(f"❌ Weekly ingestion failed: {e}")


def monthly_consolidation():
    """Consolidation mensuelle"""
    logger.info("🔄 Starting monthly consolidation...")
    
    try:
        # Fusionner les tables Gold
        merged_path = orchestrator.merge_gold_tables()
        logger.info(f"✅ Gold tables merged: {merged_path}")
        
        # Charger dans PostgreSQL
        from postgresql_loader import SafetyGraphLoader
        loader = SafetyGraphLoader()
        rows = loader.load_gold_data(str(merged_path), if_exists="replace")
        logger.info(f"✅ Loaded {rows:,} rows into Safety Graph")
        
    except Exception as e:
        logger.error(f"❌ Monthly consolidation failed: {e}")


# Configurer le schedule
schedule.every().day.at("06:00").do(daily_quick_ingestion)
schedule.every().sunday.at("02:00").do(weekly_full_ingestion)
schedule.every(1).months.at("03:00").do(monthly_consolidation)  # 1er du mois


if __name__ == "__main__":
    logger.info("🚀 HSE Scheduler started")
    logger.info(f"   Daily: 06:00")
    logger.info(f"   Weekly: Sunday 02:00")
    logger.info(f"   Monthly: 1st 03:00")
    
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute
```

### 4. Apache Airflow DAG

```python
# airflow_dag.py
"""
Apache Airflow DAG pour l'ingestion HSE
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'agenticx5',
    'depends_on_past': False,
    'email': ['alerts@genaisafety.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hse_data_ingestion',
    default_args=default_args,
    description='Pipeline d\'ingestion HSE multi-sources',
    schedule_interval='0 2 * * 0',  # Dimanche 2h
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['hse', 'safety', 'agenticx5'],
)


def run_ingestion(**context):
    """Exécuter le pipeline d'ingestion"""
    import sys
    sys.path.append('/opt/airflow/dags/agenticx5')
    
    from hse_data_ingestion import HSEPipelineOrchestrator
    
    orchestrator = HSEPipelineOrchestrator(data_dir="/data/hse")
    results = orchestrator.run_all(priority_threshold=2)
    report = orchestrator.generate_report()
    
    # Push to XCom
    context['ti'].xcom_push(key='ingestion_report', value=report)
    
    return report


def load_to_postgres(**context):
    """Charger dans PostgreSQL"""
    import sys
    sys.path.append('/opt/airflow/dags/agenticx5')
    
    from postgresql_loader import SafetyGraphLoader
    
    loader = SafetyGraphLoader()
    loader.create_tables()
    
    gold_path = "/data/hse/gold/hse_incidents_global.parquet"
    rows = loader.load_gold_data(gold_path)
    
    return rows


# Tasks
t1_ingest = PythonOperator(
    task_id='ingest_hse_data',
    python_callable=run_ingestion,
    dag=dag,
)

t2_merge = BashOperator(
    task_id='merge_gold_tables',
    bash_command='python /opt/airflow/dags/agenticx5/hse_data_ingestion.py --merge --data-dir /data/hse',
    dag=dag,
)

t3_load = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=dag,
)

t4_notify = BashOperator(
    task_id='send_notification',
    bash_command='echo "HSE Ingestion completed at $(date)"',
    dag=dag,
)

# Dependencies
t1_ingest >> t2_merge >> t3_load >> t4_notify
```

---

## 🐳 Docker Deployment

### Dockerfile

```dockerfile
# Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY hse_data_ingestion.py .
COPY postgresql_loader.py .
COPY scheduler.py .

# Create data directories
RUN mkdir -p /data/bronze /data/silver /data/gold

# Environment
ENV DATA_DIR=/data
ENV PYTHONUNBUFFERED=1

# Default command
CMD ["python", "scheduler.py"]
```

### docker-compose.yml

```yaml
version: '3.8'

services:
  hse-ingestion:
    build: .
    container_name: hse-ingestion
    environment:
      - DATA_DIR=/data
      - KAGGLE_USERNAME=${KAGGLE_USERNAME}
      - KAGGLE_KEY=${KAGGLE_KEY}
      - BLS_API_KEY=${BLS_API_KEY}
      - POSTGRES_HOST=postgres
      - POSTGRES_PORT=5432
      - POSTGRES_DB=safety_graph
      - POSTGRES_USER=agenticx5
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
    volumes:
      - hse-data:/data
      - ./logs:/app/logs
    depends_on:
      - postgres
    restart: unless-stopped

  postgres:
    image: postgres:15
    container_name: safety-graph-db
    environment:
      - POSTGRES_DB=safety_graph
      - POSTGRES_USER=agenticx5
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
    volumes:
      - postgres-data:/var/lib/postgresql/data
    ports:
      - "5432:5432"

  pgadmin:
    image: dpage/pgadmin4
    container_name: pgadmin
    environment:
      - PGADMIN_DEFAULT_EMAIL=admin@genaisafety.com
      - PGADMIN_DEFAULT_PASSWORD=${PGADMIN_PASSWORD}
    ports:
      - "5050:80"
    depends_on:
      - postgres

volumes:
  hse-data:
  postgres-data:
```

### Commandes Docker

```bash
# Build et démarrer
docker-compose up -d --build

# Voir les logs
docker-compose logs -f hse-ingestion

# Exécuter manuellement une ingestion
docker exec hse-ingestion python hse_data_ingestion.py --all --priority 1

# Arrêter
docker-compose down
```

---

## 📋 Résumé des Modes d'Exécution

| Mode | Environnement | Commande | Fréquence |
|------|---------------|----------|-----------|
| **CLI Local** | Dev | `python hse_data_ingestion.py --source X` | Ad-hoc |
| **Notebook Zerve** | Data Science | Cellules interactives | Ad-hoc |
| **Cron Job** | Linux Server | `crontab -e` | Quotidien/Hebdo |
| **Task Scheduler** | Windows | PowerShell | Quotidien/Hebdo |
| **Python Schedule** | Any | `python scheduler.py` | Continu |
| **Airflow DAG** | Enterprise | Airflow UI | Configurable |
| **Docker** | Container | `docker-compose up` | Continu |

---

## 🔗 Intégration avec Safety Graph

```
Sources HSE          Pipeline               Safety Graph DB
─────────────       ──────────              ───────────────
                         │
┌─────────┐         ┌────▼────┐            ┌─────────────────┐
│ Kaggle  │────────▶│ Bronze  │            │  PostgreSQL     │
│ OSHA    │         │ (Raw)   │            │                 │
│ Eurostat│         └────┬────┘            │ ┌─────────────┐ │
│ ILOSTAT │              │                 │ │hse_incidents│ │
│ DARES   │         ┌────▼────┐            │ │  _global    │ │
│ CNESST  │────────▶│ Silver  │            │ └─────────────┘ │
└─────────┘         │(Cleaned)│            │                 │
                    └────┬────┘            │ ┌─────────────┐ │
                         │                 │ │hse_data_    │ │
                    ┌────▼────┐            │ │  sources    │ │
                    │  Gold   │───────────▶│ └─────────────┘ │
                    │(Unified)│            │                 │
                    └─────────┘            └─────────────────┘
                                                    │
                                           ┌────────▼────────┐
                                           │   AgenticX5     │
                                           │   100 Agents    │
                                           │   Dashboard     │
                                           └─────────────────┘
```

---

© 2026 AgenticX5 — GenAISafety / Preventera
