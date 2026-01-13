# 🔄 HSE Data Ingestion Package — AgenticX5

## 📦 Contenu du Package

| Fichier | Description |
|---------|-------------|
| `hse_data_ingestion.py` | Module principal avec tous les connecteurs (Kaggle, OSHA, Eurostat, ILOSTAT, DARES, etc.) |
| `postgresql_loader.py` | Chargeur PostgreSQL pour Safety Graph |
| `quickstart.py` | Script de démarrage rapide avec menu interactif |
| `requirements.txt` | Dépendances Python |
| `GUIDE_EXECUTION_HSE_PIPELINES.md` | Guide complet d'exécution multi-environnements |
| `hse_pipeline_dashboard.html` | Dashboard HTML interactif de contrôle |
| `catalogue_datasets_hse_opendata.html` | Catalogue des 47 datasets HSE identifiés |

## 🚀 Démarrage Rapide

```bash
# 1. Installer les dépendances
pip install -r requirements.txt

# 2. Configurer les variables d'environnement
export KAGGLE_USERNAME=your_username
export KAGGLE_KEY=your_key

# 3. Lancer le menu interactif
python quickstart.py

# 4. Ou exécuter directement
python hse_data_ingestion.py --list
python hse_data_ingestion.py --source kaggle_osha_injuries
python hse_data_ingestion.py --all --priority 1
```

## 📊 Sources Disponibles

- **Kaggle**: OSHA Injuries, Industrial Safety
- **USA**: OSHA Inspections/Severe Injuries, BLS CFOI/SOII
- **Europe**: Eurostat ESAW (27 pays)
- **International**: ILOSTAT (180+ pays)
- **France**: DARES, CARSAT
- **Canada**: CNESST Québec

## 🔧 Environnements Supportés

- ✅ Local (CLI Python)
- ✅ Zerve.ai (Notebooks)
- ✅ Docker/Docker Compose
- ✅ Apache Airflow
- ✅ Cron/Task Scheduler

## 📚 Documentation

Consultez `GUIDE_EXECUTION_HSE_PIPELINES.md` pour le guide complet.

---
© 2026 AgenticX5 — GenAISafety / Preventera
