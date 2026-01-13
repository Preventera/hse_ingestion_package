# 🚀 Guide d'Installation et GitHub — HSE Ingestion AgenticX5

## 📥 PARTIE 1: Exécuter le ZIP Localement

### Étape 1: Extraire le ZIP

**Windows:**
```powershell
# Clic droit sur hse_ingestion_agenticx5.zip → "Extraire tout..."
# OU en PowerShell:
Expand-Archive -Path hse_ingestion_agenticx5.zip -DestinationPath C:\AgenticX5\hse-ingestion
cd C:\AgenticX5\hse-ingestion\hse_ingestion_package
```

**Mac/Linux:**
```bash
# Extraire
unzip hse_ingestion_agenticx5.zip -d ~/agenticx5-hse
cd ~/agenticx5-hse/hse_ingestion_package
```

### Étape 2: Créer l'environnement Python

```bash
# Créer un environnement virtuel
python -m venv venv

# Activer l'environnement
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate

# Installer les dépendances
pip install -r requirements.txt
```

### Étape 3: Configurer les clés API

**Créer un fichier `.env`:**
```bash
# .env
KAGGLE_USERNAME=votre_username_kaggle
KAGGLE_KEY=votre_api_key_kaggle
BLS_API_KEY=votre_cle_bls

# PostgreSQL (optionnel)
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=safety_graph
POSTGRES_USER=agenticx5
POSTGRES_PASSWORD=votre_mot_de_passe
```

**Pour Kaggle:**
1. Aller sur https://www.kaggle.com/settings
2. Section "API" → "Create New Token"
3. Télécharger `kaggle.json`
4. Copier les valeurs dans `.env`

### Étape 4: Exécuter le pipeline

```bash
# Option A: Menu interactif (recommandé pour débuter)
python quickstart.py

# Option B: Ligne de commande
python hse_data_ingestion.py --list                    # Voir les sources
python hse_data_ingestion.py --source kaggle_osha_injuries  # Une source
python hse_data_ingestion.py --all --priority 1        # Toutes priorité 1

# Option C: Ouvrir le dashboard HTML
# Double-cliquer sur hse_pipeline_dashboard.html
```

---

## 🐙 PARTIE 2: Créer le Repository GitHub

### OUI, vous devriez créer un GitHub pour:
- ✅ Versionner le code
- ✅ Collaborer avec l'équipe SquadrAI
- ✅ Activer CI/CD automatique
- ✅ Documenter le projet
- ✅ Partager avec la communauté HSE

### Étape 1: Créer le repo sur GitHub

1. Aller sur https://github.com/new
2. **Repository name:** `agenticx5-hse-ingestion`
3. **Description:** `Pipeline d'ingestion de données HSE multi-sources pour AgenticX5/Safety Graph`
4. **Visibility:** Private (ou Public si open source)
5. ✅ Add a README file
6. ✅ Add .gitignore → Python
7. **License:** MIT ou Apache 2.0
8. Cliquer "Create repository"

### Étape 2: Structure recommandée du repo

```
agenticx5-hse-ingestion/
│
├── 📄 README.md                    # Documentation principale
├── 📄 LICENSE                      # Licence open source
├── 📄 .gitignore                   # Fichiers à ignorer
├── 📄 .env.example                 # Template variables d'environnement
├── 📄 requirements.txt             # Dépendances Python
├── 📄 setup.py                     # Installation package
├── 📄 pyproject.toml               # Config moderne Python
│
├── 📁 src/                         # Code source
│   ├── __init__.py
│   ├── hse_data_ingestion.py       # Module principal
│   ├── postgresql_loader.py        # Chargeur PostgreSQL
│   └── connectors/                 # Connecteurs par source
│       ├── __init__.py
│       ├── kaggle_connector.py
│       ├── osha_connector.py
│       ├── eurostat_connector.py
│       └── ...
│
├── 📁 scripts/                     # Scripts utilitaires
│   ├── quickstart.py
│   └── scheduler.py
│
├── 📁 config/                      # Configuration
│   ├── sources.yaml
│   └── logging.yaml
│
├── 📁 dashboards/                  # Interfaces visuelles
│   ├── hse_pipeline_dashboard.html
│   └── catalogue_datasets.html
│
├── 📁 docs/                        # Documentation
│   ├── GUIDE_EXECUTION.md
│   ├── API_REFERENCE.md
│   └── CONTRIBUTING.md
│
├── 📁 tests/                       # Tests unitaires
│   ├── __init__.py
│   ├── test_connectors.py
│   └── test_pipeline.py
│
├── 📁 docker/                      # Docker
│   ├── Dockerfile
│   └── docker-compose.yml
│
└── 📁 .github/                     # GitHub Actions CI/CD
    └── workflows/
        ├── ci.yml
        └── scheduled_ingestion.yml
```

### Étape 3: Initialiser Git localement

```bash
# Dans le dossier du projet extrait
cd ~/agenticx5-hse/hse_ingestion_package

# Initialiser Git
git init

# Ajouter le remote GitHub
git remote add origin https://github.com/VOTRE_USERNAME/agenticx5-hse-ingestion.git

# Créer la branche main
git branch -M main

# Ajouter les fichiers
git add .

# Premier commit
git commit -m "🚀 Initial commit - HSE Data Ingestion Pipeline"

# Pousser vers GitHub
git push -u origin main
```

### Étape 4: Fichiers à ajouter

**`.gitignore`:**
```gitignore
# Environnement
venv/
.env
*.pyc
__pycache__/

# Données
data/
*.parquet
*.csv

# IDE
.vscode/
.idea/

# Logs
*.log
logs/

# Kaggle
kaggle.json

# OS
.DS_Store
Thumbs.db
```

**`.env.example`:**
```bash
# Copier ce fichier vers .env et remplir les valeurs

# Kaggle API (https://www.kaggle.com/settings)
KAGGLE_USERNAME=
KAGGLE_KEY=

# BLS API (https://www.bls.gov/developers/)
BLS_API_KEY=

# PostgreSQL Safety Graph
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=safety_graph
POSTGRES_USER=
POSTGRES_PASSWORD=

# Répertoire de données
DATA_DIR=./data
```

---

## 🔄 PARTIE 3: GitHub Actions (CI/CD Automatique)

### Workflow CI (Tests automatiques)

Créer `.github/workflows/ci.yml`:
```yaml
name: CI - HSE Ingestion Pipeline

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v4
    
    - name: Set up Python
      uses: actions/setup-python@v5
      with:
        python-version: '3.11'
    
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -r requirements.txt
        pip install pytest pytest-cov
    
    - name: Run tests
      run: |
        pytest tests/ -v --cov=src
    
    - name: Lint with flake8
      run: |
        pip install flake8
        flake8 src/ --max-line-length=120
```

### Workflow Scheduled (Ingestion planifiée)

Créer `.github/workflows/scheduled_ingestion.yml`:
```yaml
name: Scheduled HSE Ingestion

on:
  schedule:
    # Tous les dimanches à 2h UTC
    - cron: '0 2 * * 0'
  workflow_dispatch:  # Déclenchement manuel

jobs:
  ingest:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v4
    
    - name: Set up Python
      uses: actions/setup-python@v5
      with:
        python-version: '3.11'
    
    - name: Install dependencies
      run: pip install -r requirements.txt
    
    - name: Run ingestion pipeline
      env:
        KAGGLE_USERNAME: ${{ secrets.KAGGLE_USERNAME }}
        KAGGLE_KEY: ${{ secrets.KAGGLE_KEY }}
        BLS_API_KEY: ${{ secrets.BLS_API_KEY }}
      run: |
        python src/hse_data_ingestion.py --all --priority 2 --report
    
    - name: Upload artifacts
      uses: actions/upload-artifact@v4
      with:
        name: ingestion-report
        path: data/ingestion_report_*.json
```

### Configurer les Secrets GitHub

1. Aller dans Settings → Secrets and variables → Actions
2. Ajouter les secrets:
   - `KAGGLE_USERNAME`
   - `KAGGLE_KEY`
   - `BLS_API_KEY`
   - `POSTGRES_PASSWORD` (si déploiement cloud)

---

## 📋 Résumé des Commandes

```bash
# === INSTALLATION LOCALE ===
unzip hse_ingestion_agenticx5.zip
cd hse_ingestion_package
python -m venv venv
source venv/bin/activate  # ou venv\Scripts\activate sur Windows
pip install -r requirements.txt
cp .env.example .env      # Éditer avec vos clés

# === EXÉCUTION ===
python quickstart.py                               # Menu interactif
python hse_data_ingestion.py --list                # Lister sources
python hse_data_ingestion.py --all --priority 1    # Exécuter

# === GITHUB ===
git init
git remote add origin https://github.com/USER/agenticx5-hse-ingestion.git
git add .
git commit -m "🚀 Initial commit"
git push -u origin main
```

---

## ❓ FAQ

**Q: Dois-je absolument créer un GitHub?**
R: Non, mais c'est fortement recommandé pour la collaboration, le versioning et l'automatisation.

**Q: Le repo doit-il être public ou privé?**
R: Privé si données sensibles, Public si vous voulez contribuer à la communauté HSE open source.

**Q: Puis-je utiliser GitLab ou Bitbucket?**
R: Oui, les workflows CI/CD sont similaires (GitLab CI, Bitbucket Pipelines).

**Q: Comment intégrer avec Zerve.ai?**
R: Cloner le repo dans Zerve et importer les modules dans vos notebooks.

---

© 2026 AgenticX5 — GenAISafety / Preventera
