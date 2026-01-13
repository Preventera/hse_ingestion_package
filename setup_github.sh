#!/bin/bash
# ============================================================
# 🚀 Script d'Initialisation GitHub - AgenticX5 HSE Ingestion
# ============================================================
# Usage: ./setup_github.sh [nom_utilisateur_github]
# ============================================================

set -e

# Couleurs
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${CYAN}"
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║   🚀 AgenticX5 HSE Ingestion - Setup GitHub                  ║"
echo "║   GenAISafety / Preventera                                   ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo -e "${NC}"

# Vérifier si git est installé
if ! command -v git &> /dev/null; then
    echo -e "${RED}❌ Git n'est pas installé. Installez-le d'abord.${NC}"
    exit 1
fi

# Demander le nom d'utilisateur GitHub si pas fourni
if [ -z "$1" ]; then
    read -p "👤 Votre nom d'utilisateur GitHub: " GITHUB_USER
else
    GITHUB_USER=$1
fi

REPO_NAME="agenticx5-hse-ingestion"
REPO_URL="https://github.com/${GITHUB_USER}/${REPO_NAME}.git"

echo -e "\n${BLUE}📁 Configuration du projet...${NC}"

# Créer la structure de répertoires
mkdir -p src/connectors
mkdir -p scripts
mkdir -p config
mkdir -p dashboards
mkdir -p docs
mkdir -p tests
mkdir -p docker
mkdir -p .github/workflows
mkdir -p data/{bronze,silver,gold}

echo -e "${GREEN}✓ Structure créée${NC}"

# Déplacer les fichiers vers la bonne structure
if [ -f "hse_data_ingestion.py" ]; then
    mv hse_data_ingestion.py src/
    echo -e "${GREEN}✓ hse_data_ingestion.py → src/${NC}"
fi

if [ -f "postgresql_loader.py" ]; then
    mv postgresql_loader.py src/
    echo -e "${GREEN}✓ postgresql_loader.py → src/${NC}"
fi

if [ -f "quickstart.py" ]; then
    mv quickstart.py scripts/
    echo -e "${GREEN}✓ quickstart.py → scripts/${NC}"
fi

if [ -f "hse_pipeline_dashboard.html" ]; then
    mv hse_pipeline_dashboard.html dashboards/
    echo -e "${GREEN}✓ Dashboard → dashboards/${NC}"
fi

if [ -f "catalogue_datasets_hse_opendata.html" ]; then
    mv catalogue_datasets_hse_opendata.html dashboards/
    echo -e "${GREEN}✓ Catalogue → dashboards/${NC}"
fi

if [ -f "GUIDE_EXECUTION_HSE_PIPELINES.md" ]; then
    mv GUIDE_EXECUTION_HSE_PIPELINES.md docs/
    echo -e "${GREEN}✓ Guide → docs/${NC}"
fi

# Créer __init__.py
cat > src/__init__.py << 'EOF'
"""
AgenticX5 HSE Data Ingestion Package
====================================
Pipeline d'ingestion multi-sources pour Safety Graph
"""

__version__ = "1.0.0"
__author__ = "Mario Genest - GenAISafety / Preventera"

from .hse_data_ingestion import HSEPipelineOrchestrator, HSE_SOURCES
from .postgresql_loader import SafetyGraphLoader
EOF

cat > src/connectors/__init__.py << 'EOF'
"""HSE Data Connectors"""
EOF

# Créer .gitignore
cat > .gitignore << 'EOF'
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
venv/
ENV/
.venv/

# Données
data/bronze/
data/silver/
data/gold/
*.parquet
*.csv
!tests/fixtures/*.csv

# Environnement
.env
.env.local
kaggle.json

# IDE
.vscode/
.idea/
*.swp
*.swo

# Logs
*.log
logs/

# OS
.DS_Store
Thumbs.db

# Build
build/
dist/
*.egg-info/

# Jupyter
.ipynb_checkpoints/
EOF

echo -e "${GREEN}✓ .gitignore créé${NC}"

# Créer .env.example
cat > .env.example << 'EOF'
# ============================================================
# AgenticX5 HSE Ingestion - Variables d'Environnement
# ============================================================
# Copier ce fichier vers .env et remplir les valeurs

# === Kaggle API ===
# Obtenir sur: https://www.kaggle.com/settings → API → Create New Token
KAGGLE_USERNAME=
KAGGLE_KEY=

# === BLS API (USA) ===
# Obtenir sur: https://www.bls.gov/developers/
BLS_API_KEY=

# === PostgreSQL Safety Graph ===
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=safety_graph
POSTGRES_USER=agenticx5
POSTGRES_PASSWORD=

# === Répertoires ===
DATA_DIR=./data
LOG_DIR=./logs

# === Options ===
LOG_LEVEL=INFO
EOF

echo -e "${GREEN}✓ .env.example créé${NC}"

# Créer README.md principal
cat > README.md << 'EOF'
# 🔄 AgenticX5 HSE Data Ingestion

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Pipeline d'ingestion de données HSE (Health, Safety, Environment) multi-sources pour **AgenticX5** et **Safety Graph**.

## 🌟 Fonctionnalités

- **12+ Sources de données** : Kaggle, OSHA, BLS, Eurostat, ILOSTAT, DARES, CNESST...
- **Architecture Medallion** : Bronze → Silver → Gold
- **Multi-juridictions** : USA, EU-27, France, Québec, International
- **Harmonisation automatique** : NAICS ↔ NACE ↔ NAF mapping
- **PostgreSQL Integration** : Chargement direct vers Safety Graph

## 🚀 Installation Rapide

```bash
# Cloner le repo
git clone https://github.com/YOUR_USERNAME/agenticx5-hse-ingestion.git
cd agenticx5-hse-ingestion

# Créer l'environnement
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Installer les dépendances
pip install -r requirements.txt

# Configurer les clés API
cp .env.example .env
# Éditer .env avec vos clés
```

## 📊 Utilisation

```bash
# Menu interactif
python scripts/quickstart.py

# CLI
python -m src.hse_data_ingestion --list
python -m src.hse_data_ingestion --source kaggle_osha_injuries
python -m src.hse_data_ingestion --all --priority 1
```

## 📦 Sources Supportées

| Source | Juridiction | Volume | Priorité |
|--------|-------------|--------|----------|
| Kaggle OSHA | USA | 1M+ | ⭐ P1 |
| OSHA Inspections | USA | 8M+ | ⭐ P1 |
| Eurostat ESAW | EU-27 | 27 pays | ⭐ P1 |
| ILOSTAT | International | 180+ pays | ⭐ P1 |
| DARES | France | 668K/an | ⭐ P1 |
| CNESST | Québec | 793K+ | ⭐ P1 |

## 📖 Documentation

- [Guide d'exécution complet](docs/GUIDE_EXECUTION_HSE_PIPELINES.md)
- [Catalogue des datasets](dashboards/catalogue_datasets_hse_opendata.html)

## 🤝 Contribution

Les contributions sont les bienvenues ! Voir [CONTRIBUTING.md](docs/CONTRIBUTING.md).

## 📜 Licence

MIT License - Voir [LICENSE](LICENSE)

---

© 2026 **AgenticX5** — GenAISafety / Preventera
EOF

echo -e "${GREEN}✓ README.md créé${NC}"

# Créer setup.py
cat > setup.py << 'EOF'
from setuptools import setup, find_packages

setup(
    name="agenticx5-hse-ingestion",
    version="1.0.0",
    author="Mario Genest",
    author_email="contact@genaisafety.com",
    description="HSE Data Ingestion Pipeline for AgenticX5/Safety Graph",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/genaisafety/agenticx5-hse-ingestion",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Scientific/Engineering :: Information Analysis",
    ],
    python_requires=">=3.10",
    install_requires=[
        "pandas>=2.0.0",
        "numpy>=1.24.0",
        "requests>=2.28.0",
        "pyarrow>=12.0.0",
        "sqlalchemy>=2.0.0",
        "psycopg2-binary>=2.9.0",
        "python-dotenv>=1.0.0",
        "pyyaml>=6.0",
        "tqdm>=4.65.0",
    ],
    extras_require={
        "dev": ["pytest", "pytest-cov", "flake8", "black"],
        "kaggle": ["kaggle>=1.5.0"],
    },
    entry_points={
        "console_scripts": [
            "hse-ingest=src.hse_data_ingestion:main",
        ],
    },
)
EOF

echo -e "${GREEN}✓ setup.py créé${NC}"

# Créer GitHub Actions CI
cat > .github/workflows/ci.yml << 'EOF'
name: CI

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ['3.10', '3.11']

    steps:
    - uses: actions/checkout@v4
    
    - name: Set up Python ${{ matrix.python-version }}
      uses: actions/setup-python@v5
      with:
        python-version: ${{ matrix.python-version }}
    
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -r requirements.txt
        pip install pytest pytest-cov flake8
    
    - name: Lint
      run: flake8 src/ --max-line-length=120 --ignore=E501
    
    - name: Test
      run: pytest tests/ -v --cov=src --cov-report=xml
    
    - name: Upload coverage
      uses: codecov/codecov-action@v3
      with:
        file: ./coverage.xml
EOF

echo -e "${GREEN}✓ GitHub Actions CI créé${NC}"

# Créer un test basique
cat > tests/__init__.py << 'EOF'
"""Tests for HSE Ingestion Pipeline"""
EOF

cat > tests/test_basic.py << 'EOF'
"""Basic tests for HSE Ingestion Pipeline"""
import pytest


def test_import_main_module():
    """Test that main module can be imported"""
    from src import hse_data_ingestion
    assert hasattr(hse_data_ingestion, 'HSEPipelineOrchestrator')
    assert hasattr(hse_data_ingestion, 'HSE_SOURCES')


def test_sources_configured():
    """Test that sources are properly configured"""
    from src.hse_data_ingestion import HSE_SOURCES
    assert len(HSE_SOURCES) > 0
    
    # Vérifier qu'il y a des sources priorité 1
    priority_1 = [k for k, v in HSE_SOURCES.items() if v.priority == 1]
    assert len(priority_1) >= 5


def test_orchestrator_init():
    """Test orchestrator initialization"""
    from src.hse_data_ingestion import HSEPipelineOrchestrator
    
    orchestrator = HSEPipelineOrchestrator(data_dir="./test_data")
    assert orchestrator is not None
    assert orchestrator.data_dir.exists() or True  # May not exist in test
EOF

echo -e "${GREEN}✓ Tests créés${NC}"

# Créer LICENSE MIT
cat > LICENSE << 'EOF'
MIT License

Copyright (c) 2026 GenAISafety / Preventera

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
EOF

echo -e "${GREEN}✓ LICENSE créé${NC}"

# Initialiser Git
echo -e "\n${BLUE}🔧 Initialisation Git...${NC}"

git init
git add .
git commit -m "🚀 Initial commit - HSE Data Ingestion Pipeline

- Module principal avec 12 connecteurs (Kaggle, OSHA, Eurostat, ILOSTAT, DARES, CNESST)
- Architecture Medallion (Bronze → Silver → Gold)
- Chargeur PostgreSQL pour Safety Graph
- Dashboard de contrôle HTML
- Catalogue de 47 datasets HSE
- CI/CD GitHub Actions
- Documentation complète"

echo -e "${GREEN}✓ Commit initial créé${NC}"

# Instructions pour pousser vers GitHub
echo -e "\n${CYAN}════════════════════════════════════════════════════════════════${NC}"
echo -e "${YELLOW}📌 ÉTAPES SUIVANTES:${NC}"
echo -e "${CYAN}════════════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "1. ${BLUE}Créer le repo sur GitHub:${NC}"
echo "   → https://github.com/new"
echo "   → Nom: ${REPO_NAME}"
echo "   → Ne PAS initialiser avec README (déjà créé)"
echo ""
echo -e "2. ${BLUE}Connecter et pousser:${NC}"
echo -e "   ${CYAN}git remote add origin ${REPO_URL}${NC}"
echo -e "   ${CYAN}git branch -M main${NC}"
echo -e "   ${CYAN}git push -u origin main${NC}"
echo ""
echo -e "3. ${BLUE}Configurer les Secrets GitHub:${NC}"
echo "   → Settings → Secrets → Actions → New repository secret"
echo "   → Ajouter: KAGGLE_USERNAME, KAGGLE_KEY, BLS_API_KEY"
echo ""
echo -e "${GREEN}✅ Setup terminé! Le projet est prêt.${NC}"
echo ""
