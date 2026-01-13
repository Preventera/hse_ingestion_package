# ============================================================
# 🚀 Script d'Initialisation GitHub - AgenticX5 HSE Ingestion
# Version Windows PowerShell
# ============================================================
# Usage: .\setup_github.ps1 -GitHubUser "votre_username"
# ============================================================

param(
    [string]$GitHubUser = ""
)

$ErrorActionPreference = "Stop"

# Couleurs
function Write-ColorOutput($ForegroundColor) {
    $fc = $host.UI.RawUI.ForegroundColor
    $host.UI.RawUI.ForegroundColor = $ForegroundColor
    if ($args) { Write-Output $args }
    $host.UI.RawUI.ForegroundColor = $fc
}

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║   🚀 AgenticX5 HSE Ingestion - Setup GitHub (Windows)        ║" -ForegroundColor Cyan
Write-Host "║   GenAISafety / Preventera                                   ║" -ForegroundColor Cyan
Write-Host "╚══════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# Vérifier Git
try {
    git --version | Out-Null
} catch {
    Write-Host "❌ Git n'est pas installé. Téléchargez-le sur https://git-scm.com/" -ForegroundColor Red
    exit 1
}

# Demander le username si pas fourni
if ([string]::IsNullOrEmpty($GitHubUser)) {
    $GitHubUser = Read-Host "👤 Votre nom d'utilisateur GitHub"
}

$RepoName = "agenticx5-hse-ingestion"
$RepoUrl = "https://github.com/$GitHubUser/$RepoName.git"

Write-Host "`n📁 Configuration du projet..." -ForegroundColor Blue

# Créer la structure
$dirs = @(
    "src\connectors",
    "scripts",
    "config",
    "dashboards",
    "docs",
    "tests",
    "docker",
    ".github\workflows",
    "data\bronze",
    "data\silver",
    "data\gold"
)

foreach ($dir in $dirs) {
    New-Item -ItemType Directory -Path $dir -Force | Out-Null
}
Write-Host "✓ Structure créée" -ForegroundColor Green

# Déplacer les fichiers
$moves = @{
    "hse_data_ingestion.py" = "src\"
    "postgresql_loader.py" = "src\"
    "quickstart.py" = "scripts\"
    "hse_pipeline_dashboard.html" = "dashboards\"
    "catalogue_datasets_hse_opendata.html" = "dashboards\"
    "GUIDE_EXECUTION_HSE_PIPELINES.md" = "docs\"
}

foreach ($file in $moves.Keys) {
    if (Test-Path $file) {
        Move-Item $file $moves[$file] -Force
        Write-Host "✓ $file déplacé" -ForegroundColor Green
    }
}

# Créer __init__.py
@"
"""
AgenticX5 HSE Data Ingestion Package
====================================
Pipeline d'ingestion multi-sources pour Safety Graph
"""

__version__ = "1.0.0"
__author__ = "Mario Genest - GenAISafety / Preventera"

from .hse_data_ingestion import HSEPipelineOrchestrator, HSE_SOURCES
from .postgresql_loader import SafetyGraphLoader
"@ | Out-File -FilePath "src\__init__.py" -Encoding UTF8

"" | Out-File -FilePath "src\connectors\__init__.py" -Encoding UTF8

# Créer .gitignore
@"
# Python
__pycache__/
*.py[cod]
venv/
.venv/

# Données
data/bronze/
data/silver/
data/gold/
*.parquet
*.csv

# Environnement
.env
kaggle.json

# IDE
.vscode/
.idea/

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
"@ | Out-File -FilePath ".gitignore" -Encoding UTF8
Write-Host "✓ .gitignore créé" -ForegroundColor Green

# Créer .env.example
@"
# AgenticX5 HSE Ingestion - Variables d'Environnement
# Copier vers .env et remplir les valeurs

# Kaggle API
KAGGLE_USERNAME=
KAGGLE_KEY=

# BLS API
BLS_API_KEY=

# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=safety_graph
POSTGRES_USER=agenticx5
POSTGRES_PASSWORD=

# Répertoires
DATA_DIR=./data
"@ | Out-File -FilePath ".env.example" -Encoding UTF8
Write-Host "✓ .env.example créé" -ForegroundColor Green

# Créer README.md
@"
# 🔄 AgenticX5 HSE Data Ingestion

Pipeline d'ingestion de données HSE multi-sources pour **AgenticX5** et **Safety Graph**.

## 🚀 Installation Rapide

```powershell
# Cloner
git clone $RepoUrl
cd $RepoName

# Environnement
python -m venv venv
.\venv\Scripts\Activate

# Dépendances
pip install -r requirements.txt

# Configuration
copy .env.example .env
# Éditer .env avec vos clés
```

## 📊 Utilisation

```powershell
python scripts\quickstart.py
python -m src.hse_data_ingestion --list
python -m src.hse_data_ingestion --all --priority 1
```

## 📦 Sources Supportées

- Kaggle OSHA (1M+ records)
- OSHA Inspections (8M+ records)
- Eurostat ESAW (27 pays EU)
- ILOSTAT (180+ pays)
- DARES France (668K/an)
- CNESST Québec (793K+)

---
© 2026 AgenticX5 — GenAISafety / Preventera
"@ | Out-File -FilePath "README.md" -Encoding UTF8
Write-Host "✓ README.md créé" -ForegroundColor Green

# Créer test basique
@"
import pytest

def test_import():
    from src import hse_data_ingestion
    assert hasattr(hse_data_ingestion, 'HSEPipelineOrchestrator')
"@ | Out-File -FilePath "tests\test_basic.py" -Encoding UTF8

"" | Out-File -FilePath "tests\__init__.py" -Encoding UTF8
Write-Host "✓ Tests créés" -ForegroundColor Green

# Initialiser Git
Write-Host "`n🔧 Initialisation Git..." -ForegroundColor Blue

git init
git add .
git commit -m "🚀 Initial commit - HSE Data Ingestion Pipeline"

Write-Host "✓ Commit initial créé" -ForegroundColor Green

# Instructions finales
Write-Host ""
Write-Host "════════════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "📌 ÉTAPES SUIVANTES:" -ForegroundColor Yellow
Write-Host "════════════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""
Write-Host "1. Créer le repo sur GitHub:" -ForegroundColor Blue
Write-Host "   → https://github.com/new"
Write-Host "   → Nom: $RepoName"
Write-Host "   → Ne PAS initialiser avec README"
Write-Host ""
Write-Host "2. Connecter et pousser:" -ForegroundColor Blue
Write-Host "   git remote add origin $RepoUrl" -ForegroundColor Cyan
Write-Host "   git branch -M main" -ForegroundColor Cyan
Write-Host "   git push -u origin main" -ForegroundColor Cyan
Write-Host ""
Write-Host "3. Configurer les Secrets GitHub:" -ForegroundColor Blue
Write-Host "   → Settings → Secrets → Actions"
Write-Host "   → Ajouter: KAGGLE_USERNAME, KAGGLE_KEY"
Write-Host ""
Write-Host "✅ Setup terminé!" -ForegroundColor Green
Write-Host ""
