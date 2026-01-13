# SafeTwin X5 - Intégration Safety Graph

## 📊 Documentation d'Architecture et d'Intégration

**Version:** 1.0.0  
**Date:** 13 janvier 2026  
**Auteur:** AgenticX5 / Preventera

---

## 1. Vue d'ensemble

### 1.1 Objectif

Cette documentation décrit l'intégration des **2,853,583 records HSE** du Safety Graph dans la plateforme **SafeTwin X5** pour enrichir le jumeau numérique avec des données statistiques réelles multi-juridictionnelles.

### 1.2 Sources de données intégrées

| Source | Records | Juridiction | Période | Classification |
|--------|---------|-------------|---------|----------------|
| 🇺🇸 OSHA USA | 1,635,164 | États-Unis | 2016-2021 | NAICS |
| 🇪🇺 Eurostat EU-27 | 424,682 | 27 pays UE | 2010-2022 | NACE |
| 🇨🇦 CNESST Québec | 793,737 | Québec | 2017-2023 | SCIAN |
| **TOTAL** | **2,853,583** | Multi-juridictions | 2010-2023 | Harmonisé |

---

## 2. Architecture d'intégration

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ARCHITECTURE SAFETWIN X5 + SAFETY GRAPH                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    SAFETY GRAPH (PostgreSQL)                        │   │
│  │                    2,853,583 records HSE                            │   │
│  │                                                                     │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                 │   │
│  │  │osha_injuries│  │eurostat_esaw│  │cnesst_lesions│                 │   │
│  │  │   _raw      │  │             │  │   _quebec   │                 │   │
│  │  │ 1,635,164   │  │   424,682   │  │   793,737   │                 │   │
│  │  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘                 │   │
│  │         │                │                │                        │   │
│  │         └────────────────┼────────────────┘                        │   │
│  │                          │                                         │   │
│  │  ┌───────────────────────▼───────────────────────────────────┐    │   │
│  │  │              VUES ANALYTIQUES (SQL)                       │    │   │
│  │  │  • safetwin_sector_risk_profile                           │    │   │
│  │  │  • safetwin_cnesst_tendances                              │    │   │
│  │  │  • safetwin_global_summary                                │    │   │
│  │  └───────────────────────┬───────────────────────────────────┘    │   │
│  └──────────────────────────┼────────────────────────────────────────┘   │
│                             │                                            │
│                             ▼                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐ │
│  │              MODULE D'ENRICHISSEMENT (Python)                       │ │
│  │              safetwin_enrichment.py                                 │ │
│  │                                                                     │ │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                 │ │
│  │  │get_sector_  │  │benchmark_   │  │hugo_hazard_ │                 │ │
│  │  │risk_profile │  │sector       │  │mapper_enrich│                 │ │
│  │  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘                 │ │
│  │         │                │                │                        │ │
│  │         └────────────────┼────────────────┘                        │ │
│  │                          │                                         │ │
│  └──────────────────────────┼─────────────────────────────────────────┘ │
│                             │                                            │
│                             ▼                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐ │
│  │                    SAFETWIN X5 PLATFORM                             │ │
│  │                                                                     │ │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                 │ │
│  │  │ SafetyGraph │  │ Agents HUGO │  │  SafetyHub  │                 │ │
│  │  │   (Neo4j)   │  │ (IA proact.)│  │ (Dashboard) │                 │ │
│  │  │             │  │             │  │             │                 │ │
│  │  │ Dangers ←─→ │  │ HazardMapper│  │ Vue 3D      │                 │ │
│  │  │ Contrôles   │  │ Benchmark   │  │ Alertes     │                 │ │
│  │  │ SOP/Preuves │  │ TrendAnalyst│  │ Rapports    │                 │ │
│  │  └─────────────┘  └─────────────┘  └─────────────┘                 │ │
│  └─────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Composants livrés

### 3.1 Vues SQL analytiques

**Fichier:** `safetwin_analytics_views.sql`

| Vue | Description | Usage |
|-----|-------------|-------|
| `safetwin_cnesst_sector_stats` | Stats par secteur SCIAN et année | Tableaux de bord |
| `safetwin_cnesst_top_sectors` | Top secteurs par volume | Priorisation |
| `safetwin_cnesst_tendances` | Tendances 2017-2023 | Graphiques |
| `safetwin_sector_risk_profile` | Profil de risque par secteur | Agents HUGO |
| `safetwin_global_summary` | Résumé multi-juridictionnel | KPIs |

**Fonctions SQL:**
- `get_sector_risk_profile(secteur)` - Profil complet d'un secteur
- `compare_to_provincial_avg(secteur, indicateur)` - Benchmarking

### 3.2 Module Python d'enrichissement

**Fichier:** `safetwin_enrichment.py`

```python
from safetwin_enrichment import SafeTwinEnricher

# Initialisation
enricher = SafeTwinEnricher()

# Obtenir le profil de risque d'un secteur
profile = enricher.get_sector_risk_profile("CONSTRUCTION")

# Benchmarking vs moyenne provinciale
benchmark = enricher.benchmark_sector("CONSTRUCTION", "TMS")

# Enrichissement complet pour SafeTwin
enrichment = enricher.get_full_enrichment("CONSTRUCTION")

# Données pour les agents HUGO
hazard_data = enricher.hugo_hazard_mapper_enrichment("CONSTRUCTION")
benchmark_data = enricher.hugo_benchmark_agent_enrichment("CONSTRUCTION")
trend_data = enricher.hugo_trend_analyst_enrichment("CONSTRUCTION")

# Générer un rapport
report = enricher.generate_sector_report("CONSTRUCTION")
print(report)

# Export JSON pour l'API
json_data = enricher.export_to_json("CONSTRUCTION")
```

**CLI disponible:**
```bash
# Lister tous les secteurs
python safetwin_enrichment.py --list

# Stats globales
python safetwin_enrichment.py --stats

# Rapport secteur
python safetwin_enrichment.py --secteur "CONSTRUCTION"

# Export JSON
python safetwin_enrichment.py --secteur "CONSTRUCTION" --json

# Données pour agent HUGO
python safetwin_enrichment.py --secteur "CONSTRUCTION" --hugo hazard
```

### 3.3 Dashboard HTML interactif

**Fichier:** `safetwin_dashboard.html`

Fonctionnalités:
- Vue des 3 sources de données (OSHA, Eurostat, CNESST)
- Graphique de répartition (donut chart)
- Tendances 2017-2023 (line chart)
- Top 10 secteurs SCIAN avec indicateurs TMS/PSY
- Modal de détail par secteur
- Design glassmorphism dark mode
- Animations et transitions fluides

---

## 4. Schéma des données CNESST

### 4.1 Colonnes disponibles

| Colonne | Type | Description |
|---------|------|-------------|
| `ID` | INTEGER | Identifiant unique |
| `NATURE_LESION` | TEXT | Type de blessure |
| `SIEGE_LESION` | TEXT | Partie du corps affectée |
| `GENRE` | TEXT | Type d'événement/accident |
| `AGENT_CAUSAL_LESION` | TEXT | Cause de la lésion |
| `SEXE_PERS_PHYS` | TEXT | Sexe de la personne |
| `GROUPE_AGE` | TEXT | Groupe d'âge |
| `SECTEUR_SCIAN` | TEXT | Secteur d'activité (SCIAN) |
| `IND_LESION_TMS` | TEXT | Indicateur TMS (OUI/NON) |
| `IND_LESION_PSY` | TEXT | Indicateur psychologique |
| `IND_LESION_COVID_19` | TEXT | Indicateur COVID-19 |
| `IND_LESION_SURDITE` | TEXT | Indicateur surdité |
| `IND_LESION_MACHINE` | TEXT | Indicateur machine |
| `ANNEE` | INTEGER | Année (2017-2023) |

### 4.2 Principaux secteurs SCIAN

| Code | Secteur | Volume |
|------|---------|--------|
| 62 | Soins de santé et assistance sociale | 243,521 (30.7%) |
| 31-33D | Fabrication - Biens durables | 78,432 (9.9%) |
| 23 | Construction | 71,254 (9.0%) |
| 31-33N | Fabrication - Biens non durables | 56,123 (7.1%) |
| 44-45 | Commerce de détail | 54,876 (6.9%) |
| 48-49 | Transport et entreposage | 43,521 (5.5%) |

---

## 5. Intégration avec les Agents HUGO

### 5.1 HazardMapper

L'agent HazardMapper utilise les données pour identifier les dangers prioritaires:

```python
hazard_data = enricher.hugo_hazard_mapper_enrichment("SOINS DE SANTE")

# Retourne:
{
    "secteur": "SOINS DE SANTE ET ASSISTANCE SOCIALE",
    "risk_level": "🔴 CRITIQUE",
    "risk_score": 72.5,
    "dangers_prioritaires": [
        {"type": "TMS", "taux": 20.5, "priorite": 1},
        {"type": "PSY", "taux": 6.7, "priorite": 1},
        {"type": "MACHINE", "taux": 0.3, "priorite": 3}
    ],
    "zones_a_risque": [...],
    "agents_causaux": [...]
}
```

### 5.2 BenchmarkAgent

Compare les performances du client vs la moyenne provinciale:

```python
benchmark_data = enricher.hugo_benchmark_agent_enrichment("CONSTRUCTION")

# Retourne:
{
    "secteur": "CONSTRUCTION",
    "benchmarks": [
        {
            "indicateur": "TMS",
            "taux_secteur": 25.8,
            "taux_provincial": 23.5,
            "ecart": 2.3,
            "statut": "⚠️ AU-DESSUS",
            "recommandation": "Action prioritaire requise..."
        },
        ...
    ],
    "score_global": 50.0
}
```

### 5.3 TrendAnalyst

Analyse les tendances sur 7 ans:

```python
trend_data = enricher.hugo_trend_analyst_enrichment("CONSTRUCTION")

# Retourne:
{
    "secteur": "CONSTRUCTION",
    "tendances": {2017: 8234, 2018: 9102, ...},
    "variation_pct": 16.5,
    "tendance": "📈 HAUSSE",
    "analyse": "Variation de +16.5% sur 7 ans"
}
```

---

## 6. Installation et déploiement

### 6.1 Prérequis

- PostgreSQL 14+
- Python 3.10+
- Packages: `pandas`, `sqlalchemy`, `psycopg2-binary`

### 6.2 Installation des vues SQL

```bash
# Connexion à PostgreSQL
psql -d safety_graph -U postgres

# Exécuter le script
\i safetwin_analytics_views.sql
```

### 6.3 Test du module Python

```bash
# Vérifier la connexion
python safetwin_enrichment.py --stats

# Tester un secteur
python safetwin_enrichment.py --secteur "CONSTRUCTION"
```

### 6.4 Déployer le dashboard

```bash
# Option 1: Ouvrir localement
start safetwin_dashboard.html

# Option 2: Déployer sur Netlify
netlify deploy --prod
```

---

## 7. API REST (future)

### 7.1 Endpoints prévus

| Endpoint | Méthode | Description |
|----------|---------|-------------|
| `/api/v1/sectors` | GET | Liste des secteurs |
| `/api/v1/sectors/{scian}/profile` | GET | Profil de risque |
| `/api/v1/sectors/{scian}/benchmark` | GET | Benchmarking |
| `/api/v1/sectors/{scian}/trends` | GET | Tendances |
| `/api/v1/stats/global` | GET | Stats globales |

### 7.2 Exemple de réponse

```json
GET /api/v1/sectors/CONSTRUCTION/profile

{
    "secteur_scian": "CONSTRUCTION",
    "total_lesions": 71254,
    "pct_total_qc": 9.0,
    "risk_score": 58.3,
    "risk_level": "ÉLEVÉ",
    "risk_tms_pct": 25.8,
    "risk_psy_pct": 0.7,
    "top_nature": "BLES. TRAUMA. MUSCLES,TENDONS,ETC.",
    "top_siege": "DOS",
    "top_agent": "EFFORT EXCESSIF"
}
```

---

## 8. Roadmap

### Phase 1 - MVP (✅ Complété)
- [x] Pipeline d'ingestion HSE
- [x] Chargement OSHA, Eurostat, CNESST
- [x] Vues SQL analytiques
- [x] Module Python d'enrichissement
- [x] Dashboard HTML interactif
- [x] Documentation

### Phase 2 - Intégration SafeTwin (Q1 2026)
- [ ] Connexion au SafetyGraph Neo4j
- [ ] API REST FastAPI
- [ ] Intégration agents HUGO
- [ ] Dashboard React intégré

### Phase 3 - Production (Q2 2026)
- [ ] Multi-tenant
- [ ] Caching Redis
- [ ] Monitoring et alertes
- [ ] Rapports PDF automatiques

---

## 9. Support

**Preventera Inc.**
- Web: [preventera.com](https://preventera.com)
- Email: support@preventera.com

**AgenticX5**
- Web: [agenticx5.com](https://agenticx5.com)
- GitHub: [github.com/Preventera](https://github.com/Preventera)

---

## 10. Changelog

### v1.0.0 (2026-01-13)
- Initial release
- 2,853,583 records intégrés
- Vues SQL, module Python, dashboard HTML
- Documentation complète

---

*Document généré par AgenticX5 Safety Graph Pipeline*
