#!/usr/bin/env python3
"""
============================================================================
SAFETWIN X5 - MODULE D'ENRICHISSEMENT SAFETY GRAPH
============================================================================
Ce module connecte SafeTwin X5 aux 2,853,583 records HSE pour enrichir
le jumeau numérique avec des données statistiques réelles.

Sources intégrées:
- OSHA USA: 1,635,164 records (2016-2021)
- Eurostat EU-27: 424,682 records (2010-2022)  
- CNESST Québec: 793,737 records (2017-2023)

Version: 1.0.0
Date: 2026-01-13
Auteur: AgenticX5 / Preventera
============================================================================
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
import json
import logging

# Configuration logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger('SafeTwinEnrichment')


# ============================================================================
# CONFIGURATION
# ============================================================================

@dataclass
class SafetyGraphConfig:
    """Configuration de connexion à Safety Graph"""
    host: str = "localhost"
    port: int = 5432
    database: str = "safety_graph"
    user: str = "postgres"
    password: str = "postgres"
    
    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


# ============================================================================
# CLASSES DE DONNÉES
# ============================================================================

@dataclass
class SectorRiskProfile:
    """Profil de risque d'un secteur SCIAN"""
    secteur_scian: str
    total_lesions: int
    pct_total_qc: float
    risk_tms_pct: float
    risk_psy_pct: float
    risk_machine_pct: float
    risk_surdite_pct: float
    risk_score: float
    risk_level: str
    top_nature_lesion: Optional[str] = None
    top_siege_lesion: Optional[str] = None
    top_agent_causal: Optional[str] = None
    tendance_7_ans: Optional[str] = None


@dataclass
class BenchmarkResult:
    """Résultat de benchmarking vs moyenne provinciale"""
    secteur: str
    indicateur: str
    taux_secteur: float
    taux_provincial: float
    ecart: float
    ecart_pct: float
    statut: str
    recommandation: str


@dataclass
class SafeTwinEnrichment:
    """Données d'enrichissement pour SafeTwin X5"""
    secteur_scian: str
    risk_profile: SectorRiskProfile
    benchmarks: List[BenchmarkResult]
    tendances: Dict[int, int]
    top_5_natures: List[Dict[str, Any]]
    top_5_sieges: List[Dict[str, Any]]
    top_5_agents: List[Dict[str, Any]]
    recommandations_ia: List[str]


# ============================================================================
# CLASSE PRINCIPALE
# ============================================================================

class SafeTwinEnricher:
    """
    Enrichisseur SafeTwin X5 avec les données Safety Graph.
    
    Utilisation:
        enricher = SafeTwinEnricher()
        profile = enricher.get_sector_risk_profile("SOINS DE SANTE ET ASSISTANCE SOCIALE")
        enrichment = enricher.get_full_enrichment("CONSTRUCTION")
    """
    
    def __init__(self, config: Optional[SafetyGraphConfig] = None):
        """Initialise la connexion à Safety Graph"""
        self.config = config or SafetyGraphConfig()
        self.engine = create_engine(self.config.connection_string)
        logger.info(f"✅ Connecté à Safety Graph: {self.config.database}")
        
        # Cache pour les stats provinciales
        self._provincial_stats = None
    
    # ========================================================================
    # MÉTHODES PRINCIPALES
    # ========================================================================
    
    def get_sector_risk_profile(self, secteur_scian: str) -> Optional[SectorRiskProfile]:
        """
        Obtient le profil de risque complet d'un secteur SCIAN.
        
        Args:
            secteur_scian: Nom du secteur (ex: "CONSTRUCTION", "SOINS DE SANTE...")
            
        Returns:
            SectorRiskProfile avec toutes les statistiques
        """
        query = """
        SELECT 
            "SECTEUR_SCIAN" as secteur,
            COUNT(*) as total_lesions,
            ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM cnesst_lesions_quebec), 2) as pct_total,
            ROUND(100.0 * SUM(CASE WHEN "IND_LESION_TMS" = 'OUI' THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_tms,
            ROUND(100.0 * SUM(CASE WHEN "IND_LESION_PSY" = 'OUI' THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_psy,
            ROUND(100.0 * SUM(CASE WHEN "IND_LESION_MACHINE" = 'OUI' THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_machine,
            ROUND(100.0 * SUM(CASE WHEN "IND_LESION_SURDITE" = 'OUI' THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_surdite
        FROM cnesst_lesions_quebec
        WHERE "SECTEUR_SCIAN" ILIKE %s
        GROUP BY "SECTEUR_SCIAN"
        """
        
        df = pd.read_sql(query, self.engine, params=(f"%{secteur_scian}%",))
        
        if df.empty:
            logger.warning(f"⚠️ Secteur non trouvé: {secteur_scian}")
            return None
        
        row = df.iloc[0]
        
        # Calculer le score de risque
        risk_score = self._calculate_risk_score(
            row['total_lesions'],
            row['pct_tms'],
            row['pct_psy']
        )
        
        # Déterminer le niveau de risque
        risk_level = self._get_risk_level(row['total_lesions'])
        
        # Obtenir les tops
        top_nature = self._get_top_value(secteur_scian, "NATURE_LESION")
        top_siege = self._get_top_value(secteur_scian, "SIEGE_LESION")
        top_agent = self._get_top_value(secteur_scian, "AGENT_CAUSAL_LESION")
        
        return SectorRiskProfile(
            secteur_scian=row['secteur'],
            total_lesions=int(row['total_lesions']),
            pct_total_qc=float(row['pct_total']),
            risk_tms_pct=float(row['pct_tms']),
            risk_psy_pct=float(row['pct_psy']),
            risk_machine_pct=float(row['pct_machine']),
            risk_surdite_pct=float(row['pct_surdite']),
            risk_score=risk_score,
            risk_level=risk_level,
            top_nature_lesion=top_nature,
            top_siege_lesion=top_siege,
            top_agent_causal=top_agent
        )
    
    def benchmark_sector(self, secteur_scian: str, indicateur: str = "TMS") -> BenchmarkResult:
        """
        Compare un secteur à la moyenne provinciale pour un indicateur.
        
        Args:
            secteur_scian: Nom du secteur
            indicateur: "TMS", "PSY", "MACHINE", "SURDITE"
            
        Returns:
            BenchmarkResult avec écart et recommandations
        """
        col_map = {
            "TMS": "IND_LESION_TMS",
            "PSY": "IND_LESION_PSY",
            "MACHINE": "IND_LESION_MACHINE",
            "SURDITE": "IND_LESION_SURDITE"
        }
        
        col = col_map.get(indicateur.upper())
        if not col:
            raise ValueError(f"Indicateur invalide: {indicateur}")
        
        # Stats secteur
        query_secteur = f"""
        SELECT 
            ROUND(100.0 * SUM(CASE WHEN "{col}" = 'OUI' THEN 1 ELSE 0 END) / COUNT(*), 2) as taux
        FROM cnesst_lesions_quebec
        WHERE "SECTEUR_SCIAN" ILIKE %s
        """
        df_secteur = pd.read_sql(query_secteur, self.engine, params=(f"%{secteur_scian}%",))
        
        # Stats provinciales
        query_prov = f"""
        SELECT 
            ROUND(100.0 * SUM(CASE WHEN "{col}" = 'OUI' THEN 1 ELSE 0 END) / COUNT(*), 2) as taux
        FROM cnesst_lesions_quebec
        """
        df_prov = pd.read_sql(query_prov, self.engine)
        
        taux_secteur = float(df_secteur.iloc[0]['taux'])
        taux_prov = float(df_prov.iloc[0]['taux'])
        ecart = taux_secteur - taux_prov
        ecart_pct = (ecart / taux_prov * 100) if taux_prov > 0 else 0
        
        # Déterminer statut et recommandation
        if ecart > taux_prov * 0.2:
            statut = "⚠️ AU-DESSUS"
            recommandation = f"Action prioritaire requise: réduire les {indicateur} de {abs(ecart):.1f}%"
        elif ecart < -taux_prov * 0.2:
            statut = "✅ EN-DESSOUS"
            recommandation = f"Performance supérieure: maintenir les bonnes pratiques"
        else:
            statut = "➡️ MOYENNE"
            recommandation = f"Dans la moyenne provinciale: surveiller les tendances"
        
        return BenchmarkResult(
            secteur=secteur_scian,
            indicateur=indicateur,
            taux_secteur=taux_secteur,
            taux_provincial=taux_prov,
            ecart=ecart,
            ecart_pct=ecart_pct,
            statut=statut,
            recommandation=recommandation
        )
    
    def get_sector_trends(self, secteur_scian: str) -> Dict[int, int]:
        """Obtient les tendances 2017-2023 d'un secteur"""
        query = """
        SELECT "ANNEE" as annee, COUNT(*) as total
        FROM cnesst_lesions_quebec
        WHERE "SECTEUR_SCIAN" ILIKE %s
        GROUP BY "ANNEE"
        ORDER BY "ANNEE"
        """
        df = pd.read_sql(query, self.engine, params=(f"%{secteur_scian}%",))
        return {int(row['annee']): int(row['total']) for _, row in df.iterrows()}
    
    def get_top_n(self, secteur_scian: str, column: str, n: int = 5) -> List[Dict[str, Any]]:
        """Obtient le top N pour une colonne donnée"""
        query = f"""
        SELECT "{column}" as valeur, COUNT(*) as total,
               ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as pct
        FROM cnesst_lesions_quebec
        WHERE "SECTEUR_SCIAN" ILIKE %s AND "{column}" IS NOT NULL
        GROUP BY "{column}"
        ORDER BY total DESC
        LIMIT {n}
        """
        df = pd.read_sql(query, self.engine, params=(f"%{secteur_scian}%",))
        return df.to_dict('records')
    
    def get_full_enrichment(self, secteur_scian: str) -> Optional[SafeTwinEnrichment]:
        """
        Obtient l'enrichissement complet pour SafeTwin X5.
        
        Args:
            secteur_scian: Nom du secteur SCIAN
            
        Returns:
            SafeTwinEnrichment avec toutes les données pour le jumeau numérique
        """
        logger.info(f"🔍 Enrichissement SafeTwin pour: {secteur_scian}")
        
        # Profil de risque
        profile = self.get_sector_risk_profile(secteur_scian)
        if not profile:
            return None
        
        # Benchmarks
        benchmarks = [
            self.benchmark_sector(secteur_scian, "TMS"),
            self.benchmark_sector(secteur_scian, "PSY"),
            self.benchmark_sector(secteur_scian, "MACHINE"),
            self.benchmark_sector(secteur_scian, "SURDITE")
        ]
        
        # Tendances
        tendances = self.get_sector_trends(secteur_scian)
        
        # Top 5
        top_natures = self.get_top_n(secteur_scian, "NATURE_LESION", 5)
        top_sieges = self.get_top_n(secteur_scian, "SIEGE_LESION", 5)
        top_agents = self.get_top_n(secteur_scian, "AGENT_CAUSAL_LESION", 5)
        
        # Générer recommandations IA
        recommandations = self._generate_ai_recommendations(profile, benchmarks)
        
        return SafeTwinEnrichment(
            secteur_scian=secteur_scian,
            risk_profile=profile,
            benchmarks=benchmarks,
            tendances=tendances,
            top_5_natures=top_natures,
            top_5_sieges=top_sieges,
            top_5_agents=top_agents,
            recommandations_ia=recommandations
        )
    
    # ========================================================================
    # MÉTHODES POUR LES AGENTS HUGO
    # ========================================================================
    
    def hugo_hazard_mapper_enrichment(self, secteur_scian: str) -> Dict[str, Any]:
        """
        Données pour l'agent HazardMapper de SafeTwin X5.
        Retourne les dangers prioritaires basés sur les stats réelles.
        """
        enrichment = self.get_full_enrichment(secteur_scian)
        if not enrichment:
            return {}
        
        return {
            "secteur": secteur_scian,
            "risk_level": enrichment.risk_profile.risk_level,
            "risk_score": enrichment.risk_profile.risk_score,
            "dangers_prioritaires": [
                {
                    "type": "TMS",
                    "taux": enrichment.risk_profile.risk_tms_pct,
                    "priorite": 1 if enrichment.risk_profile.risk_tms_pct > 20 else 2
                },
                {
                    "type": "PSY",
                    "taux": enrichment.risk_profile.risk_psy_pct,
                    "priorite": 1 if enrichment.risk_profile.risk_psy_pct > 5 else 2
                },
                {
                    "type": "MACHINE",
                    "taux": enrichment.risk_profile.risk_machine_pct,
                    "priorite": 1 if enrichment.risk_profile.risk_machine_pct > 3 else 3
                }
            ],
            "zones_a_risque": enrichment.top_5_natures,
            "agents_causaux": enrichment.top_5_agents,
            "parties_corps": enrichment.top_5_sieges
        }
    
    def hugo_benchmark_agent_enrichment(self, secteur_scian: str) -> Dict[str, Any]:
        """
        Données pour l'agent BenchmarkAgent de SafeTwin X5.
        Compare le secteur aux moyennes provinciales.
        """
        benchmarks = [
            self.benchmark_sector(secteur_scian, ind)
            for ind in ["TMS", "PSY", "MACHINE", "SURDITE"]
        ]
        
        return {
            "secteur": secteur_scian,
            "benchmarks": [
                {
                    "indicateur": b.indicateur,
                    "taux_secteur": b.taux_secteur,
                    "taux_provincial": b.taux_provincial,
                    "ecart": b.ecart,
                    "ecart_pct": b.ecart_pct,
                    "statut": b.statut,
                    "recommandation": b.recommandation
                }
                for b in benchmarks
            ],
            "score_global": sum(1 for b in benchmarks if "EN-DESSOUS" in b.statut) / len(benchmarks) * 100
        }
    
    def hugo_trend_analyst_enrichment(self, secteur_scian: str) -> Dict[str, Any]:
        """
        Données pour l'agent TrendAnalyst de SafeTwin X5.
        Analyse les tendances 2017-2023.
        """
        tendances = self.get_sector_trends(secteur_scian)
        
        if len(tendances) < 2:
            return {"secteur": secteur_scian, "tendances": tendances, "analyse": "Données insuffisantes"}
        
        annees = sorted(tendances.keys())
        premiere = tendances[annees[0]]
        derniere = tendances[annees[-1]]
        variation = ((derniere - premiere) / premiere * 100) if premiere > 0 else 0
        
        return {
            "secteur": secteur_scian,
            "tendances": tendances,
            "annee_debut": annees[0],
            "annee_fin": annees[-1],
            "valeur_debut": premiere,
            "valeur_fin": derniere,
            "variation_pct": round(variation, 1),
            "tendance": "📈 HAUSSE" if variation > 10 else "📉 BAISSE" if variation < -10 else "➡️ STABLE",
            "analyse": f"Variation de {variation:+.1f}% sur {len(annees)} ans"
        }
    
    # ========================================================================
    # MÉTHODES UTILITAIRES
    # ========================================================================
    
    def _calculate_risk_score(self, total: int, pct_tms: float, pct_psy: float) -> float:
        """Calcule un score de risque 0-100"""
        # Normaliser le volume (log scale)
        volume_score = min(40, (total / 10000) * 10)
        # Score TMS (max 30)
        tms_score = min(30, pct_tms * 1.5)
        # Score PSY (max 30)
        psy_score = min(30, pct_psy * 5)
        
        return round(volume_score + tms_score + psy_score, 1)
    
    def _get_risk_level(self, total: int) -> str:
        """Détermine le niveau de risque basé sur le volume"""
        if total > 50000:
            return "🔴 CRITIQUE"
        elif total > 20000:
            return "🟠 ÉLEVÉ"
        elif total > 5000:
            return "🟡 MODÉRÉ"
        else:
            return "🟢 FAIBLE"
    
    def _get_top_value(self, secteur: str, column: str) -> Optional[str]:
        """Obtient la valeur la plus fréquente pour une colonne"""
        query = f"""
        SELECT "{column}" as valeur
        FROM cnesst_lesions_quebec
        WHERE "SECTEUR_SCIAN" ILIKE %s AND "{column}" IS NOT NULL
        GROUP BY "{column}"
        ORDER BY COUNT(*) DESC
        LIMIT 1
        """
        df = pd.read_sql(query, self.engine, params=(f"%{secteur}%",))
        return df.iloc[0]['valeur'] if not df.empty else None
    
    def _generate_ai_recommendations(
        self, 
        profile: SectorRiskProfile, 
        benchmarks: List[BenchmarkResult]
    ) -> List[str]:
        """Génère des recommandations IA basées sur les données"""
        recs = []
        
        # Recommandations basées sur le niveau de risque
        if profile.risk_level in ["🔴 CRITIQUE", "🟠 ÉLEVÉ"]:
            recs.append(f"⚠️ Secteur à haut risque ({profile.total_lesions:,} lésions): audit SST complet recommandé")
        
        # Recommandations TMS
        if profile.risk_tms_pct > 25:
            recs.append(f"🦴 TMS élevés ({profile.risk_tms_pct}%): programme ergonomique prioritaire")
        
        # Recommandations PSY
        if profile.risk_psy_pct > 5:
            recs.append(f"🧠 Risques psychosociaux ({profile.risk_psy_pct}%): programme bien-être recommandé")
        
        # Recommandations basées sur les benchmarks
        for b in benchmarks:
            if "AU-DESSUS" in b.statut:
                recs.append(f"📊 {b.indicateur}: {b.recommandation}")
        
        # Recommandation basée sur la nature des lésions
        if profile.top_nature_lesion:
            recs.append(f"🎯 Nature dominante: {profile.top_nature_lesion} - cibler les mesures de prévention")
        
        return recs
    
    # ========================================================================
    # MÉTHODES DE RAPPORT
    # ========================================================================
    
    def generate_sector_report(self, secteur_scian: str) -> str:
        """Génère un rapport texte complet pour un secteur"""
        enrichment = self.get_full_enrichment(secteur_scian)
        if not enrichment:
            return f"❌ Secteur non trouvé: {secteur_scian}"
        
        p = enrichment.risk_profile
        
        report = f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  SAFETWIN X5 - RAPPORT D'ENRICHISSEMENT SECTEUR                              ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  Secteur: {p.secteur_scian[:60]:<60} ║
║  Source: CNESST Québec (793,737 records totaux)                              ║
╚══════════════════════════════════════════════════════════════════════════════╝

📊 PROFIL DE RISQUE
───────────────────────────────────────────────────────────────────────────────
  Total lésions secteur:    {p.total_lesions:>10,}
  % du total Québec:        {p.pct_total_qc:>10.1f}%
  Score de risque:          {p.risk_score:>10.1f}/100
  Niveau de risque:         {p.risk_level:>10}

📈 INDICATEURS CLÉS
───────────────────────────────────────────────────────────────────────────────
  TMS (musculo-squelettiques): {p.risk_tms_pct:>6.1f}%
  PSY (psychologiques):        {p.risk_psy_pct:>6.1f}%
  Machine:                     {p.risk_machine_pct:>6.1f}%
  Surdité:                     {p.risk_surdite_pct:>6.1f}%

🎯 TOP CARACTÉRISTIQUES
───────────────────────────────────────────────────────────────────────────────
  Nature dominante:   {p.top_nature_lesion or 'N/A'}
  Siège dominant:     {p.top_siege_lesion or 'N/A'}
  Agent causal:       {p.top_agent_causal or 'N/A'}

📊 BENCHMARKING VS MOYENNE PROVINCIALE
───────────────────────────────────────────────────────────────────────────────"""
        
        for b in enrichment.benchmarks:
            report += f"\n  {b.indicateur:8} | Secteur: {b.taux_secteur:5.1f}% | Prov: {b.taux_provincial:5.1f}% | {b.statut}"
        
        report += """

🤖 RECOMMANDATIONS IA (AGENTS HUGO)
───────────────────────────────────────────────────────────────────────────────"""
        
        for i, rec in enumerate(enrichment.recommandations_ia, 1):
            report += f"\n  {i}. {rec}"
        
        report += """

📈 TENDANCES 2017-2023
───────────────────────────────────────────────────────────────────────────────"""
        
        for annee, total in sorted(enrichment.tendances.items()):
            bar = "█" * int(total / max(enrichment.tendances.values()) * 30)
            report += f"\n  {annee}: {bar} {total:,}"
        
        report += "\n\n═══════════════════════════════════════════════════════════════════════════════\n"
        
        return report
    
    def export_to_json(self, secteur_scian: str) -> str:
        """Exporte l'enrichissement en JSON pour l'API SafeTwin"""
        enrichment = self.get_full_enrichment(secteur_scian)
        if not enrichment:
            return json.dumps({"error": f"Secteur non trouvé: {secteur_scian}"})
        
        return json.dumps({
            "secteur_scian": enrichment.secteur_scian,
            "risk_profile": {
                "total_lesions": enrichment.risk_profile.total_lesions,
                "pct_total_qc": enrichment.risk_profile.pct_total_qc,
                "risk_score": enrichment.risk_profile.risk_score,
                "risk_level": enrichment.risk_profile.risk_level,
                "risk_tms_pct": enrichment.risk_profile.risk_tms_pct,
                "risk_psy_pct": enrichment.risk_profile.risk_psy_pct,
                "top_nature": enrichment.risk_profile.top_nature_lesion,
                "top_siege": enrichment.risk_profile.top_siege_lesion,
                "top_agent": enrichment.risk_profile.top_agent_causal
            },
            "benchmarks": [
                {
                    "indicateur": b.indicateur,
                    "taux_secteur": b.taux_secteur,
                    "taux_provincial": b.taux_provincial,
                    "ecart": b.ecart,
                    "statut": b.statut
                }
                for b in enrichment.benchmarks
            ],
            "tendances": enrichment.tendances,
            "top_5_natures": enrichment.top_5_natures,
            "top_5_sieges": enrichment.top_5_sieges,
            "top_5_agents": enrichment.top_5_agents,
            "recommandations_ia": enrichment.recommandations_ia
        }, indent=2, ensure_ascii=False)


# ============================================================================
# FONCTIONS UTILITAIRES
# ============================================================================

def list_all_sectors() -> List[str]:
    """Liste tous les secteurs SCIAN disponibles"""
    config = SafetyGraphConfig()
    engine = create_engine(config.connection_string)
    
    query = """
    SELECT DISTINCT "SECTEUR_SCIAN" as secteur, COUNT(*) as total
    FROM cnesst_lesions_quebec
    GROUP BY "SECTEUR_SCIAN"
    ORDER BY total DESC
    """
    df = pd.read_sql(query, engine)
    return df['secteur'].tolist()


def get_global_stats() -> Dict[str, int]:
    """Retourne les statistiques globales de Safety Graph"""
    config = SafetyGraphConfig()
    engine = create_engine(config.connection_string)
    
    return {
        "osha_usa": pd.read_sql("SELECT COUNT(*) FROM osha_injuries_raw", engine).iloc[0, 0],
        "eurostat_eu": pd.read_sql("SELECT COUNT(*) FROM eurostat_esaw", engine).iloc[0, 0],
        "cnesst_qc": pd.read_sql("SELECT COUNT(*) FROM cnesst_lesions_quebec", engine).iloc[0, 0]
    }


# ============================================================================
# POINT D'ENTRÉE CLI
# ============================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="SafeTwin X5 Enrichment Module")
    parser.add_argument("--secteur", "-s", help="Secteur SCIAN à analyser")
    parser.add_argument("--list", "-l", action="store_true", help="Lister tous les secteurs")
    parser.add_argument("--stats", action="store_true", help="Afficher les stats globales")
    parser.add_argument("--json", "-j", action="store_true", help="Export JSON")
    parser.add_argument("--hugo", choices=["hazard", "benchmark", "trend"], help="Données pour agent HUGO")
    
    args = parser.parse_args()
    
    if args.list:
        print("\n📋 SECTEURS SCIAN DISPONIBLES:")
        print("=" * 60)
        for i, s in enumerate(list_all_sectors(), 1):
            print(f"  {i:2}. {s}")
    
    elif args.stats:
        stats = get_global_stats()
        total = sum(stats.values())
        print("\n🏆 SAFETY GRAPH - STATISTIQUES GLOBALES")
        print("=" * 60)
        print(f"  🇺🇸 OSHA USA:        {stats['osha_usa']:>12,}")
        print(f"  🇪🇺 Eurostat EU-27:  {stats['eurostat_eu']:>12,}")
        print(f"  🇨🇦 CNESST Québec:   {stats['cnesst_qc']:>12,}")
        print("=" * 60)
        print(f"  📈 TOTAL:            {total:>12,}")
    
    elif args.secteur:
        enricher = SafeTwinEnricher()
        
        if args.json:
            print(enricher.export_to_json(args.secteur))
        elif args.hugo == "hazard":
            print(json.dumps(enricher.hugo_hazard_mapper_enrichment(args.secteur), indent=2, ensure_ascii=False))
        elif args.hugo == "benchmark":
            print(json.dumps(enricher.hugo_benchmark_agent_enrichment(args.secteur), indent=2, ensure_ascii=False))
        elif args.hugo == "trend":
            print(json.dumps(enricher.hugo_trend_analyst_enrichment(args.secteur), indent=2, ensure_ascii=False))
        else:
            print(enricher.generate_sector_report(args.secteur))
    
    else:
        parser.print_help()
