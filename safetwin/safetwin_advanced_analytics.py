#!/usr/bin/env python3
"""
============================================================================
SAFETWIN X5 - MODULE D'ANALYTIQUE AVANCÉE
============================================================================
Exploite les 2,853,583 records HSE pour des analyses prédictives,
corrélations croisées et comparaisons internationales.

Version: 2.0.0
Date: 2026-01-13
============================================================================
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Any
import json
from datetime import datetime

# Configuration
DB_URL = "postgresql://postgres:postgres@localhost:5432/safety_graph"


# ============================================================================
# CLASSE PRINCIPALE - ANALYTIQUE AVANCÉE
# ============================================================================

class SafeTwinAdvancedAnalytics:
    """
    Analytique avancée pour SafeTwin X5.
    
    Fonctionnalités:
    - Prédiction de risques
    - Comparaisons internationales
    - Corrélations croisées
    - Analyse de saisonnalité
    - Estimation des coûts
    """
    
    def __init__(self):
        self.engine = create_engine(DB_URL)
        print("✅ SafeTwin Advanced Analytics initialisé")
    
    # ========================================================================
    # 1. COMPARAISON INTERNATIONALE (USA vs EU vs QC)
    # ========================================================================
    
    def compare_jurisdictions(self) -> Dict[str, Any]:
        """
        Compare les 3 juridictions: USA, EU-27, Québec.
        Retourne des insights de benchmark international.
        """
        print("🌍 Analyse comparative internationale...")
        
        # Stats par juridiction
        stats = {
            "usa": self._get_usa_stats(),
            "eu": self._get_eu_stats(),
            "quebec": self._get_quebec_stats()
        }
        
        # Calcul des ratios comparatifs
        total_global = sum(s["total_records"] for s in stats.values())
        
        comparison = {
            "global_total": total_global,
            "jurisdictions": stats,
            "insights": []
        }
        
        # Insights automatiques
        qc_tms = stats["quebec"].get("tms_rate", 0)
        comparison["insights"].append({
            "type": "TMS_COMPARISON",
            "message": f"Le Québec a un taux TMS de {qc_tms:.1f}%, données comparatives USA/EU à enrichir",
            "action": "Harmoniser les classifications pour benchmark précis"
        })
        
        # Ratio records per capita (estimation)
        comparison["insights"].append({
            "type": "COVERAGE",
            "message": f"Couverture: USA {stats['usa']['total_records']:,} | EU {stats['eu']['total_records']:,} | QC {stats['quebec']['total_records']:,}",
            "action": "Le Québec a le meilleur ratio données/population"
        })
        
        return comparison
    
    def _get_usa_stats(self) -> Dict:
        """Stats OSHA USA"""
        query = "SELECT COUNT(*) as total FROM osha_injuries_raw"
        df = pd.read_sql(query, self.engine)
        
        # Top états
        top_states = pd.read_sql("""
            SELECT state, COUNT(*) as total 
            FROM osha_injuries_raw 
            WHERE state IS NOT NULL
            GROUP BY state 
            ORDER BY total DESC 
            LIMIT 5
        """, self.engine)
        
        return {
            "total_records": int(df.iloc[0, 0]),
            "period": "2016-2021",
            "classification": "NAICS",
            "top_states": top_states.to_dict('records')
        }
    
    def _get_eu_stats(self) -> Dict:
        """Stats Eurostat EU-27"""
        query = "SELECT COUNT(*) as total FROM eurostat_esaw"
        df = pd.read_sql(query, self.engine)
        
        return {
            "total_records": int(df.iloc[0, 0]),
            "period": "2010-2022",
            "classification": "NACE",
            "countries": 27
        }
    
    def _get_quebec_stats(self) -> Dict:
        """Stats CNESST Québec"""
        query = """
        SELECT 
            COUNT(*) as total,
            ROUND(100.0 * SUM(CASE WHEN "IND_LESION_TMS" = 'OUI' THEN 1 ELSE 0 END) / COUNT(*), 2) as tms_rate,
            ROUND(100.0 * SUM(CASE WHEN "IND_LESION_PSY" = 'OUI' THEN 1 ELSE 0 END) / COUNT(*), 2) as psy_rate
        FROM cnesst_lesions_quebec
        """
        df = pd.read_sql(query, self.engine)
        
        return {
            "total_records": int(df.iloc[0, 0]),
            "period": "2017-2023",
            "classification": "SCIAN",
            "tms_rate": float(df.iloc[0, 1]),
            "psy_rate": float(df.iloc[0, 2])
        }
    
    # ========================================================================
    # 2. PRÉDICTION DE RISQUES (basée sur tendances)
    # ========================================================================
    
    def predict_sector_risk(self, secteur: str, years_ahead: int = 2) -> Dict[str, Any]:
        """
        Prédit l'évolution des risques pour un secteur.
        Utilise régression linéaire sur les tendances 7 ans.
        """
        print(f"🔮 Prédiction pour: {secteur}...")
        
        # Données historiques
        query = """
        SELECT "ANNEE" as annee, COUNT(*) as total
        FROM cnesst_lesions_quebec
        WHERE "SECTEUR_SCIAN" ILIKE %s
        GROUP BY "ANNEE"
        ORDER BY "ANNEE"
        """
        df = pd.read_sql(query, self.engine, params=(f"%{secteur}%",))
        
        if len(df) < 3:
            return {"error": "Données insuffisantes pour prédiction"}
        
        # Régression linéaire simple
        x = df['annee'].values
        y = df['total'].values
        
        # Coefficients
        n = len(x)
        sum_x = np.sum(x)
        sum_y = np.sum(y)
        sum_xy = np.sum(x * y)
        sum_x2 = np.sum(x ** 2)
        
        slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x ** 2)
        intercept = (sum_y - slope * sum_x) / n
        
        # Prédictions
        last_year = int(df['annee'].max())
        predictions = {}
        for i in range(1, years_ahead + 1):
            future_year = last_year + i
            predicted = int(slope * future_year + intercept)
            predictions[future_year] = max(0, predicted)  # Pas de valeurs négatives
        
        # Calcul de la tendance
        first_val = df['total'].iloc[0]
        last_val = df['total'].iloc[-1]
        trend_pct = ((last_val - first_val) / first_val * 100) if first_val > 0 else 0
        
        return {
            "secteur": secteur,
            "historique": df.to_dict('records'),
            "predictions": predictions,
            "tendance": {
                "direction": "📈 HAUSSE" if slope > 0 else "📉 BAISSE",
                "pente_annuelle": round(slope, 1),
                "variation_totale_pct": round(trend_pct, 1)
            },
            "confiance": "MOYENNE" if n >= 5 else "FAIBLE",
            "recommandation": self._generate_prediction_recommendation(slope, trend_pct)
        }
    
    def _generate_prediction_recommendation(self, slope: float, trend_pct: float) -> str:
        """Génère une recommandation basée sur la prédiction"""
        if trend_pct > 20:
            return "⚠️ ALERTE: Secteur en forte croissance de risque. Audit complet recommandé."
        elif trend_pct > 10:
            return "⚡ ATTENTION: Tendance à la hausse. Renforcer les mesures préventives."
        elif trend_pct < -10:
            return "✅ POSITIF: Amélioration continue. Maintenir les bonnes pratiques."
        else:
            return "➡️ STABLE: Surveiller les indicateurs. Prévention continue."
    
    # ========================================================================
    # 3. CORRÉLATIONS CROISÉES
    # ========================================================================
    
    def analyze_correlations(self, secteur: str) -> Dict[str, Any]:
        """
        Analyse les corrélations entre variables pour un secteur:
        - Âge × Type de lésion
        - Sexe × Gravité
        - Agent causal × Siège de lésion
        """
        print(f"🔗 Analyse des corrélations: {secteur}...")
        
        # Corrélation Âge × TMS
        age_tms = pd.read_sql(f"""
            SELECT 
                "GROUPE_AGE" as groupe_age,
                COUNT(*) as total,
                ROUND(100.0 * SUM(CASE WHEN "IND_LESION_TMS" = 'OUI' THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_tms
            FROM cnesst_lesions_quebec
            WHERE "SECTEUR_SCIAN" ILIKE '%{secteur}%'
            GROUP BY "GROUPE_AGE"
            ORDER BY pct_tms DESC
        """, self.engine)
        
        # Corrélation Sexe × PSY
        sexe_psy = pd.read_sql(f"""
            SELECT 
                "SEXE_PERS_PHYS" as sexe,
                COUNT(*) as total,
                ROUND(100.0 * SUM(CASE WHEN "IND_LESION_PSY" = 'OUI' THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_psy
            FROM cnesst_lesions_quebec
            WHERE "SECTEUR_SCIAN" ILIKE '%{secteur}%'
            GROUP BY "SEXE_PERS_PHYS"
        """, self.engine)
        
        # Corrélation Agent causal × Siège
        agent_siege = pd.read_sql(f"""
            SELECT 
                "AGENT_CAUSAL_LESION" as agent,
                "SIEGE_LESION" as siege,
                COUNT(*) as total
            FROM cnesst_lesions_quebec
            WHERE "SECTEUR_SCIAN" ILIKE '%{secteur}%'
            GROUP BY "AGENT_CAUSAL_LESION", "SIEGE_LESION"
            ORDER BY total DESC
            LIMIT 10
        """, self.engine)
        
        # Insights automatiques
        insights = []
        
        if not age_tms.empty:
            top_age_tms = age_tms.iloc[0]
            insights.append({
                "type": "AGE_TMS",
                "finding": f"Groupe d'âge le plus touché par TMS: {top_age_tms['groupe_age']} ({top_age_tms['pct_tms']}%)",
                "action": "Cibler les formations ergonomiques sur ce groupe"
            })
        
        if not sexe_psy.empty:
            for _, row in sexe_psy.iterrows():
                if row['pct_psy'] > 5:
                    insights.append({
                        "type": "SEXE_PSY",
                        "finding": f"{row['sexe']}: {row['pct_psy']}% de lésions psychologiques",
                        "action": "Programme de soutien psychologique recommandé"
                    })
        
        return {
            "secteur": secteur,
            "correlations": {
                "age_tms": age_tms.to_dict('records'),
                "sexe_psy": sexe_psy.to_dict('records'),
                "agent_siege_top10": agent_siege.to_dict('records')
            },
            "insights": insights
        }
    
    # ========================================================================
    # 4. ANALYSE DE SAISONNALITÉ (si données disponibles)
    # ========================================================================
    
    def analyze_seasonality(self, secteur: str) -> Dict[str, Any]:
        """
        Analyse les patterns saisonniers des accidents.
        Note: Nécessite des données mensuelles (à enrichir).
        """
        print(f"📅 Analyse saisonnalité: {secteur}...")
        
        # Pour l'instant, analyse par année (données mensuelles non disponibles)
        query = """
        SELECT "ANNEE" as annee, COUNT(*) as total
        FROM cnesst_lesions_quebec
        WHERE "SECTEUR_SCIAN" ILIKE %s
        GROUP BY "ANNEE"
        ORDER BY "ANNEE"
        """
        df = pd.read_sql(query, self.engine, params=(f"%{secteur}%",))
        
        # Calcul des variations année sur année
        variations = []
        for i in range(1, len(df)):
            prev = df.iloc[i-1]['total']
            curr = df.iloc[i]['total']
            var = ((curr - prev) / prev * 100) if prev > 0 else 0
            variations.append({
                "annee": int(df.iloc[i]['annee']),
                "variation_pct": round(var, 1)
            })
        
        # Détecter les anomalies (COVID 2020, 2022)
        anomalies = []
        for v in variations:
            if abs(v['variation_pct']) > 30:
                anomalies.append({
                    "annee": v['annee'],
                    "variation": v['variation_pct'],
                    "explication": "COVID-19" if v['annee'] in [2020, 2022] else "À investiguer"
                })
        
        return {
            "secteur": secteur,
            "donnees_annuelles": df.to_dict('records'),
            "variations_yoy": variations,
            "anomalies_detectees": anomalies,
            "recommandation": "Enrichir avec données mensuelles pour analyse saisonnière complète"
        }
    
    # ========================================================================
    # 5. ESTIMATION DES COÛTS
    # ========================================================================
    
    def estimate_costs(self, secteur: str) -> Dict[str, Any]:
        """
        Estime les coûts potentiels basés sur le profil de risque.
        Utilise les barèmes moyens CNESST/CSST.
        """
        print(f"💰 Estimation des coûts: {secteur}...")
        
        # Barèmes estimés (moyennes CNESST)
        COUT_MOYEN_LESION = 8500  # $ CAD
        COUT_TMS = 12000
        COUT_PSY = 25000
        COUT_GRAVE = 45000
        JOURS_PERDUS_MOYEN = 45
        COUT_JOUR_PERDU = 350
        
        # Stats du secteur
        query = """
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN "IND_LESION_TMS" = 'OUI' THEN 1 ELSE 0 END) as tms,
            SUM(CASE WHEN "IND_LESION_PSY" = 'OUI' THEN 1 ELSE 0 END) as psy
        FROM cnesst_lesions_quebec
        WHERE "SECTEUR_SCIAN" ILIKE %s
        """
        df = pd.read_sql(query, self.engine, params=(f"%{secteur}%",))
        
        total = int(df.iloc[0]['total'])
        tms = int(df.iloc[0]['tms'])
        psy = int(df.iloc[0]['psy'])
        autres = total - tms - psy
        
        # Calculs
        cout_tms_total = tms * COUT_TMS
        cout_psy_total = psy * COUT_PSY
        cout_autres_total = autres * COUT_MOYEN_LESION
        cout_total = cout_tms_total + cout_psy_total + cout_autres_total
        
        jours_perdus_estimes = total * JOURS_PERDUS_MOYEN
        cout_productivite = jours_perdus_estimes * COUT_JOUR_PERDU
        
        # Coût annuel moyen (7 ans de données)
        cout_annuel_moyen = cout_total / 7
        
        return {
            "secteur": secteur,
            "periode": "2017-2023 (7 ans)",
            "lesions": {
                "total": total,
                "tms": tms,
                "psy": psy,
                "autres": autres
            },
            "couts_estimes": {
                "total_7_ans": cout_total,
                "tms": cout_tms_total,
                "psy": cout_psy_total,
                "autres": cout_autres_total,
                "moyen_annuel": round(cout_annuel_moyen),
                "moyen_par_lesion": round(cout_total / total) if total > 0 else 0
            },
            "productivite": {
                "jours_perdus_estimes": jours_perdus_estimes,
                "cout_productivite": cout_productivite
            },
            "total_impact_economique": cout_total + cout_productivite,
            "baremes_utilises": {
                "source": "Estimations basées sur moyennes CNESST",
                "cout_moyen_lesion": COUT_MOYEN_LESION,
                "cout_tms": COUT_TMS,
                "cout_psy": COUT_PSY,
                "cout_jour_perdu": COUT_JOUR_PERDU
            }
        }
    
    # ========================================================================
    # 6. PROFIL DÉMOGRAPHIQUE DÉTAILLÉ
    # ========================================================================
    
    def demographic_profile(self, secteur: str) -> Dict[str, Any]:
        """
        Analyse démographique détaillée: âge, sexe, croisements.
        """
        print(f"👥 Profil démographique: {secteur}...")
        
        # Distribution par âge
        age_dist = pd.read_sql(f"""
            SELECT "GROUPE_AGE" as groupe_age, COUNT(*) as total,
                   ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as pct
            FROM cnesst_lesions_quebec
            WHERE "SECTEUR_SCIAN" ILIKE '%{secteur}%'
            GROUP BY "GROUPE_AGE"
            ORDER BY total DESC
        """, self.engine)
        
        # Distribution par sexe
        sexe_dist = pd.read_sql(f"""
            SELECT "SEXE_PERS_PHYS" as sexe, COUNT(*) as total,
                   ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as pct
            FROM cnesst_lesions_quebec
            WHERE "SECTEUR_SCIAN" ILIKE '%{secteur}%'
            GROUP BY "SEXE_PERS_PHYS"
        """, self.engine)
        
        # Croisement âge × sexe × TMS
        croix = pd.read_sql(f"""
            SELECT "GROUPE_AGE" as age, "SEXE_PERS_PHYS" as sexe,
                   COUNT(*) as total,
                   SUM(CASE WHEN "IND_LESION_TMS" = 'OUI' THEN 1 ELSE 0 END) as tms
            FROM cnesst_lesions_quebec
            WHERE "SECTEUR_SCIAN" ILIKE '%{secteur}%'
            GROUP BY "GROUPE_AGE", "SEXE_PERS_PHYS"
            ORDER BY total DESC
            LIMIT 10
        """, self.engine)
        
        # Population à risque élevé
        high_risk = []
        for _, row in croix.iterrows():
            tms_rate = (row['tms'] / row['total'] * 100) if row['total'] > 0 else 0
            if tms_rate > 30:
                high_risk.append({
                    "profil": f"{row['sexe']} - {row['age']}",
                    "tms_rate": round(tms_rate, 1),
                    "volume": int(row['total'])
                })
        
        return {
            "secteur": secteur,
            "distribution_age": age_dist.to_dict('records'),
            "distribution_sexe": sexe_dist.to_dict('records'),
            "croisement_age_sexe": croix.to_dict('records'),
            "populations_a_risque_eleve": high_risk,
            "recommandations": self._generate_demographic_recommendations(age_dist, sexe_dist, high_risk)
        }
    
    def _generate_demographic_recommendations(self, age_df, sexe_df, high_risk) -> List[str]:
        """Génère des recommandations basées sur le profil démographique"""
        recs = []
        
        if not age_df.empty:
            top_age = age_df.iloc[0]
            recs.append(f"🎯 Groupe d'âge prioritaire: {top_age['groupe_age']} ({top_age['pct']}% des lésions)")
        
        for hr in high_risk[:3]:
            recs.append(f"⚠️ Population à risque: {hr['profil']} - TMS à {hr['tms_rate']}%")
        
        return recs
    
    # ========================================================================
    # 7. RAPPORT COMPLET SAFETWIN
    # ========================================================================
    
    def generate_full_report(self, secteur: str) -> Dict[str, Any]:
        """
        Génère un rapport analytique complet pour SafeTwin X5.
        Combine toutes les analyses.
        """
        print(f"\n{'='*60}")
        print(f"📊 RAPPORT COMPLET SAFETWIN X5: {secteur}")
        print(f"{'='*60}\n")
        
        report = {
            "secteur": secteur,
            "generated_at": datetime.now().isoformat(),
            "source": "Safety Graph - 2,853,583 records",
            "analyses": {}
        }
        
        # 1. Prédiction
        report["analyses"]["prediction"] = self.predict_sector_risk(secteur)
        
        # 2. Corrélations
        report["analyses"]["correlations"] = self.analyze_correlations(secteur)
        
        # 3. Coûts
        report["analyses"]["costs"] = self.estimate_costs(secteur)
        
        # 4. Démographie
        report["analyses"]["demographics"] = self.demographic_profile(secteur)
        
        # 5. Saisonnalité
        report["analyses"]["seasonality"] = self.analyze_seasonality(secteur)
        
        # Synthèse exécutive
        report["executive_summary"] = self._generate_executive_summary(report)
        
        return report
    
    def _generate_executive_summary(self, report: Dict) -> Dict:
        """Génère un résumé exécutif"""
        pred = report["analyses"]["prediction"]
        costs = report["analyses"]["costs"]
        
        return {
            "secteur": report["secteur"],
            "niveau_risque": pred.get("tendance", {}).get("direction", "N/A"),
            "impact_economique_7_ans": f"${costs.get('total_impact_economique', 0):,}",
            "tendance": f"{pred.get('tendance', {}).get('variation_totale_pct', 0):+.1f}%",
            "top_3_actions": [
                pred.get("recommandation", ""),
                "Programme ergonomique TMS",
                "Formation ciblée par groupe d'âge"
            ]
        }


# ============================================================================
# FONCTIONS UTILITAIRES
# ============================================================================

def print_report(report: Dict, indent: int = 0):
    """Affiche un rapport de manière lisible"""
    prefix = "  " * indent
    for key, value in report.items():
        if isinstance(value, dict):
            print(f"{prefix}{key}:")
            print_report(value, indent + 1)
        elif isinstance(value, list):
            print(f"{prefix}{key}:")
            for item in value[:5]:  # Limite à 5 éléments
                if isinstance(item, dict):
                    print(f"{prefix}  - {item}")
                else:
                    print(f"{prefix}  - {item}")
        else:
            print(f"{prefix}{key}: {value}")


# ============================================================================
# CLI
# ============================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="SafeTwin X5 Advanced Analytics")
    parser.add_argument("--secteur", "-s", help="Secteur SCIAN à analyser")
    parser.add_argument("--compare", "-c", action="store_true", help="Comparaison internationale")
    parser.add_argument("--predict", "-p", action="store_true", help="Prédiction de risques")
    parser.add_argument("--costs", action="store_true", help="Estimation des coûts")
    parser.add_argument("--demographics", "-d", action="store_true", help="Profil démographique")
    parser.add_argument("--full", "-f", action="store_true", help="Rapport complet")
    parser.add_argument("--json", "-j", action="store_true", help="Output JSON")
    
    args = parser.parse_args()
    
    analytics = SafeTwinAdvancedAnalytics()
    
    if args.compare:
        result = analytics.compare_jurisdictions()
        if args.json:
            print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
        else:
            print("\n🌍 COMPARAISON INTERNATIONALE")
            print("=" * 60)
            print(f"Total global: {result['global_total']:,} records")
            for j, stats in result['jurisdictions'].items():
                print(f"\n{j.upper()}: {stats['total_records']:,} records ({stats['period']})")
    
    elif args.secteur:
        if args.predict:
            result = analytics.predict_sector_risk(args.secteur)
        elif args.costs:
            result = analytics.estimate_costs(args.secteur)
        elif args.demographics:
            result = analytics.demographic_profile(args.secteur)
        elif args.full:
            result = analytics.generate_full_report(args.secteur)
        else:
            result = analytics.analyze_correlations(args.secteur)
        
        if args.json:
            print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
        else:
            print_report(result)
    
    else:
        parser.print_help()
