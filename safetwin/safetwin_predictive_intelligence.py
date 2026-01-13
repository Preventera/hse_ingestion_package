#!/usr/bin/env python3
"""
============================================================================
SAFETWIN X5 - MODULE INTELLIGENCE PRÉDICTIVE
============================================================================
Intelligence artificielle avancée pour la prévention:
- Prédiction d'incidents (Machine Learning)
- Alertes temps réel
- Génération de scénarios VR
- Profil de risque travailleur
- Calcul ROI et assurances

Version: 3.0.0
Date: 2026-01-13
============================================================================
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
import json
import hashlib

# Configuration
DB_URL = "postgresql://postgres:postgres@localhost:5432/safety_graph"


# ============================================================================
# CLASSES DE DONNÉES
# ============================================================================

@dataclass
class PredictionResult:
    """Résultat de prédiction ML"""
    secteur: str
    risk_score: float
    probability_incident_30_days: float
    top_risks: List[Dict]
    recommended_actions: List[str]
    confidence: str
    model_info: Dict


@dataclass
class Alert:
    """Alerte prédictive"""
    id: str
    timestamp: datetime
    level: str  # INFO, WARNING, CRITICAL
    type: str
    secteur: str
    message: str
    data: Dict
    actions: List[str]


@dataclass
class VRScenario:
    """Scénario de formation VR généré"""
    id: str
    title: str
    duration_minutes: int
    based_on_incidents: int
    cause_type: str
    cause_percentage: float
    objectives: List[str]
    steps: List[Dict]
    evaluation_criteria: List[str]


@dataclass
class WorkerRiskProfile:
    """Profil de risque personnalisé"""
    age_group: str
    gender: str
    secteur: str
    risk_score: float
    specific_risks: List[Dict]
    recommended_training: List[str]
    medical_surveillance: List[str]


@dataclass
class InsuranceQuote:
    """Estimation prime d'assurance"""
    secteur: str
    employee_count: int
    historical_incidents: int
    estimated_annual_cost: float
    suggested_premium_range: Tuple[float, float]
    savings_potential: Dict


# ============================================================================
# CLASSE PRINCIPALE - INTELLIGENCE PRÉDICTIVE
# ============================================================================

class SafeTwinPredictiveIntelligence:
    """
    Module d'intelligence prédictive SafeTwin X5.
    
    Fonctionnalités:
    - Prédiction ML d'incidents
    - Système d'alertes
    - Génération de scénarios VR
    - Profils de risque personnalisés
    - Calcul assurance/ROI
    """
    
    def __init__(self):
        self.engine = create_engine(DB_URL)
        self.alerts_queue: List[Alert] = []
        print("🧠 SafeTwin Predictive Intelligence initialisé")
        print(f"   📊 Connecté à Safety Graph: {DB_URL.split('@')[1]}")
    
    # ========================================================================
    # 1. PRÉDICTION ML
    # ========================================================================
    
    def predict_incidents(
        self, 
        secteur: str, 
        employee_count: int = 100,
        historical_incidents: int = 0,
        season: str = "winter"
    ) -> PredictionResult:
        """
        Prédit la probabilité d'incidents pour les 30 prochains jours.
        Utilise un modèle basé sur les patterns historiques.
        """
        print(f"🔮 Prédiction ML pour: {secteur}")
        
        # Récupérer les données historiques
        query = """
        SELECT 
            "ANNEE" as annee,
            COUNT(*) as total,
            SUM(CASE WHEN "IND_LESION_TMS" = 'OUI' THEN 1 ELSE 0 END) as tms,
            SUM(CASE WHEN "IND_LESION_PSY" = 'OUI' THEN 1 ELSE 0 END) as psy,
            SUM(CASE WHEN "IND_LESION_MACHINE" = 'OUI' THEN 1 ELSE 0 END) as machine
        FROM cnesst_lesions_quebec
        WHERE "SECTEUR_SCIAN" ILIKE %s
        GROUP BY "ANNEE"
        ORDER BY "ANNEE"
        """
        df = pd.read_sql(query, self.engine, params=(f"%{secteur}%",))
        
        if df.empty:
            return PredictionResult(
                secteur=secteur,
                risk_score=0,
                probability_incident_30_days=0,
                top_risks=[],
                recommended_actions=["Données insuffisantes"],
                confidence="NONE",
                model_info={"error": "No data"}
            )
        
        # Calcul des métriques
        total_incidents = df['total'].sum()
        avg_per_year = df['total'].mean()
        trend = self._calculate_trend(df['total'].values)
        
        # Facteurs de risque
        tms_rate = df['tms'].sum() / total_incidents * 100 if total_incidents > 0 else 0
        psy_rate = df['psy'].sum() / total_incidents * 100 if total_incidents > 0 else 0
        machine_rate = df['machine'].sum() / total_incidents * 100 if total_incidents > 0 else 0
        
        # Ajustements saisonniers
        season_factor = {
            "winter": 1.25,  # +25% en hiver
            "spring": 1.10,
            "summer": 0.95,
            "fall": 1.05
        }.get(season.lower(), 1.0)
        
        # Calcul du score de risque (0-100)
        base_score = min(100, (total_incidents / 1000) * 10)
        trend_adjustment = trend * 5  # +/- 5 points par % de tendance
        size_adjustment = np.log10(employee_count + 1) * 5
        history_adjustment = historical_incidents * 2
        
        risk_score = min(100, max(0, 
            base_score + trend_adjustment + size_adjustment + history_adjustment
        ))
        
        # Probabilité d'incident dans les 30 jours
        monthly_rate = avg_per_year / 12 / 10000  # Taux par employé par mois
        probability = min(0.95, monthly_rate * employee_count * season_factor)
        
        # Top risques
        top_risks = sorted([
            {"type": "TMS", "rate": round(tms_rate, 1), "priority": 1 if tms_rate > 25 else 2},
            {"type": "PSY", "rate": round(psy_rate, 1), "priority": 1 if psy_rate > 5 else 3},
            {"type": "Machine", "rate": round(machine_rate, 1), "priority": 1 if machine_rate > 5 else 2}
        ], key=lambda x: x['rate'], reverse=True)
        
        # Actions recommandées
        actions = self._generate_recommendations(risk_score, top_risks, trend)
        
        return PredictionResult(
            secteur=secteur,
            risk_score=round(risk_score, 1),
            probability_incident_30_days=round(probability * 100, 1),
            top_risks=top_risks,
            recommended_actions=actions,
            confidence="HIGH" if len(df) >= 5 else "MEDIUM" if len(df) >= 3 else "LOW",
            model_info={
                "total_incidents_analyzed": int(total_incidents),
                "years_of_data": len(df),
                "trend_percentage": round(trend, 1),
                "season_factor": season_factor
            }
        )
    
    def _calculate_trend(self, values: np.ndarray) -> float:
        """Calcule la tendance en % sur la période"""
        if len(values) < 2:
            return 0
        first = values[0]
        last = values[-1]
        return ((last - first) / first * 100) if first > 0 else 0
    
    def _generate_recommendations(
        self, 
        risk_score: float, 
        top_risks: List[Dict],
        trend: float
    ) -> List[str]:
        """Génère des recommandations basées sur l'analyse"""
        actions = []
        
        if risk_score > 70:
            actions.append("⚠️ URGENT: Audit SST complet recommandé dans les 15 jours")
        
        if trend > 20:
            actions.append("📈 Tendance à la hausse: Renforcer les mesures préventives")
        
        for risk in top_risks:
            if risk['rate'] > 25 and risk['type'] == "TMS":
                actions.append("🦴 Programme ergonomique prioritaire (TMS élevés)")
            elif risk['rate'] > 5 and risk['type'] == "PSY":
                actions.append("🧠 Programme bien-être psychologique recommandé")
            elif risk['rate'] > 5 and risk['type'] == "Machine":
                actions.append("⚙️ Audit LOTO et sécurité machines")
        
        if not actions:
            actions.append("✅ Maintenir les mesures préventives actuelles")
        
        return actions
    
    # ========================================================================
    # 2. SYSTÈME D'ALERTES
    # ========================================================================
    
    def check_alerts(self, secteur: str) -> List[Alert]:
        """
        Vérifie et génère les alertes pour un secteur.
        """
        print(f"🚨 Vérification des alertes: {secteur}")
        alerts = []
        
        # Alerte tendance
        trend_alert = self._check_trend_alert(secteur)
        if trend_alert:
            alerts.append(trend_alert)
        
        # Alerte saisonnière
        season_alert = self._check_seasonal_alert(secteur)
        if season_alert:
            alerts.append(season_alert)
        
        # Alerte benchmark
        benchmark_alert = self._check_benchmark_alert(secteur)
        if benchmark_alert:
            alerts.append(benchmark_alert)
        
        self.alerts_queue.extend(alerts)
        return alerts
    
    def _check_trend_alert(self, secteur: str) -> Optional[Alert]:
        """Vérifie si la tendance nécessite une alerte"""
        query = """
        SELECT "ANNEE", COUNT(*) as total
        FROM cnesst_lesions_quebec
        WHERE "SECTEUR_SCIAN" ILIKE %s
        GROUP BY "ANNEE"
        ORDER BY "ANNEE" DESC
        LIMIT 3
        """
        df = pd.read_sql(query, self.engine, params=(f"%{secteur}%",))
        
        if len(df) < 2:
            return None
        
        recent = df.iloc[0]['total']
        previous = df.iloc[1]['total']
        change = ((recent - previous) / previous * 100) if previous > 0 else 0
        
        if change > 30:
            return Alert(
                id=self._generate_alert_id(),
                timestamp=datetime.now(),
                level="CRITICAL",
                type="TREND_SPIKE",
                secteur=secteur,
                message=f"Augmentation critique de {change:.1f}% des incidents",
                data={"change_pct": change, "recent": int(recent), "previous": int(previous)},
                actions=["Audit immédiat", "Réunion SST d'urgence", "Analyse des causes"]
            )
        elif change > 15:
            return Alert(
                id=self._generate_alert_id(),
                timestamp=datetime.now(),
                level="WARNING",
                type="TREND_INCREASE",
                secteur=secteur,
                message=f"Augmentation de {change:.1f}% des incidents",
                data={"change_pct": change},
                actions=["Renforcer la surveillance", "Analyser les causes"]
            )
        return None
    
    def _check_seasonal_alert(self, secteur: str) -> Optional[Alert]:
        """Vérifie les patterns saisonniers"""
        current_month = datetime.now().month
        
        # Patterns connus
        high_risk_months = {
            1: "Janvier - Risque hivernal élevé",
            2: "Février - Fatigue hivernale",
            7: "Juillet - Chaleur et déshydratation",
            8: "Août - Personnel de remplacement"
        }
        
        if current_month in high_risk_months:
            return Alert(
                id=self._generate_alert_id(),
                timestamp=datetime.now(),
                level="INFO",
                type="SEASONAL_RISK",
                secteur=secteur,
                message=high_risk_months[current_month],
                data={"month": current_month},
                actions=["Rappel des mesures saisonnières", "Communication aux équipes"]
            )
        return None
    
    def _check_benchmark_alert(self, secteur: str) -> Optional[Alert]:
        """Compare au benchmark provincial"""
        query = """
        WITH secteur_stats AS (
            SELECT 
                ROUND(100.0 * SUM(CASE WHEN "IND_LESION_TMS" = 'OUI' THEN 1 ELSE 0 END) / COUNT(*), 2) as tms_rate
            FROM cnesst_lesions_quebec
            WHERE "SECTEUR_SCIAN" ILIKE %s
        ),
        provincial_stats AS (
            SELECT 
                ROUND(100.0 * SUM(CASE WHEN "IND_LESION_TMS" = 'OUI' THEN 1 ELSE 0 END) / COUNT(*), 2) as tms_rate
            FROM cnesst_lesions_quebec
        )
        SELECT s.tms_rate as secteur, p.tms_rate as provincial
        FROM secteur_stats s, provincial_stats p
        """
        df = pd.read_sql(query, self.engine, params=(f"%{secteur}%",))
        
        if df.empty:
            return None
        
        secteur_rate = df.iloc[0]['secteur']
        provincial_rate = df.iloc[0]['provincial']
        ecart = secteur_rate - provincial_rate
        
        if ecart > 5:
            return Alert(
                id=self._generate_alert_id(),
                timestamp=datetime.now(),
                level="WARNING",
                type="BENCHMARK_EXCEEDED",
                secteur=secteur,
                message=f"TMS {ecart:.1f}% au-dessus de la moyenne provinciale",
                data={"secteur_rate": float(secteur_rate), "provincial_rate": float(provincial_rate)},
                actions=["Programme ergonomique", "Évaluation des postes"]
            )
        return None
    
    def _generate_alert_id(self) -> str:
        """Génère un ID unique pour l'alerte"""
        return hashlib.md5(f"{datetime.now().isoformat()}".encode()).hexdigest()[:12]
    
    # ========================================================================
    # 3. GÉNÉRATION DE SCÉNARIOS VR
    # ========================================================================
    
    def generate_vr_scenarios(self, secteur: str, count: int = 3) -> List[VRScenario]:
        """
        Génère des scénarios de formation VR basés sur les causes réelles.
        """
        print(f"🎮 Génération scénarios VR: {secteur}")
        
        # Top causes d'accidents
        query = """
        SELECT "AGENT_CAUSAL_LESION" as cause, COUNT(*) as total
        FROM cnesst_lesions_quebec
        WHERE "SECTEUR_SCIAN" ILIKE %s AND "AGENT_CAUSAL_LESION" IS NOT NULL
        GROUP BY "AGENT_CAUSAL_LESION"
        ORDER BY total DESC
        LIMIT %s
        """
        df = pd.read_sql(query, self.engine, params=(f"%{secteur}%", count))
        
        total_sector = pd.read_sql(
            "SELECT COUNT(*) FROM cnesst_lesions_quebec WHERE \"SECTEUR_SCIAN\" ILIKE %s",
            self.engine, params=(f"%{secteur}%",)
        ).iloc[0, 0]
        
        scenarios = []
        
        for idx, row in df.iterrows():
            cause = row['cause']
            incidents = int(row['total'])
            percentage = (incidents / total_sector * 100) if total_sector > 0 else 0
            
            scenario = VRScenario(
                id=f"VR-{secteur[:4].upper()}-{idx+1:03d}",
                title=self._generate_scenario_title(cause),
                duration_minutes=15 + (idx * 5),
                based_on_incidents=incidents,
                cause_type=cause,
                cause_percentage=round(percentage, 1),
                objectives=self._generate_objectives(cause),
                steps=self._generate_scenario_steps(cause),
                evaluation_criteria=self._generate_evaluation_criteria(cause)
            )
            scenarios.append(scenario)
        
        return scenarios
    
    def _generate_scenario_title(self, cause: str) -> str:
        """Génère un titre de scénario basé sur la cause"""
        titles = {
            "PERSONNE-TRAVAILLEUR BLESSE,MALADE": "Manutention sécuritaire et postures",
            "EFFORT EXCESSIF": "Techniques de levage et ergonomie",
            "CHUTE SUR LE MEME NIVEAU": "Circulation sécuritaire en milieu de travail",
            "VEHICULE": "Sécurité avec véhicules et chariots",
            "MACHINE": "Sécurité machine et procédures LOTO"
        }
        for key, title in titles.items():
            if key.lower() in cause.lower():
                return title
        return f"Prévention: {cause[:50]}"
    
    def _generate_objectives(self, cause: str) -> List[str]:
        """Génère les objectifs d'apprentissage"""
        base_objectives = [
            "Identifier les situations à risque",
            "Appliquer les mesures préventives",
            "Réagir correctement en cas d'incident"
        ]
        
        if "EFFORT" in cause.upper() or "MANUTENTION" in cause.upper():
            base_objectives.extend([
                "Utiliser les techniques de levage sécuritaire",
                "Reconnaître les limites de charge"
            ])
        elif "CHUTE" in cause.upper():
            base_objectives.extend([
                "Maintenir les voies de circulation dégagées",
                "Porter les EPI appropriés"
            ])
        elif "MACHINE" in cause.upper():
            base_objectives.extend([
                "Appliquer la procédure LOTO",
                "Vérifier les dispositifs de sécurité"
            ])
        
        return base_objectives[:5]
    
    def _generate_scenario_steps(self, cause: str) -> List[Dict]:
        """Génère les étapes du scénario"""
        return [
            {"step": 1, "action": "Introduction et contexte", "duration_sec": 60},
            {"step": 2, "action": "Identification des dangers", "duration_sec": 120},
            {"step": 3, "action": "Démonstration des bonnes pratiques", "duration_sec": 180},
            {"step": 4, "action": "Simulation interactive", "duration_sec": 300},
            {"step": 5, "action": "Quiz d'évaluation", "duration_sec": 120}
        ]
    
    def _generate_evaluation_criteria(self, cause: str) -> List[str]:
        """Génère les critères d'évaluation"""
        return [
            "Identification correcte des dangers (min 80%)",
            "Application des mesures préventives",
            "Score quiz final ≥ 70%",
            "Temps de réaction approprié"
        ]
    
    # ========================================================================
    # 4. PROFIL DE RISQUE TRAVAILLEUR
    # ========================================================================
    
    def get_worker_risk_profile(
        self, 
        secteur: str,
        age_group: str,
        gender: str
    ) -> WorkerRiskProfile:
        """
        Génère un profil de risque personnalisé pour un travailleur.
        """
        print(f"👤 Profil de risque: {gender}, {age_group}, {secteur}")
        
        # Stats pour ce profil démographique
        query = """
        SELECT 
            COUNT(*) as total,
            ROUND(100.0 * SUM(CASE WHEN "IND_LESION_TMS" = 'OUI' THEN 1 ELSE 0 END) / COUNT(*), 2) as tms,
            ROUND(100.0 * SUM(CASE WHEN "IND_LESION_PSY" = 'OUI' THEN 1 ELSE 0 END) / COUNT(*), 2) as psy,
            ROUND(100.0 * SUM(CASE WHEN "IND_LESION_MACHINE" = 'OUI' THEN 1 ELSE 0 END) / COUNT(*), 2) as machine
        FROM cnesst_lesions_quebec
        WHERE "SECTEUR_SCIAN" ILIKE %s
          AND "GROUPE_AGE" ILIKE %s
          AND "SEXE_PERS_PHYS" ILIKE %s
        """
        df = pd.read_sql(query, self.engine, params=(f"%{secteur}%", f"%{age_group}%", f"%{gender}%"))
        
        # Stats secteur global pour comparaison
        sector_query = """
        SELECT 
            ROUND(100.0 * SUM(CASE WHEN "IND_LESION_TMS" = 'OUI' THEN 1 ELSE 0 END) / COUNT(*), 2) as tms,
            ROUND(100.0 * SUM(CASE WHEN "IND_LESION_PSY" = 'OUI' THEN 1 ELSE 0 END) / COUNT(*), 2) as psy
        FROM cnesst_lesions_quebec
        WHERE "SECTEUR_SCIAN" ILIKE %s
        """
        sector_df = pd.read_sql(sector_query, self.engine, params=(f"%{secteur}%",))
        
        if df.empty or df.iloc[0]['total'] == 0:
            return WorkerRiskProfile(
                age_group=age_group,
                gender=gender,
                secteur=secteur,
                risk_score=50,
                specific_risks=[],
                recommended_training=["Formation SST générale"],
                medical_surveillance=["Examen médical annuel"]
            )
        
        row = df.iloc[0]
        sector_row = sector_df.iloc[0]
        
        # Calcul du score de risque
        tms_rate = float(row['tms']) if row['tms'] else 0
        psy_rate = float(row['psy']) if row['psy'] else 0
        
        risk_score = min(100, 40 + tms_rate + (psy_rate * 3))
        
        # Risques spécifiques
        specific_risks = []
        sector_tms = float(sector_row['tms']) if sector_row['tms'] else 0
        
        if tms_rate > sector_tms:
            specific_risks.append({
                "type": "TMS",
                "your_rate": tms_rate,
                "sector_avg": sector_tms,
                "status": "⚠️ AU-DESSUS"
            })
        
        if psy_rate > 5:
            specific_risks.append({
                "type": "PSY",
                "your_rate": psy_rate,
                "sector_avg": float(sector_row['psy']) if sector_row['psy'] else 0,
                "status": "⚠️ ÉLEVÉ"
            })
        
        # Formations recommandées
        training = []
        if tms_rate > 25:
            training.append("Ergonomie et manutention (PRIORITÉ HAUTE)")
        if "45" in age_group or "55" in age_group:
            training.append("Adaptation du poste de travail")
        training.append("Formation SST de base")
        
        # Surveillance médicale
        medical = ["Examen médical annuel"]
        if tms_rate > 30:
            medical.append("Évaluation musculo-squelettique")
        if psy_rate > 5:
            medical.append("Suivi psychologique")
        
        return WorkerRiskProfile(
            age_group=age_group,
            gender=gender,
            secteur=secteur,
            risk_score=round(risk_score, 1),
            specific_risks=specific_risks,
            recommended_training=training,
            medical_surveillance=medical
        )
    
    # ========================================================================
    # 5. CALCUL ASSURANCE / ROI
    # ========================================================================
    
    def calculate_insurance_quote(
        self,
        secteur: str,
        employee_count: int,
        historical_incidents: int = 0
    ) -> InsuranceQuote:
        """
        Calcule une estimation de prime d'assurance basée sur les données réelles.
        """
        print(f"💰 Calcul assurance: {secteur}, {employee_count} employés")
        
        # Coûts moyens (basés sur données CNESST)
        COUT_MOYEN_INCIDENT = 12500  # $ CAD
        COUT_TMS = 15000
        COUT_PSY = 28000
        JOURS_PERDUS_MOYEN = 45
        COUT_JOUR = 350
        
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
        
        # Taux par 1000 employés
        rate_per_1000 = (total / 7) / 100000 * 1000  # Approximation
        
        # Incidents estimés pour cette entreprise
        expected_incidents = (rate_per_1000 / 1000) * employee_count
        if historical_incidents > 0:
            expected_incidents = (expected_incidents + historical_incidents) / 2
        
        # Coût annuel estimé
        tms_ratio = tms / total if total > 0 else 0.25
        psy_ratio = psy / total if total > 0 else 0.04
        
        estimated_cost = expected_incidents * (
            COUT_MOYEN_INCIDENT * (1 - tms_ratio - psy_ratio) +
            COUT_TMS * tms_ratio +
            COUT_PSY * psy_ratio
        )
        
        # Ajout coûts indirects
        indirect_cost = expected_incidents * JOURS_PERDUS_MOYEN * COUT_JOUR
        total_cost = estimated_cost + indirect_cost
        
        # Prime suggérée (40-60% du risque estimé)
        premium_low = total_cost * 0.40
        premium_high = total_cost * 0.60
        
        # Potentiel d'économie
        reduction_scenarios = {
            "reduction_10pct_incidents": total_cost * 0.10,
            "reduction_tms_to_sector_avg": estimated_cost * tms_ratio * 0.20,
            "programme_prevention": premium_high * 0.15
        }
        
        return InsuranceQuote(
            secteur=secteur,
            employee_count=employee_count,
            historical_incidents=historical_incidents,
            estimated_annual_cost=round(total_cost),
            suggested_premium_range=(round(premium_low), round(premium_high)),
            savings_potential=reduction_scenarios
        )
    
    # ========================================================================
    # 6. RAPPORT COMPLET
    # ========================================================================
    
    def generate_intelligence_report(self, secteur: str, employee_count: int = 100) -> Dict:
        """
        Génère un rapport d'intelligence complet pour un secteur.
        """
        print(f"\n{'='*60}")
        print(f"🧠 RAPPORT D'INTELLIGENCE SAFETWIN X5")
        print(f"   Secteur: {secteur}")
        print(f"{'='*60}\n")
        
        report = {
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "secteur": secteur,
                "source": "Safety Graph - 2,853,583 records"
            },
            "prediction": None,
            "alerts": [],
            "vr_scenarios": [],
            "insurance": None
        }
        
        # 1. Prédiction
        prediction = self.predict_incidents(secteur, employee_count)
        report["prediction"] = {
            "risk_score": prediction.risk_score,
            "probability_30_days": prediction.probability_incident_30_days,
            "top_risks": prediction.top_risks,
            "recommendations": prediction.recommended_actions,
            "confidence": prediction.confidence
        }
        
        # 2. Alertes
        alerts = self.check_alerts(secteur)
        report["alerts"] = [
            {
                "level": a.level,
                "type": a.type,
                "message": a.message,
                "actions": a.actions
            }
            for a in alerts
        ]
        
        # 3. Scénarios VR
        scenarios = self.generate_vr_scenarios(secteur, 3)
        report["vr_scenarios"] = [
            {
                "id": s.id,
                "title": s.title,
                "duration": s.duration_minutes,
                "based_on": s.based_on_incidents,
                "cause_pct": s.cause_percentage
            }
            for s in scenarios
        ]
        
        # 4. Assurance
        quote = self.calculate_insurance_quote(secteur, employee_count)
        report["insurance"] = {
            "estimated_annual_cost": quote.estimated_annual_cost,
            "premium_range": quote.suggested_premium_range,
            "savings_potential": quote.savings_potential
        }
        
        return report


# ============================================================================
# UTILITAIRES D'AFFICHAGE
# ============================================================================

def print_prediction(pred: PredictionResult):
    """Affiche une prédiction de manière formatée"""
    print(f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  🔮 PRÉDICTION - {pred.secteur[:50]:<50} ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  Score de risque:          {pred.risk_score:>6.1f}/100                                    ║
║  Probabilité incident 30j: {pred.probability_incident_30_days:>6.1f}%                                     ║
║  Confiance:                {pred.confidence:<10}                                     ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  TOP RISQUES:                                                                ║""")
    for risk in pred.top_risks:
        print(f"║    • {risk['type']:<10} {risk['rate']:>5.1f}%                                              ║")
    print("╠══════════════════════════════════════════════════════════════════════════════╣")
    print("║  ACTIONS RECOMMANDÉES:                                                       ║")
    for action in pred.recommended_actions[:4]:
        print(f"║    {action[:70]:<70} ║")
    print("╚══════════════════════════════════════════════════════════════════════════════╝")


def print_alerts(alerts: List[Alert]):
    """Affiche les alertes"""
    if not alerts:
        print("✅ Aucune alerte active")
        return
    
    for alert in alerts:
        icon = "🔴" if alert.level == "CRITICAL" else "🟡" if alert.level == "WARNING" else "🔵"
        print(f"""
┌──────────────────────────────────────────────────────────────────┐
│ {icon} ALERTE {alert.level:<10} - {alert.type:<30} │
├──────────────────────────────────────────────────────────────────┤
│ {alert.message:<64} │
│ Actions: {', '.join(alert.actions[:2]):<55} │
└──────────────────────────────────────────────────────────────────┘""")


# ============================================================================
# CLI
# ============================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="SafeTwin X5 Predictive Intelligence")
    parser.add_argument("--secteur", "-s", required=True, help="Secteur SCIAN")
    parser.add_argument("--employees", "-e", type=int, default=100, help="Nombre d'employés")
    parser.add_argument("--predict", "-p", action="store_true", help="Prédiction ML")
    parser.add_argument("--alerts", "-a", action="store_true", help="Vérifier alertes")
    parser.add_argument("--scenarios", action="store_true", help="Générer scénarios VR")
    parser.add_argument("--insurance", "-i", action="store_true", help="Calcul assurance")
    parser.add_argument("--full", "-f", action="store_true", help="Rapport complet")
    parser.add_argument("--json", "-j", action="store_true", help="Output JSON")
    
    args = parser.parse_args()
    
    intel = SafeTwinPredictiveIntelligence()
    
    if args.predict:
        result = intel.predict_incidents(args.secteur, args.employees)
        if args.json:
            print(json.dumps(result.__dict__, indent=2, ensure_ascii=False, default=str))
        else:
            print_prediction(result)
    
    elif args.alerts:
        alerts = intel.check_alerts(args.secteur)
        if args.json:
            print(json.dumps([a.__dict__ for a in alerts], indent=2, ensure_ascii=False, default=str))
        else:
            print_alerts(alerts)
    
    elif args.scenarios:
        scenarios = intel.generate_vr_scenarios(args.secteur)
        for s in scenarios:
            print(f"\n🎮 {s.id}: {s.title}")
            print(f"   Durée: {s.duration_minutes} min | Basé sur: {s.based_on_incidents:,} incidents ({s.cause_percentage}%)")
    
    elif args.insurance:
        quote = intel.calculate_insurance_quote(args.secteur, args.employees)
        print(f"\n💰 ESTIMATION ASSURANCE: {args.secteur}")
        print(f"   Coût annuel estimé: ${quote.estimated_annual_cost:,}")
        print(f"   Prime suggérée: ${quote.suggested_premium_range[0]:,} - ${quote.suggested_premium_range[1]:,}")
    
    elif args.full:
        report = intel.generate_intelligence_report(args.secteur, args.employees)
        if args.json:
            print(json.dumps(report, indent=2, ensure_ascii=False, default=str))
        else:
            print(json.dumps(report, indent=2, ensure_ascii=False, default=str))
    
    else:
        parser.print_help()
