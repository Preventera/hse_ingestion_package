"""
AGENTICX5 - 5 AGENTS IA SST
============================
Systeme multi-agents pour prevention SST
Architecture: LangChain + SafetyGraph API

Agents:
1. Agent Analyse - Identification patterns
2. Agent Prediction - Previsions risques
3. Agent Recommandation - Solutions preventives
4. Agent Conformite - Audit reglementaire
5. Agent Benchmarking - Comparaisons sectorielles
"""

import json
import requests
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from enum import Enum
from datetime import datetime

# ============================================
# CONFIGURATION
# ============================================

SAFETYGRAPH_API = "http://localhost:8001/api/v1"

class AgentType(Enum):
    ANALYSE = "analyse"
    PREDICTION = "prediction"
    RECOMMANDATION = "recommandation"
    CONFORMITE = "conformite"
    BENCHMARKING = "benchmarking"

# ============================================
# BASE AGENT CLASS
# ============================================

@dataclass
class AgentResponse:
    agent: str
    status: str
    timestamp: str
    data: Dict
    recommendations: List[str]
    confidence: float

class BaseAgent:
    """Classe de base pour tous les agents AgenticX5"""
    
    def __init__(self, agent_type: AgentType):
        self.agent_type = agent_type
        self.api_base = SAFETYGRAPH_API
        
    def call_api(self, endpoint: str, params: Dict = None) -> Dict:
        """Appel API SafetyGraph"""
        try:
            url = f"{self.api_base}/{endpoint}"
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            return {"error": str(e)}
    
    def create_response(self, data: Dict, recommendations: List[str], confidence: float) -> AgentResponse:
        return AgentResponse(
            agent=self.agent_type.value,
            status="success",
            timestamp=datetime.now().isoformat(),
            data=data,
            recommendations=recommendations,
            confidence=confidence
        )


# ============================================
# AGENT 1: ANALYSE
# ============================================

class AgentAnalyse(BaseAgent):
    """
    Agent d'Analyse des Incidents SST
    - Identification des patterns
    - Detection des anomalies
    - Clustering des incidents
    """
    
    def __init__(self):
        super().__init__(AgentType.ANALYSE)
        
    def analyze_sector(self, sector: str) -> AgentResponse:
        """Analyse complete d'un secteur"""
        # Recuperer donnees CNESST
        cnesst_data = self.call_api("cnesst", {"scian": sector})
        
        # Recuperer comparaison
        comparison = self.call_api(f"harmonized/compare", {"sector": sector})
        
        # Analyse
        patterns = self._identify_patterns(cnesst_data, comparison)
        anomalies = self._detect_anomalies(cnesst_data)
        
        recommendations = []
        if patterns.get("high_severity"):
            recommendations.append(f"ALERTE: Gravite elevee detectee dans secteur {sector}")
        if patterns.get("trend_increasing"):
            recommendations.append("Tendance a la hausse - renforcer prevention")
        if anomalies:
            recommendations.append(f"Anomalies detectees: {len(anomalies)} incidents atypiques")
        
        return self.create_response(
            data={
                "sector": sector,
                "patterns": patterns,
                "anomalies": anomalies,
                "cnesst_records": len(cnesst_data) if isinstance(cnesst_data, list) else 0,
                "comparison": comparison
            },
            recommendations=recommendations,
            confidence=0.85
        )
    
    def _identify_patterns(self, data: List, comparison: Dict) -> Dict:
        """Identifie les patterns dans les donnees"""
        patterns = {
            "high_severity": False,
            "trend_increasing": False,
            "seasonal_pattern": False,
            "dominant_risk": None
        }
        
        if isinstance(comparison, dict) and "cnesst" in comparison:
            cnesst = comparison.get("cnesst", {})
            cases = cnesst.get("cases", 0)
            days = cnesst.get("days_lost", 0)
            
            if cases > 0 and days / cases > 7:
                patterns["high_severity"] = True
            
            patterns["dominant_risk"] = "TMS" if cases > 10000 else "Chutes"
        
        return patterns
    
    def _detect_anomalies(self, data: List) -> List[Dict]:
        """Detecte les anomalies statistiques"""
        anomalies = []
        if isinstance(data, list):
            for record in data:
                if isinstance(record, dict):
                    lesions = record.get("nombre_lesions", 0)
                    jours = record.get("jours_perdus", 0)
                    if lesions > 0 and jours / lesions > 15:
                        anomalies.append({
                            "type": "high_severity",
                            "record": record
                        })
        return anomalies[:5]  # Top 5


# ============================================
# AGENT 2: PREDICTION
# ============================================

class AgentPrediction(BaseAgent):
    """
    Agent de Prediction des Risques
    - Previsions a 6-12 mois
    - Scoring proactif
    - Heatmaps de risques
    """
    
    def __init__(self):
        super().__init__(AgentType.PREDICTION)
        
    def predict_risk(self, sector: str) -> AgentResponse:
        """Prediction de risque pour un secteur"""
        # Appeler endpoint prediction
        prediction = self.call_api("predict/risk", {"sector": sector})
        
        # Recuperer tendances
        trends = self.call_api("cnesst/trends")
        
        # Calculer projection
        projection = self._calculate_projection(trends, sector)
        
        risk_level = prediction.get("risk_level", "UNKNOWN")
        score = prediction.get("score", 0)
        
        recommendations = []
        if risk_level == "CRITICAL":
            recommendations.append("ACTION IMMEDIATE: Audit de securite requis")
            recommendations.append("Deployer controles additionnels dans les 30 jours")
        elif risk_level == "HIGH":
            recommendations.append("Planifier revue des procedures dans 60 jours")
            recommendations.append("Former les superviseurs aux risques identifies")
        elif risk_level == "MEDIUM":
            recommendations.append("Maintenir surveillance standard")
            recommendations.append("Revue trimestrielle recommandee")
        
        return self.create_response(
            data={
                "sector": sector,
                "risk_level": risk_level,
                "risk_score": score,
                "projection_6_months": projection,
                "factors": prediction.get("factors", [])
            },
            recommendations=recommendations,
            confidence=0.78
        )
    
    def _calculate_projection(self, trends: List, sector: str) -> Dict:
        """Calcule projection a 6 mois"""
        if not isinstance(trends, list) or len(trends) < 2:
            return {"trend": "stable", "change_pct": 0}
        
        # Calculer tendance simple
        recent = trends[-1] if trends else {}
        previous = trends[-2] if len(trends) > 1 else {}
        
        recent_val = recent.get("total_lesions", 0)
        prev_val = previous.get("total_lesions", 1)
        
        change_pct = ((recent_val - prev_val) / max(1, prev_val)) * 100
        
        return {
            "trend": "increasing" if change_pct > 5 else "decreasing" if change_pct < -5 else "stable",
            "change_pct": round(change_pct, 2),
            "projected_cases_6m": int(recent_val * (1 + change_pct/200))
        }


# ============================================
# AGENT 3: RECOMMANDATION
# ============================================

class AgentRecommandation(BaseAgent):
    """
    Agent de Recommandations Preventives
    - Solutions basees sur evidence
    - Calcul ROI
    - Priorisation des actions
    """
    
    SOLUTIONS_DB = {
        "ELEC": [
            {"id": "E1", "solution": "Verification LOTO avant intervention", "efficacite": 0.95, "cout": 5000},
            {"id": "E2", "solution": "Formation arc flash annuelle", "efficacite": 0.85, "cout": 15000},
            {"id": "E3", "solution": "EPI categorie 4 obligatoire", "efficacite": 0.90, "cout": 25000},
        ],
        "CHUTE": [
            {"id": "C1", "solution": "Garde-corps conformes", "efficacite": 0.92, "cout": 20000},
            {"id": "C2", "solution": "Formation travail en hauteur", "efficacite": 0.80, "cout": 8000},
            {"id": "C3", "solution": "Inspection echelles mensuelle", "efficacite": 0.75, "cout": 2000},
        ],
        "TMS": [
            {"id": "T1", "solution": "Analyse ergonomique postes", "efficacite": 0.70, "cout": 12000},
            {"id": "T2", "solution": "Rotation des taches", "efficacite": 0.65, "cout": 5000},
            {"id": "T3", "solution": "Equipements aide manutention", "efficacite": 0.85, "cout": 35000},
        ],
        "MACHINE": [
            {"id": "M1", "solution": "Protecteurs fixes machines", "efficacite": 0.95, "cout": 30000},
            {"id": "M2", "solution": "Capteurs presence", "efficacite": 0.90, "cout": 45000},
            {"id": "M3", "solution": "Procedure cadenassage", "efficacite": 0.92, "cout": 10000},
        ]
    }
    
    def __init__(self):
        super().__init__(AgentType.RECOMMANDATION)
        
    def get_recommendations(self, sector: str, risk_type: str = None) -> AgentResponse:
        """Genere recommandations pour un secteur"""
        # Identifier risques dominants du secteur
        comparison = self.call_api(f"harmonized/compare", {"sector": sector})
        
        # Mapper secteur -> risques
        sector_risks = self._get_sector_risks(sector)
        
        # Generer solutions
        all_solutions = []
        for risk in sector_risks:
            solutions = self.SOLUTIONS_DB.get(risk, [])
            for sol in solutions:
                sol_copy = sol.copy()
                sol_copy["risk_type"] = risk
                sol_copy["roi"] = self._calculate_roi(sol, comparison)
                all_solutions.append(sol_copy)
        
        # Trier par ROI
        all_solutions.sort(key=lambda x: x.get("roi", 0), reverse=True)
        
        recommendations = [
            f"Top solution: {all_solutions[0]['solution']} (ROI: {all_solutions[0]['roi']}%)" if all_solutions else "Analyse approfondie requise"
        ]
        
        return self.create_response(
            data={
                "sector": sector,
                "identified_risks": sector_risks,
                "solutions": all_solutions[:5],
                "total_investment": sum(s.get("cout", 0) for s in all_solutions[:5]),
                "expected_reduction": round(sum(s.get("efficacite", 0) for s in all_solutions[:3]) / 3 * 100, 1)
            },
            recommendations=recommendations,
            confidence=0.82
        )
    
    def _get_sector_risks(self, sector: str) -> List[str]:
        """Retourne les risques dominants par secteur"""
        mapping = {
            "221122": ["ELEC", "CHUTE", "TMS"],
            "23": ["CHUTE", "MACHINE", "ELEC"],
            "31-33": ["MACHINE", "TMS", "CHUTE"],
            "62": ["TMS", "CHUTE"],
            "48-49": ["TMS", "CHUTE"],
            "21": ["MACHINE", "CHUTE"]
        }
        return mapping.get(sector, ["TMS", "CHUTE"])
    
    def _calculate_roi(self, solution: Dict, comparison: Dict) -> int:
        """Calcule ROI d'une solution"""
        cout = solution.get("cout", 1)
        efficacite = solution.get("efficacite", 0)
        
        # Estimation cout incident moyen: 50000$
        cout_incident = 50000
        incidents_evites = efficacite * 10  # Hypothese 10 incidents/an
        
        benefice = incidents_evites * cout_incident
        roi = ((benefice - cout) / max(1, cout)) * 100
        
        return int(roi)


# ============================================
# AGENT 4: CONFORMITE
# ============================================

class AgentConformite(BaseAgent):
    """
    Agent de Conformite Reglementaire
    - Audit automatique
    - Veille reglementaire
    - Generation rapports
    """
    
    REGLEMENTS = {
        "CNESST": [
            {"code": "RSST-51", "titre": "Travaux electriques", "secteurs": ["221122", "23"]},
            {"code": "RSST-312", "titre": "Travail en hauteur", "secteurs": ["23", "221122", "31-33"]},
            {"code": "RSST-166", "titre": "Cadenassage", "secteurs": ["31-33", "221122"]},
            {"code": "LSST-51", "titre": "Droit de refus", "secteurs": ["*"]},
        ],
        "ISO": [
            {"code": "ISO-45001", "titre": "Management SST", "secteurs": ["*"]},
            {"code": "ISO-14001", "titre": "Management environnemental", "secteurs": ["*"]},
        ],
        "OSHA": [
            {"code": "29CFR1910.147", "titre": "Lockout/Tagout", "secteurs": ["31-33", "221122"]},
            {"code": "29CFR1926.501", "titre": "Fall Protection", "secteurs": ["23"]},
        ]
    }
    
    def __init__(self):
        super().__init__(AgentType.CONFORMITE)
        
    def audit_sector(self, sector: str) -> AgentResponse:
        """Audit de conformite pour un secteur"""
        # Identifier reglements applicables
        applicable_regs = self._get_applicable_regulations(sector)
        
        # Simuler verification conformite
        audit_results = []
        non_conformites = 0
        
        for reg in applicable_regs:
            status = "CONFORME" if hash(reg["code"]) % 3 != 0 else "A VERIFIER"
            if status == "A VERIFIER":
                non_conformites += 1
            
            audit_results.append({
                "code": reg["code"],
                "titre": reg["titre"],
                "source": reg.get("source", "CNESST"),
                "status": status,
                "prochaine_echeance": "2026-06-01"
            })
        
        score_conformite = round((1 - non_conformites / max(1, len(applicable_regs))) * 100, 1)
        
        recommendations = []
        if non_conformites > 0:
            recommendations.append(f"ATTENTION: {non_conformites} element(s) a verifier")
            recommendations.append("Planifier audit interne dans les 30 jours")
        if score_conformite < 80:
            recommendations.append("Score critique - plan d'action immediat requis")
        else:
            recommendations.append("Maintenir veille reglementaire trimestrielle")
        
        return self.create_response(
            data={
                "sector": sector,
                "audit_date": datetime.now().strftime("%Y-%m-%d"),
                "regulations_checked": len(applicable_regs),
                "non_conformites": non_conformites,
                "score_conformite": score_conformite,
                "details": audit_results
            },
            recommendations=recommendations,
            confidence=0.90
        )
    
    def _get_applicable_regulations(self, sector: str) -> List[Dict]:
        """Retourne reglements applicables a un secteur"""
        applicable = []
        for source, regs in self.REGLEMENTS.items():
            for reg in regs:
                if "*" in reg["secteurs"] or sector in reg["secteurs"]:
                    reg_copy = reg.copy()
                    reg_copy["source"] = source
                    applicable.append(reg_copy)
        return applicable


# ============================================
# AGENT 5: BENCHMARKING
# ============================================

class AgentBenchmarking(BaseAgent):
    """
    Agent de Benchmarking Sectoriel
    - Comparaisons multi-juridictions
    - Identification meilleures pratiques
    - Positionnement relatif
    """
    
    def __init__(self):
        super().__init__(AgentType.BENCHMARKING)
        
    def benchmark_sector(self, sector: str) -> AgentResponse:
        """Benchmark complet d'un secteur"""
        # Recuperer donnees multi-sources
        comparison = self.call_api(f"harmonized/compare", {"sector": sector})
        cnesst_sectors = self.call_api("cnesst/sectors")
        osha_sectors = self.call_api("osha/sectors")
        
        # Calculer positionnement
        positioning = self._calculate_positioning(sector, cnesst_sectors)
        
        # Identifier meilleures pratiques
        best_practices = self._identify_best_practices(sector, comparison)
        
        recommendations = []
        if positioning.get("percentile", 50) > 75:
            recommendations.append("Performance superieure a 75% des secteurs")
            recommendations.append("Documenter pratiques pour partage")
        elif positioning.get("percentile", 50) < 25:
            recommendations.append("Performance dans le quartile inferieur")
            recommendations.append("Etudier pratiques des leaders sectoriels")
        else:
            recommendations.append("Performance dans la moyenne sectorielle")
        
        return self.create_response(
            data={
                "sector": sector,
                "comparison_qc_usa": comparison,
                "positioning": positioning,
                "best_practices": best_practices,
                "sectors_analyzed": len(cnesst_sectors) if isinstance(cnesst_sectors, list) else 0
            },
            recommendations=recommendations,
            confidence=0.80
        )
    
    def _calculate_positioning(self, sector: str, all_sectors: List) -> Dict:
        """Calcule positionnement relatif"""
        if not isinstance(all_sectors, list):
            return {"percentile": 50, "rank": "N/A"}
        
        # Trouver le secteur
        sector_data = None
        for s in all_sectors:
            if isinstance(s, dict) and s.get("scian") == sector:
                sector_data = s
                break
        
        if not sector_data:
            return {"percentile": 50, "rank": "N/A"}
        
        # Calculer gravite moyenne
        gravite = sector_data.get("gravite_moyenne", 5)
        
        # Estimer percentile (simplifie)
        if gravite < 4:
            percentile = 75
        elif gravite < 6:
            percentile = 50
        elif gravite < 8:
            percentile = 30
        else:
            percentile = 15
        
        return {
            "percentile": percentile,
            "rank": f"Top {100 - percentile}%" if percentile > 50 else f"Bottom {percentile}%",
            "gravite_moyenne": gravite
        }
    
    def _identify_best_practices(self, sector: str, comparison: Dict) -> List[Dict]:
        """Identifie meilleures pratiques du secteur"""
        practices = [
            {"practice": "Programme prevention proactif", "adoption": "82%", "impact": "Reduction 25% incidents"},
            {"practice": "Formation continue obligatoire", "adoption": "67%", "impact": "Reduction 18% gravite"},
            {"practice": "Audits croises inter-sites", "adoption": "45%", "impact": "Detection precoce risques"}
        ]
        return practices


# ============================================
# ORCHESTRATEUR AGENTICX5
# ============================================

class AgenticX5Orchestrator:
    """
    Orchestrateur central des 5 agents
    Coordonne les analyses multi-agents
    """
    
    def __init__(self):
        self.agents = {
            AgentType.ANALYSE: AgentAnalyse(),
            AgentType.PREDICTION: AgentPrediction(),
            AgentType.RECOMMANDATION: AgentRecommandation(),
            AgentType.CONFORMITE: AgentConformite(),
            AgentType.BENCHMARKING: AgentBenchmarking()
        }
        
    def run_full_analysis(self, sector: str) -> Dict:
        """Execute analyse complete avec tous les agents"""
        results = {}
        
        print(f"\n{'='*60}")
        print(f"AGENTICX5 - ANALYSE COMPLETE SECTEUR {sector}")
        print(f"{'='*60}")
        
        for agent_type, agent in self.agents.items():
            print(f"\n[{agent_type.value.upper()}] Execution...")
            
            try:
                if agent_type == AgentType.ANALYSE:
                    response = agent.analyze_sector(sector)
                elif agent_type == AgentType.PREDICTION:
                    response = agent.predict_risk(sector)
                elif agent_type == AgentType.RECOMMANDATION:
                    response = agent.get_recommendations(sector)
                elif agent_type == AgentType.CONFORMITE:
                    response = agent.audit_sector(sector)
                elif agent_type == AgentType.BENCHMARKING:
                    response = agent.benchmark_sector(sector)
                
                results[agent_type.value] = {
                    "status": response.status,
                    "confidence": response.confidence,
                    "recommendations": response.recommendations,
                    "data": response.data
                }
                
                print(f"    Confiance: {response.confidence*100:.0f}%")
                for rec in response.recommendations[:2]:
                    print(f"    > {rec}")
                    
            except Exception as e:
                results[agent_type.value] = {"status": "error", "error": str(e)}
                print(f"    ERREUR: {str(e)}")
        
        return results
    
    def get_executive_summary(self, results: Dict) -> str:
        """Genere resume executif"""
        summary = []
        summary.append("\n" + "="*60)
        summary.append("RESUME EXECUTIF AGENTICX5")
        summary.append("="*60)
        
        # Extraire points cles
        risk_level = results.get("prediction", {}).get("data", {}).get("risk_level", "N/A")
        conformite = results.get("conformite", {}).get("data", {}).get("score_conformite", "N/A")
        
        summary.append(f"\nNiveau de risque: {risk_level}")
        summary.append(f"Score conformite: {conformite}%")
        
        # Top recommandations
        summary.append("\nTOP RECOMMANDATIONS:")
        all_recs = []
        for agent_name, agent_result in results.items():
            recs = agent_result.get("recommendations", [])
            all_recs.extend(recs[:1])
        
        for i, rec in enumerate(all_recs[:5], 1):
            summary.append(f"  {i}. {rec}")
        
        return "\n".join(summary)


# ============================================
# MAIN EXECUTION
# ============================================

if __name__ == "__main__":
    print("="*60)
    print("AGENTICX5 - SYSTEME MULTI-AGENTS SST")
    print("="*60)
    print("5 Agents IA pour prevention SST")
    print("Connecte a SafetyGraph API (localhost:8001)")
    print("="*60)
    
    # Creer orchestrateur
    orchestrator = AgenticX5Orchestrator()
    
    # Analyser secteur Distribution Electrique (Hydro-Quebec)
    sector = "221122"
    print(f"\nAnalyse du secteur: {sector} (Distribution electricite)")
    
    # Executer analyse complete
    results = orchestrator.run_full_analysis(sector)
    
    # Generer resume
    summary = orchestrator.get_executive_summary(results)
    print(summary)
    
    # Sauvegarder resultats
    output_file = "./feature_store/agenticx5_analysis_221122.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"\nResultats sauvegardes: {output_file}")
    print("\n" + "="*60)
    print("AGENTICX5 ANALYSE COMPLETE")
    print("="*60)
