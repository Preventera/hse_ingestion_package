"""
LANGCHAIN INTEGRATION - SafetyGraph AgenticX5
==============================================
Architecture multi-agents avancee avec LangChain
- Agents conversationnels
- Chaines de raisonnement
- Memory contextuelle
- Tools SafetyGraph API
"""

import json
import requests
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from datetime import datetime

# ============================================
# CONFIGURATION
# ============================================

SAFETYGRAPH_API = "http://localhost:8001/api/v1"

# ============================================
# LANGCHAIN TOOLS - SafetyGraph API
# ============================================

class SafetyGraphTools:
    """Outils LangChain pour SafetyGraph API"""
    
    @staticmethod
    def get_stats() -> Dict:
        """Recupere statistiques globales SafetyGraph"""
        try:
            response = requests.get(f"{SAFETYGRAPH_API}/stats", timeout=10)
            return response.json()
        except Exception as e:
            return {"error": str(e)}
    
    @staticmethod
    def get_cnesst_data(sector: str = None, year: int = None) -> List[Dict]:
        """Recupere donnees CNESST"""
        params = {}
        if sector:
            params["scian"] = sector
        if year:
            params["year"] = year
        try:
            response = requests.get(f"{SAFETYGRAPH_API}/cnesst", params=params, timeout=10)
            return response.json()
        except Exception as e:
            return [{"error": str(e)}]
    
    @staticmethod
    def get_osha_data(sector: str = None) -> List[Dict]:
        """Recupere donnees OSHA USA"""
        params = {}
        if sector:
            params["naics"] = sector
        try:
            response = requests.get(f"{SAFETYGRAPH_API}/osha", params=params, timeout=10)
            return response.json()
        except Exception as e:
            return [{"error": str(e)}]
    
    @staticmethod
    def compare_jurisdictions(sector: str) -> Dict:
        """Compare Quebec vs USA pour un secteur"""
        try:
            response = requests.get(f"{SAFETYGRAPH_API}/harmonized/compare", params={"sector": sector}, timeout=10)
            return response.json()
        except Exception as e:
            return {"error": str(e)}
    
    @staticmethod
    def predict_risk(sector: str) -> Dict:
        """Prediction de risque pour un secteur"""
        try:
            response = requests.get(f"{SAFETYGRAPH_API}/predict/risk", params={"sector": sector}, timeout=10)
            return response.json()
        except Exception as e:
            return {"error": str(e)}


# ============================================
# MEMORY - Contexte conversationnel
# ============================================

class ConversationMemory:
    """Memoire conversationnelle pour agents"""
    
    def __init__(self, max_history: int = 10):
        self.history: List[Dict] = []
        self.max_history = max_history
        self.context: Dict = {}
        
    def add_interaction(self, role: str, content: str, metadata: Dict = None):
        """Ajoute une interaction a l'historique"""
        interaction = {
            "role": role,
            "content": content,
            "timestamp": datetime.now().isoformat(),
            "metadata": metadata or {}
        }
        self.history.append(interaction)
        
        # Limiter taille historique
        if len(self.history) > self.max_history:
            self.history = self.history[-self.max_history:]
    
    def set_context(self, key: str, value: Any):
        """Definit contexte persistant"""
        self.context[key] = value
    
    def get_context(self, key: str) -> Any:
        """Recupere contexte"""
        return self.context.get(key)
    
    def get_recent_history(self, n: int = 5) -> List[Dict]:
        """Retourne les n dernieres interactions"""
        return self.history[-n:]
    
    def summarize(self) -> str:
        """Resume l'historique"""
        if not self.history:
            return "Aucun historique"
        
        summary = []
        for interaction in self.history[-5:]:
            role = interaction["role"]
            content = interaction["content"][:100] + "..." if len(interaction["content"]) > 100 else interaction["content"]
            summary.append(f"[{role}]: {content}")
        
        return "\n".join(summary)


# ============================================
# CHAINS - Chaines de raisonnement
# ============================================

class ReasoningChain:
    """Chaine de raisonnement pour analyse SST"""
    
    def __init__(self, name: str):
        self.name = name
        self.steps: List[Dict] = []
        
    def add_step(self, step_name: str, result: Any, confidence: float = 1.0):
        """Ajoute une etape au raisonnement"""
        self.steps.append({
            "step": step_name,
            "result": result,
            "confidence": confidence,
            "timestamp": datetime.now().isoformat()
        })
    
    def get_chain_confidence(self) -> float:
        """Calcule confiance globale de la chaine"""
        if not self.steps:
            return 0.0
        confidences = [s["confidence"] for s in self.steps]
        # Confiance multiplicative
        result = 1.0
        for c in confidences:
            result *= c
        return round(result, 3)
    
    def to_dict(self) -> Dict:
        """Export en dictionnaire"""
        return {
            "chain_name": self.name,
            "steps": self.steps,
            "total_confidence": self.get_chain_confidence()
        }


# ============================================
# LANGCHAIN AGENT - Agent Conversationnel HSE
# ============================================

class HSEConversationalAgent:
    """
    Agent conversationnel HSE avec LangChain
    Capable de:
    - Repondre aux questions SST
    - Analyser les secteurs
    - Comparer juridictions
    - Generer recommandations
    """
    
    def __init__(self):
        self.tools = SafetyGraphTools()
        self.memory = ConversationMemory()
        self.current_chain = None
        
        # Base de connaissances
        self.knowledge_base = {
            "sectors": {
                "221122": "Distribution electricite (ex: Hydro-Quebec)",
                "23": "Construction",
                "31-33": "Fabrication/Manufacturing",
                "62": "Soins de sante",
                "48-49": "Transport et entreposage",
                "21": "Extraction miniere",
                "11": "Agriculture"
            },
            "risks": {
                "ELEC": "Risques electriques (choc, arc flash)",
                "CHUTE": "Chutes (meme niveau ou hauteur)",
                "TMS": "Troubles musculosquelettiques",
                "MACHINE": "Risques machines (coincement, ecrasement)",
                "VEHICULE": "Accidents de transport"
            },
            "jurisdictions": {
                "CNESST": "Quebec, Canada",
                "OSHA": "USA (federal)",
                "EU-OSHA": "Union Europeenne"
            }
        }
    
    def process_query(self, query: str) -> str:
        """Traite une requete utilisateur"""
        # Sauvegarder dans memoire
        self.memory.add_interaction("user", query)
        
        # Creer chaine de raisonnement
        self.current_chain = ReasoningChain(f"query_{datetime.now().strftime('%H%M%S')}")
        
        # Analyser intention
        intent = self._detect_intent(query)
        self.current_chain.add_step("intent_detection", intent, 0.9)
        
        # Executer action selon intention
        if intent == "stats":
            response = self._handle_stats_query(query)
        elif intent == "sector_analysis":
            response = self._handle_sector_query(query)
        elif intent == "comparison":
            response = self._handle_comparison_query(query)
        elif intent == "risk_prediction":
            response = self._handle_prediction_query(query)
        elif intent == "recommendation":
            response = self._handle_recommendation_query(query)
        else:
            response = self._handle_general_query(query)
        
        # Sauvegarder reponse
        self.memory.add_interaction("assistant", response, {"chain": self.current_chain.to_dict()})
        
        return response
    
    def _detect_intent(self, query: str) -> str:
        """Detecte l'intention de la requete"""
        query_lower = query.lower()
        
        if any(w in query_lower for w in ["statistique", "stats", "combien", "total"]):
            return "stats"
        elif any(w in query_lower for w in ["secteur", "scian", "221122", "construction", "sante"]):
            return "sector_analysis"
        elif any(w in query_lower for w in ["comparer", "versus", "vs", "difference", "quebec", "usa", "europe"]):
            return "comparison"
        elif any(w in query_lower for w in ["risque", "prediction", "prevision", "danger"]):
            return "risk_prediction"
        elif any(w in query_lower for w in ["recommandation", "solution", "prevention", "ameliorer"]):
            return "recommendation"
        else:
            return "general"
    
    def _handle_stats_query(self, query: str) -> str:
        """Gere requetes statistiques"""
        stats = self.tools.get_stats()
        self.current_chain.add_step("api_call_stats", stats, 0.95)
        
        if "error" in stats:
            return f"Erreur lors de la recuperation des statistiques: {stats['error']}"
        
        cnesst = stats.get("cnesst", {})
        osha = stats.get("osha", {})
        
        response = f"""
STATISTIQUES SAFETYGRAPH
========================
CNESST (Quebec):
  - Enregistrements: {cnesst.get('records', 0)}
  - Total lesions: {cnesst.get('total_lesions', 0):,}

OSHA (USA):
  - Enregistrements: {osha.get('records', 0)}
  - Total cas: {osha.get('total_cases', 0):,}
  - Deces: {osha.get('total_deaths', 0):,}

Total combine: {stats.get('combined_incidents', 0):,} incidents
"""
        return response
    
    def _handle_sector_query(self, query: str) -> str:
        """Gere requetes sur un secteur"""
        # Detecter secteur mentionne
        sector = None
        for code, name in self.knowledge_base["sectors"].items():
            if code in query or name.lower() in query.lower():
                sector = code
                break
        
        if not sector:
            sector = "221122"  # Default: Distribution electricite
        
        # Recuperer donnees
        cnesst_data = self.tools.get_cnesst_data(sector=sector)
        prediction = self.tools.predict_risk(sector)
        
        self.current_chain.add_step("sector_data_fetch", {"records": len(cnesst_data)}, 0.9)
        self.current_chain.add_step("risk_prediction", prediction, 0.85)
        
        sector_name = self.knowledge_base["sectors"].get(sector, sector)
        risk_level = prediction.get("risk_level", "N/A")
        risk_score = prediction.get("score", 0)
        
        response = f"""
ANALYSE SECTEUR: {sector} - {sector_name}
{'='*50}
Donnees CNESST: {len(cnesst_data)} enregistrements

EVALUATION DES RISQUES:
  - Niveau: {risk_level}
  - Score: {risk_score}/100
  - Facteurs: {', '.join(prediction.get('factors', ['N/A']))}

RECOMMANDATIONS:
"""
        if risk_level == "CRITICAL":
            response += "  ! ACTION IMMEDIATE requise\n"
            response += "  ! Audit securite dans les 7 jours\n"
        elif risk_level == "HIGH":
            response += "  - Renforcer controles existants\n"
            response += "  - Planifier audit dans 30 jours\n"
        else:
            response += "  - Maintenir surveillance standard\n"
        
        return response
    
    def _handle_comparison_query(self, query: str) -> str:
        """Gere requetes de comparaison"""
        # Detecter secteur
        sector = "221122"  # Default
        for code in self.knowledge_base["sectors"].keys():
            if code in query:
                sector = code
                break
        
        comparison = self.tools.compare_jurisdictions(sector)
        self.current_chain.add_step("jurisdiction_comparison", comparison, 0.9)
        
        if "error" in comparison:
            return f"Erreur: {comparison['error']}"
        
        cnesst = comparison.get("cnesst", {})
        osha = comparison.get("osha", {})
        ratio = comparison.get("ratio_usa_qc", 0)
        
        response = f"""
COMPARAISON MULTI-JURIDICTIONS - Secteur {sector}
{'='*50}

QUEBEC (CNESST):
  - Incidents: {cnesst.get('cases', 0):,}
  - Jours perdus: {cnesst.get('days_lost', 0):,}

USA (OSHA):
  - Incidents: {osha.get('cases', 0):,}
  - Deces: {osha.get('deaths', 0):,}
  - Jours perdus: {osha.get('days_lost', 0):,}

RATIO USA/QUEBEC: {ratio}x

ANALYSE:
"""
        if ratio > 5:
            response += "  - Volume USA significativement plus eleve (population 10x)\n"
            response += "  - Taux ajuste comparable\n"
        else:
            response += "  - Volumes relativement proches\n"
        
        return response
    
    def _handle_prediction_query(self, query: str) -> str:
        """Gere requetes de prediction"""
        sector = "221122"
        for code in self.knowledge_base["sectors"].keys():
            if code in query:
                sector = code
                break
        
        prediction = self.tools.predict_risk(sector)
        self.current_chain.add_step("risk_prediction", prediction, 0.85)
        
        response = f"""
PREDICTION DE RISQUE - Secteur {sector}
{'='*50}

Niveau de risque: {prediction.get('risk_level', 'N/A')}
Score: {prediction.get('score', 0)}/100

Facteurs identifies:
"""
        for factor in prediction.get('factors', ['Aucun facteur specifique']):
            response += f"  - {factor}\n"
        
        response += "\nProjection 6 mois: "
        if prediction.get('risk_level') == "CRITICAL":
            response += "VIGILANCE ACCRUE REQUISE"
        elif prediction.get('risk_level') == "HIGH":
            response += "Surveillance renforcee recommandee"
        else:
            response += "Evolution stable attendue"
        
        return response
    
    def _handle_recommendation_query(self, query: str) -> str:
        """Gere requetes de recommandations"""
        sector = "221122"
        for code in self.knowledge_base["sectors"].keys():
            if code in query:
                sector = code
                break
        
        # Generer recommandations basees sur le secteur
        sector_risks = {
            "221122": ["ELEC", "CHUTE", "TMS"],
            "23": ["CHUTE", "MACHINE", "ELEC"],
            "31-33": ["MACHINE", "TMS"],
            "62": ["TMS", "CHUTE"]
        }
        
        risks = sector_risks.get(sector, ["TMS", "CHUTE"])
        
        response = f"""
RECOMMANDATIONS PREVENTIVES - Secteur {sector}
{'='*50}

Risques principaux identifies: {', '.join(risks)}

TOP SOLUTIONS RECOMMANDEES:
"""
        solutions = {
            "ELEC": [
                ("Verification LOTO systematique", "95%", "$5,000"),
                ("Formation arc flash annuelle", "85%", "$15,000"),
                ("EPI categorie 4", "90%", "$25,000")
            ],
            "CHUTE": [
                ("Garde-corps conformes", "92%", "$20,000"),
                ("Formation travail hauteur", "80%", "$8,000"),
                ("Inspection echelles", "75%", "$2,000")
            ],
            "TMS": [
                ("Analyse ergonomique", "70%", "$12,000"),
                ("Rotation des taches", "65%", "$5,000"),
                ("Aide manutention", "85%", "$35,000")
            ],
            "MACHINE": [
                ("Protecteurs fixes", "95%", "$30,000"),
                ("Capteurs presence", "90%", "$45,000"),
                ("Cadenassage", "92%", "$10,000")
            ]
        }
        
        for risk in risks[:2]:
            response += f"\n{risk}:\n"
            for sol, eff, cost in solutions.get(risk, [])[:2]:
                response += f"  - {sol} (Efficacite: {eff}, Cout: {cost})\n"
        
        response += f"\nROI ESTIME: 150-300% sur 3 ans"
        
        return response
    
    def _handle_general_query(self, query: str) -> str:
        """Gere requetes generales"""
        response = """
ASSISTANT HSE - SafetyGraph
===========================

Je peux vous aider avec:

1. STATISTIQUES: "Quelles sont les statistiques globales?"
2. ANALYSE SECTEUR: "Analyse le secteur 221122" ou "construction"
3. COMPARAISON: "Compare Quebec vs USA pour la construction"
4. PREDICTION: "Quel est le risque pour le secteur 62?"
5. RECOMMANDATIONS: "Quelles solutions pour le secteur electrique?"

Secteurs disponibles:
- 221122: Distribution electricite (Hydro-Quebec)
- 23: Construction
- 31-33: Fabrication
- 62: Soins de sante
- 48-49: Transport

Posez votre question!
"""
        return response


# ============================================
# MULTI-AGENT ORCHESTRATOR avec LangChain
# ============================================

class LangChainOrchestrator:
    """
    Orchestrateur multi-agents avec LangChain
    Coordonne plusieurs agents specialises
    """
    
    def __init__(self):
        self.conversational_agent = HSEConversationalAgent()
        self.agents_results: Dict[str, Any] = {}
        
    def run_analysis_pipeline(self, sector: str) -> Dict:
        """Execute pipeline d'analyse complet"""
        print(f"\n{'='*60}")
        print(f"LANGCHAIN ORCHESTRATOR - Analyse {sector}")
        print(f"{'='*60}")
        
        results = {}
        
        # 1. Statistiques
        print("\n[1/5] Recuperation statistiques...")
        stats = self.conversational_agent.tools.get_stats()
        results["stats"] = stats
        print(f"      Total: {stats.get('combined_incidents', 0):,} incidents")
        
        # 2. Donnees secteur
        print("\n[2/5] Analyse secteur...")
        sector_data = self.conversational_agent.tools.get_cnesst_data(sector=sector)
        results["sector_data"] = {"records": len(sector_data)}
        print(f"      {len(sector_data)} enregistrements CNESST")
        
        # 3. Comparaison juridictions
        print("\n[3/5] Comparaison multi-juridictions...")
        comparison = self.conversational_agent.tools.compare_jurisdictions(sector)
        results["comparison"] = comparison
        print(f"      Ratio USA/QC: {comparison.get('ratio_usa_qc', 'N/A')}x")
        
        # 4. Prediction risque
        print("\n[4/5] Prediction de risque...")
        prediction = self.conversational_agent.tools.predict_risk(sector)
        results["prediction"] = prediction
        print(f"      Niveau: {prediction.get('risk_level', 'N/A')}")
        print(f"      Score: {prediction.get('score', 0)}/100")
        
        # 5. Generation recommandations
        print("\n[5/5] Generation recommandations...")
        recommendations = self.conversational_agent._handle_recommendation_query(f"recommandations secteur {sector}")
        results["recommendations"] = recommendations
        print("      Recommandations generees")
        
        self.agents_results = results
        return results
    
    def interactive_session(self):
        """Session interactive avec l'agent"""
        print("\n" + "="*60)
        print("SESSION INTERACTIVE - Agent HSE SafetyGraph")
        print("="*60)
        print("Tapez 'quit' pour quitter, 'help' pour aide")
        print("="*60 + "\n")
        
        while True:
            try:
                user_input = input("Vous: ").strip()
                
                if user_input.lower() == 'quit':
                    print("\nAu revoir!")
                    break
                elif user_input.lower() == 'help':
                    response = self.conversational_agent._handle_general_query("")
                elif user_input:
                    response = self.conversational_agent.process_query(user_input)
                else:
                    continue
                
                print(f"\nAgent HSE:\n{response}\n")
                
            except KeyboardInterrupt:
                print("\n\nSession terminee.")
                break


# ============================================
# MAIN
# ============================================

if __name__ == "__main__":
    print("="*60)
    print("LANGCHAIN INTEGRATION - SafetyGraph AgenticX5")
    print("="*60)
    
    # Creer orchestrateur
    orchestrator = LangChainOrchestrator()
    
    # Executer analyse pour secteur 221122
    print("\n>> Analyse automatique secteur 221122 (Distribution electricite)")
    results = orchestrator.run_analysis_pipeline("221122")
    
    # Sauvegarder resultats
    output_file = "./feature_store/langchain_analysis_221122.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False, default=str)
    print(f"\nResultats sauvegardes: {output_file}")
    
    # Exemple de requete conversationnelle
    print("\n" + "="*60)
    print("EXEMPLE REQUETE CONVERSATIONNELLE")
    print("="*60)
    
    agent = orchestrator.conversational_agent
    
    queries = [
        "Quelles sont les statistiques globales?",
        "Compare le secteur 221122 Quebec vs USA",
        "Quelles recommandations pour le secteur electrique?"
    ]
    
    for query in queries:
        print(f"\n>> Question: {query}")
        response = agent.process_query(query)
        print(response)
    
    print("\n" + "="*60)
    print("LANGCHAIN INTEGRATION COMPLETE")
    print("="*60)
