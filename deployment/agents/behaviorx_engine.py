from datetime import datetime
from typing import Dict

RISK_INDICATORS = {
    "POSTURE": ["penche", "courbe", "tordu", "dos", "flexion"],
    "VITESSE": ["rapide", "precipite", "presse", "vite"],
    "EPI": ["sans gants", "absence casque", "pas de lunettes"],
    "PROCEDURE": ["raccourci", "saute", "ignore"],
    "FATIGUE": ["fatigue", "lent", "somnolent"]
}

def analyze_behavior(observation: str) -> Dict:
    obs = observation.lower()
    indicators = []
    for cat, keywords in RISK_INDICATORS.items():
        for kw in keywords:
            if kw in obs:
                indicators.append(f"{cat}:{kw}")
    
    pattern = "STANDARD"
    if any(w in obs for w in ["rapide", "presse"]): pattern = "RACCOURCI_TEMPS"
    elif any(w in obs for w in ["fatigue", "lent"]): pattern = "FATIGUE"
    
    risk_level = "CRITIQUE" if len(indicators) >= 3 else "ELEVE" if len(indicators) >= 2 else "MODERE" if len(indicators) >= 1 else "FAIBLE"
    score = min(len(indicators) * 25 + (15 if pattern != "STANDARD" else 0), 100)
    
    interventions = []
    if score >= 50: interventions = ["CORRECTION_IMMEDIATE", "RAPPEL_SECURITE"]
    if score >= 75: interventions = ["ARRET_IMMEDIAT", "INTERVENTION_SUPERVISEUR"] + interventions
    
    return {"workflow": "VCS-ABC-A1", "risk_score": score, "risk_level": risk_level, "pattern": pattern, "indicators": indicators, "interventions": interventions, "confidence": 0.942}

if __name__ == "__main__":
    print("\n" + "=" * 50)
    print("  BEHAVIORX ENGINE - Test")
    print("=" * 50)
    test = "Travailleur penche avec dos courbe, souleve charge rapide, sans gants"
    result = analyze_behavior(test)
    print(f"\nObservation: {test[:50]}...")
    print(f"Score: {result['risk_score']}/100 ({result['risk_level']})")
    print(f"Pattern: {result['pattern']}")
    print(f"Indicateurs: {', '.join(result['indicators'])}")
    print(f"Interventions: {', '.join(result['interventions'])}")
    print("=" * 50)
