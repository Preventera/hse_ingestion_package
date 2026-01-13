#!/usr/bin/env python3
"""
🚀 HSE Pipeline Quick Start - AgenticX5
========================================
Script de démarrage rapide pour l'ingestion de données HSE

Usage:
    python quickstart.py                    # Menu interactif
    python quickstart.py --demo             # Mode démo (données simulées)
    python quickstart.py --source kaggle    # Source spécifique
    python quickstart.py --all              # Toutes les sources priorité 1-2
"""

import os
import sys
import json
from pathlib import Path
from datetime import datetime

# Couleurs pour le terminal
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    END = '\033[0m'
    BOLD = '\033[1m'


def print_banner():
    """Afficher la bannière"""
    banner = f"""
{Colors.CYAN}╔══════════════════════════════════════════════════════════════════╗
║                                                                    ║
║   {Colors.BOLD}🔄 HSE Data Ingestion Pipeline{Colors.END}{Colors.CYAN}                                  ║
║   {Colors.GREEN}AgenticX5 / Safety Graph{Colors.END}{Colors.CYAN}                                        ║
║                                                                    ║
║   GenAISafety / Preventera - 2026                                  ║
║                                                                    ║
╚══════════════════════════════════════════════════════════════════╝{Colors.END}
    """
    print(banner)


def check_environment():
    """Vérifier l'environnement"""
    print(f"\n{Colors.BOLD}📋 Vérification de l'environnement...{Colors.END}\n")
    
    checks = {
        "Python 3.10+": sys.version_info >= (3, 10),
        "pandas": False,
        "requests": False,
        "sqlalchemy": False,
        "kaggle": False,
    }
    
    try:
        import pandas
        checks["pandas"] = True
    except ImportError:
        pass
    
    try:
        import requests
        checks["requests"] = True
    except ImportError:
        pass
    
    try:
        import sqlalchemy
        checks["sqlalchemy"] = True
    except ImportError:
        pass
    
    try:
        import kaggle
        checks["kaggle"] = True
    except ImportError:
        pass
    
    all_ok = True
    for check, status in checks.items():
        icon = f"{Colors.GREEN}✓{Colors.END}" if status else f"{Colors.FAIL}✗{Colors.END}"
        print(f"  {icon} {check}")
        if not status:
            all_ok = False
    
    if not all_ok:
        print(f"\n{Colors.WARNING}⚠️  Certaines dépendances manquent.{Colors.END}")
        print(f"   Exécutez: {Colors.CYAN}pip install -r requirements.txt{Colors.END}\n")
    
    return all_ok


def check_api_keys():
    """Vérifier les clés API"""
    print(f"\n{Colors.BOLD}🔑 Vérification des clés API...{Colors.END}\n")
    
    keys = {
        "KAGGLE_USERNAME": os.getenv("KAGGLE_USERNAME"),
        "KAGGLE_KEY": os.getenv("KAGGLE_KEY"),
        "BLS_API_KEY": os.getenv("BLS_API_KEY"),
        "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "localhost"),
    }
    
    for key, value in keys.items():
        if value:
            masked = value[:4] + "****" if len(value) > 4 else "****"
            print(f"  {Colors.GREEN}✓{Colors.END} {key}: {masked}")
        else:
            print(f"  {Colors.WARNING}○{Colors.END} {key}: Non configuré")


def show_sources():
    """Afficher les sources disponibles"""
    print(f"\n{Colors.BOLD}📦 Sources HSE Disponibles:{Colors.END}\n")
    
    sources = [
        ("kaggle_osha_injuries", "Kaggle OSHA 2016-2021", "USA", 1, "1M+ rows"),
        ("osha_inspections", "OSHA Inspection Data", "USA", 1, "8M+ rows"),
        ("eurostat_esaw", "Eurostat ESAW", "EU-27", 1, "27 pays"),
        ("ilostat_injuries", "ILOSTAT OSH", "International", 1, "180+ pays"),
        ("dares_at", "DARES Accidents France", "France", 1, "668K/an"),
        ("cnesst_lesions", "CNESST Québec", "Quebec", 1, "793K+"),
        ("bls_cfoi", "BLS Fatal Injuries", "USA", 2, "30+ ans"),
        ("kaggle_industrial", "Industrial Safety", "International", 2, "12K rows"),
    ]
    
    print(f"  {'Source':<25} {'Nom':<30} {'Juridiction':<15} {'P':<3} {'Volume':<12}")
    print(f"  {'-'*25} {'-'*30} {'-'*15} {'-'*3} {'-'*12}")
    
    for key, name, jurisdiction, priority, volume in sources:
        p_color = Colors.FAIL if priority == 1 else Colors.WARNING
        print(f"  {key:<25} {name:<30} {jurisdiction:<15} {p_color}{priority}{Colors.END}   {volume:<12}")


def run_demo():
    """Exécuter une démo avec données simulées"""
    print(f"\n{Colors.BOLD}🎮 Mode Démo - Simulation du pipeline{Colors.END}\n")
    
    import time
    
    steps = [
        ("📥 Téléchargement données Kaggle OSHA...", 1.5),
        ("🔧 Transformation Bronze → Silver...", 1.0),
        ("✨ Harmonisation Silver → Gold...", 0.8),
        ("📊 Génération des statistiques...", 0.5),
    ]
    
    for step, duration in steps:
        print(f"  {Colors.CYAN}→{Colors.END} {step}", end="", flush=True)
        time.sleep(duration)
        print(f" {Colors.GREEN}✓{Colors.END}")
    
    # Résultats simulés
    results = {
        "source": "kaggle_osha_injuries",
        "status": "success",
        "rows_bronze": 1_234_567,
        "rows_silver": 1_230_000,
        "rows_gold": 1_230_000,
        "duration_seconds": sum(d for _, d in steps),
    }
    
    print(f"\n{Colors.BOLD}📊 Résultats:{Colors.END}")
    print(f"  • Rows Bronze: {results['rows_bronze']:,}")
    print(f"  • Rows Silver: {results['rows_silver']:,}")
    print(f"  • Rows Gold:   {results['rows_gold']:,}")
    print(f"  • Durée:       {results['duration_seconds']:.1f}s")
    
    print(f"\n{Colors.GREEN}✅ Démo terminée avec succès!{Colors.END}")
    
    return results


def run_real_pipeline(source_key: str = None, all_sources: bool = False):
    """Exécuter le pipeline réel"""
    try:
        from hse_data_ingestion import HSEPipelineOrchestrator, HSE_SOURCES
        
        orchestrator = HSEPipelineOrchestrator(data_dir="./data")
        
        if all_sources:
            print(f"\n{Colors.BOLD}🚀 Exécution de toutes les sources (priorité 1-2)...{Colors.END}\n")
            results = orchestrator.run_all(priority_threshold=2)
            report = orchestrator.generate_report()
            
            print(f"\n{Colors.BOLD}📊 Rapport:{Colors.END}")
            print(f"  • Sources: {report['total_sources']}")
            print(f"  • Succès:  {report['successful']}")
            print(f"  • Échecs:  {report['failed']}")
            print(f"  • Rows:    {report['total_rows_ingested']:,}")
            
        elif source_key:
            if source_key not in HSE_SOURCES:
                print(f"{Colors.FAIL}❌ Source inconnue: {source_key}{Colors.END}")
                print(f"   Sources valides: {', '.join(HSE_SOURCES.keys())}")
                return
            
            print(f"\n{Colors.BOLD}🚀 Exécution: {source_key}...{Colors.END}\n")
            result = orchestrator.run_single(source_key)
            
            if result["status"] == "success":
                print(f"\n{Colors.GREEN}✅ Succès!{Colors.END}")
                print(f"  • Rows Gold: {result['steps']['gold']['rows']:,}")
            else:
                print(f"\n{Colors.FAIL}❌ Échec: {result.get('error', 'Unknown')}{Colors.END}")
        
        else:
            print(f"{Colors.WARNING}⚠️ Spécifiez --source <key> ou --all{Colors.END}")
            
    except ImportError as e:
        print(f"{Colors.FAIL}❌ Module non trouvé: {e}{Colors.END}")
        print(f"   Assurez-vous que hse_data_ingestion.py est dans le même répertoire")


def interactive_menu():
    """Menu interactif"""
    while True:
        print(f"\n{Colors.BOLD}═══ Menu Principal ═══{Colors.END}")
        print(f"  1. 📋 Afficher les sources disponibles")
        print(f"  2. 🔍 Vérifier l'environnement")
        print(f"  3. 🔑 Vérifier les clés API")
        print(f"  4. 🎮 Exécuter la démo")
        print(f"  5. 🚀 Exécuter une source spécifique")
        print(f"  6. 🔄 Exécuter toutes les sources (P1-2)")
        print(f"  7. 📖 Afficher le guide")
        print(f"  0. 🚪 Quitter")
        
        choice = input(f"\n{Colors.CYAN}Choix: {Colors.END}").strip()
        
        if choice == "1":
            show_sources()
        elif choice == "2":
            check_environment()
        elif choice == "3":
            check_api_keys()
        elif choice == "4":
            run_demo()
        elif choice == "5":
            source = input(f"  Source (ex: kaggle_osha_injuries): ").strip()
            run_real_pipeline(source_key=source)
        elif choice == "6":
            confirm = input(f"  {Colors.WARNING}Confirmer l'exécution? (y/n): {Colors.END}").strip().lower()
            if confirm == 'y':
                run_real_pipeline(all_sources=True)
        elif choice == "7":
            print(f"\n{Colors.CYAN}📖 Consultez GUIDE_EXECUTION_HSE_PIPELINES.md{Colors.END}")
        elif choice == "0":
            print(f"\n{Colors.GREEN}👋 Au revoir!{Colors.END}\n")
            break
        else:
            print(f"{Colors.WARNING}⚠️ Choix invalide{Colors.END}")


def main():
    """Point d'entrée principal"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="HSE Pipeline Quick Start - AgenticX5",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--demo", action="store_true", help="Mode démo (simulation)")
    parser.add_argument("--source", type=str, help="Exécuter une source spécifique")
    parser.add_argument("--all", action="store_true", help="Exécuter toutes les sources P1-2")
    parser.add_argument("--check", action="store_true", help="Vérifier l'environnement")
    parser.add_argument("--list", action="store_true", help="Lister les sources")
    
    args = parser.parse_args()
    
    print_banner()
    
    if args.check:
        check_environment()
        check_api_keys()
    elif args.list:
        show_sources()
    elif args.demo:
        run_demo()
    elif args.source:
        run_real_pipeline(source_key=args.source)
    elif args.all:
        run_real_pipeline(all_sources=True)
    else:
        # Mode interactif
        interactive_menu()


if __name__ == "__main__":
    main()
