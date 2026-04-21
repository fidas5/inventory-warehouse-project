#!/usr/bin/env python3
import sys
import subprocess
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

def run_script(script_name):
    print(f"\n▶️  Exécution de {script_name}...")
    result = subprocess.run([sys.executable, script_name], cwd=Path(__file__).parent)
    if result.returncode != 0:
        print(f"❌ Erreur dans {script_name}")
        return False
    return True

def main():
    print("=" * 70)
    print("🚀 PIPELINE ETL COMPLET")
    print("=" * 70)
    
    scripts = [
        "01_create_structure.py",
        "02_extract_to_staging.py",
        "03_create_dimensions.py",
        "04_create_facts.py",
        "07_load_to_bigquery.py"
        #"07_load_to_sqlite.py"# plus utilisé
        #"06_create_olap_views.py"# plus utilisé
    ]
    
    for script in scripts:
        script_path = Path(__file__).parent / script
        if not run_script(str(script_path)):
            print(f"\n❌ Pipeline arrêté à {script}")
            return False
    
    print("\n" + "=" * 70)
    print("✅ PIPELINE ETL TERMINÉ AVEC SUCCÈS !")
    print("=" * 70)
    return True

if __name__ == "__main__":
    main()
