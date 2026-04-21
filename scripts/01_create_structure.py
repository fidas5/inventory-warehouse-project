#!/usr/bin/env python3
import sys
import os
import shutil
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import *

def create_structure():
    print("=" * 60)
    print("📁 ÉTAPE 1 : CRÉATION DE LA STRUCTURE")
    print("=" * 60)
    
    folders = [DATA_SOURCE, DATA_STAGING, DATA_DIMENSIONS, DATA_FACTS, OUTPUT_DIR]
    for folder in folders:
        if folder.exists():
            print(f"   ✅ Dossier existant : {folder}")
        else:
            folder.mkdir(parents=True)
            print(f"   📁 Dossier créé : {folder}")
    return True

def copy_source_file():
    print("\n📄 Copie du fichier source...")
    if SOURCE_FILE.exists():
        print(f"   ✅ Fichier déjà présent : {SOURCE_FILE}")
        return SOURCE_FILE
    
    if os.path.exists(DEFAULT_SOURCE_PATH):
        shutil.copy(DEFAULT_SOURCE_PATH, SOURCE_FILE)
        print(f"   ✅ Fichier copié depuis : {DEFAULT_SOURCE_PATH}")
        return SOURCE_FILE
    
    print(f"   ⚠️ Fichier source non trouvé")
    return None

if __name__ == "__main__":
    create_structure()
    copy_source_file()
    print("\n✅ Structure prête !")
