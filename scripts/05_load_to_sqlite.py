#!/usr/bin/env python3
import sys
import sqlite3
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import *

def load_all_to_sqlite():
    print("\n" + "=" * 60)
    print("💾 ÉTAPE 5 : CHARGEMENT DANS SQLITE")
    print("=" * 60)
    
    conn = sqlite3.connect(OUTPUT_DB)
    cursor = conn.cursor()
    
    tables = ['dim_date', 'dim_product', 'dim_warehouse', 'dim_policy', 'dim_time', 'fact_inventory']
    for table in tables:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
    
    print("   📥 Chargement des dimensions...")
    pd.read_csv(DIM_DATE_FILE).to_sql('dim_date', conn, if_exists='replace', index=False)
    pd.read_csv(DIM_PRODUCT_FILE).to_sql('dim_product', conn, if_exists='replace', index=False)
    pd.read_csv(DIM_WAREHOUSE_FILE).to_sql('dim_warehouse', conn, if_exists='replace', index=False)
    pd.read_csv(DIM_POLICY_FILE).to_sql('dim_policy', conn, if_exists='replace', index=False)
    pd.read_csv(DIM_TIME_FILE).to_sql('dim_time', conn, if_exists='replace', index=False)
    
    print("   📥 Chargement de la table de faits...")
    pd.read_csv(FACT_INVENTORY_FILE).to_sql('fact_inventory', conn, if_exists='replace', index=False)
    
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_fact_date ON fact_inventory(date_key);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_fact_product ON fact_inventory(product_key);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_fact_warehouse ON fact_inventory(warehouse_key);")
    
    conn.commit()
    cursor.execute("SELECT COUNT(*) FROM fact_inventory")
    count = cursor.fetchone()[0]
    conn.close()
    
    print(f"   ✅ Base créée : {OUTPUT_DB}")
    print(f"   📊 {count} enregistrements")
    return True

if __name__ == "__main__":
    load_all_to_sqlite()
    print("\n✅ Chargement SQLite terminé !")
