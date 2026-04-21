#!/usr/bin/env python3
import sys
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import *

def create_fact_table(df_staging, dimensions):
    print("\n" + "=" * 60)
    print("📊 ÉTAPE 4 : CRÉATION DE LA TABLE DE FAITS")
    print("=" * 60)

    dim_date = dimensions['dim_date']
    dim_product = dimensions['dim_product']
    dim_warehouse = dimensions['dim_warehouse']
    dim_policy = dimensions['dim_policy']

    # Mappings pour les clés étrangères
    date_key_map = dict(zip(dim_date['date'], dim_date['date_key']))
    product_key_map = dict(zip(dim_product['sku_id'], dim_product['product_key']))
    warehouse_key_map = dict(zip(dim_warehouse['warehouse'], dim_warehouse['warehouse_key']))
    policy_key_map = dict(zip(dim_policy['policy'], dim_policy['policy_key']))

    df_fact = df_staging.copy()

    # Ajout des clés étrangères
    print("   🔑 Ajout des clés étrangères...")
    df_fact['date_key'] = df_fact['date'].map(date_key_map)
    df_fact['product_key'] = df_fact['sku_id'].map(product_key_map)
    df_fact['warehouse_key'] = df_fact['warehouse'].map(warehouse_key_map)
    df_fact['policy_key'] = df_fact['policy'].map(policy_key_map)

    # Colonnes de mesures disponibles
    available_cols = list(df_fact.columns)
    print(f"   📋 Colonnes disponibles : {available_cols}")

    measure_columns = []
    potential_measures = [
        'on_hand_units', 'demand_forecast_units', 'demand_units',
        'order_quantity', 'stockout', 'fill_rate', 'safety_stock_units',
        'lead_time_days', 'total_cost_usd', 'promo_flag', 'holiday_flag',
        'price_usd', 'weather_index'
    ]

    for col in potential_measures:
        if col in df_fact.columns:
            measure_columns.append(col)
        else:
            print(f"   ⚠️ Colonne non trouvée : {col} (ignorée)")

    # Liste finale des colonnes de la fact :
    # clés + business keys + mesures
    fact_columns = [
        'date_key',
        'product_key',
        'warehouse_key',
        'policy_key',
        'sku_id',
        'warehouse',
        'policy'
    ] + measure_columns

    print(f"   ✅ Colonnes retenues : {fact_columns}")

    df_fact = df_fact[fact_columns]

    # KPIs dérivés
    if 'fill_rate' in df_fact.columns and 'stockout' in df_fact.columns:
        print("   📈 Calcul de l'efficiency_score...")
        df_fact['efficiency_score'] = df_fact['fill_rate'] * (1 - df_fact['stockout'] * 0.5)
        print("   ✅ efficiency_score ajouté")

    if 'total_cost_usd' in df_fact.columns and 'on_hand_units' in df_fact.columns:
        print("   📈 Calcul du cost_per_unit...")
        df_fact['cost_per_unit'] = df_fact['total_cost_usd'] / (df_fact['on_hand_units'] + 1)
        print("   ✅ cost_per_unit ajouté")

    df_fact.to_csv(FACT_INVENTORY_FILE, index=False)
    print(f"\n   💾 Sauvegardé : {FACT_INVENTORY_FILE}")
    print(f"   📊 {len(df_fact)} lignes dans la table de faits")
    print(f"   📋 {len(df_fact.columns)} colonnes au total")

    return df_fact


if __name__ == "__main__":
    if STAGING_FILE.exists():
        df_staging = pd.read_csv(STAGING_FILE)
        print(f"   📊 Lecture de {len(df_staging)} lignes depuis staging")
        
        # Recharger les dimensions
        dim_date = pd.read_csv(DIM_DATE_FILE)
        dim_product = pd.read_csv(DIM_PRODUCT_FILE)
        dim_warehouse = pd.read_csv(DIM_WAREHOUSE_FILE)
        dim_policy = pd.read_csv(DIM_POLICY_FILE)
        dim_time = pd.read_csv(DIM_TIME_FILE)
        
        dimensions = {
            'dim_date': dim_date, 
            'dim_product': dim_product,
            'dim_warehouse': dim_warehouse, 
            'dim_policy': dim_policy, 
            'dim_time': dim_time
        }
        
        df_fact = create_fact_table(df_staging, dimensions)
        print(f"\n✅ Table de faits créée avec succès !")
    else:
        print(f"❌ Fichier staging non trouvé : {STAGING_FILE}")
        print("   Exécutez d'abord 02_extract_to_staging.py")
