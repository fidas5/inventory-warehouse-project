#!/usr/bin/env python3
import sys
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import *

def create_dim_date(df_staging):
    print("\n   📅 Création de dim_date...")
    dates = pd.to_datetime(df_staging['date'].unique())
    dates = sorted(dates)
    
    dim_date = pd.DataFrame({
        'date_key': [int(d.strftime('%Y%m%d')) for d in dates],
        'date': [d.strftime('%Y-%m-%d') for d in dates],
        'year': [d.year for d in dates],
        'month': [d.month for d in dates],
        'month_name': [d.strftime('%B') for d in dates],
        'quarter': [d.quarter for d in dates],
        'week_of_year': [d.isocalendar()[1] for d in dates],
        'day_of_month': [d.day for d in dates],
        'day_of_week': [d.dayofweek for d in dates],
        'day_name': [d.strftime('%A') for d in dates],
        'is_weekend': [1 if d.dayofweek >= 5 else 0 for d in dates] 
    })
    dim_date.to_csv(DIM_DATE_FILE, index=False)
    print(f"      ✅ {len(dim_date)} lignes")
    return dim_date



def create_dim_product(df_staging):
    print("\n   🏷️ Création de dim_product...")
    products = df_staging[['sku_id']].drop_duplicates().sort_values('sku_id').reset_index(drop=True)

    dim_product = products.copy()
    # surrogate key numérique
    dim_product['product_key'] = dim_product.index + 1
    dim_product['product_name'] = dim_product['sku_id'].apply(lambda x: f"Produit {x}")
    dim_product['category'] = 'Standard'

    # réordonner les colonnes (clé en premier)
    dim_product = dim_product[['product_key', 'sku_id', 'product_name', 'category']]

    dim_product.to_csv(DIM_PRODUCT_FILE, index=False)
    print(f"      ✅ {len(dim_product)} lignes")
    return dim_product



def create_dim_warehouse(df_staging):
    print("\n   🏪 Création de dim_warehouse...")
    warehouses = df_staging[['warehouse', 'region']].drop_duplicates().sort_values('warehouse').reset_index(drop=True)

    dim_warehouse = warehouses.copy()
    # surrogate key
    dim_warehouse['warehouse_key'] = dim_warehouse.index + 1
    dim_warehouse['warehouse_name'] = dim_warehouse['warehouse']
    dim_warehouse['city'] = dim_warehouse['warehouse'].map({
        'WH_A': 'Local',
        'WH_B': 'EMEA',
        'WH_EST': 'Paris',
        'WH_OUEST': 'Nantes',
        'WH_CENTRE': 'Lyon'
    }).fillna('Inconnu')

    dim_warehouse = dim_warehouse[['warehouse_key', 'warehouse', 'warehouse_name', 'region', 'city']]

    dim_warehouse.to_csv(DIM_WAREHOUSE_FILE, index=False)
    print(f"      ✅ {len(dim_warehouse)} lignes")
    return dim_warehouse



def create_dim_policy(df_staging):
    print("\n   📋 Création de dim_policy...")
    policies = df_staging[['policy']].drop_duplicates().sort_values('policy').reset_index(drop=True)

    dim_policy = policies.copy()
    # surrogate key
    dim_policy['policy_key'] = dim_policy.index + 1
    dim_policy['policy_name'] = dim_policy['policy']
    dim_policy['description'] = dim_policy['policy'].map({
        'minmax': 'Politique Min-Max',
        'base_stock': 'Politique Base Stock',
        'ml_reorder': 'Politique ML'
    }).fillna('Standard')

    dim_policy = dim_policy[['policy_key', 'policy', 'policy_name', 'description']]

    dim_policy.to_csv(DIM_POLICY_FILE, index=False)
    print(f"      ✅ {len(dim_policy)} lignes")
    return dim_policy



def create_dim_time(df_staging):
    print("\n   ⏰ Création de dim_time...")
    time_slots = []
    for hour in range(24):
        time_slots.append({
            'time_key': hour, 
            'hour': hour, 
            'period': 'Matin' if hour < 12 else 'Soir',
            'is_business_hours': 1 if 9 <= hour <= 18 else 0
        })
    dim_time = pd.DataFrame(time_slots)
    dim_time.to_csv(DIM_TIME_FILE, index=False)
    print(f"      ✅ {len(dim_time)} lignes")
    return dim_time

def run_all_dimensions(df_staging):
    print("\n" + "=" * 60)
    print("🏗️ ÉTAPE 3 : CRÉATION DES DIMENSIONS")
    print("=" * 60)
    
    dim_date = create_dim_date(df_staging)
    dim_product = create_dim_product(df_staging)
    dim_warehouse = create_dim_warehouse(df_staging)
    dim_policy = create_dim_policy(df_staging)
    dim_time = create_dim_time(df_staging)
    
    return {
        'dim_date': dim_date, 
        'dim_product': dim_product, 
        'dim_warehouse': dim_warehouse, 
        'dim_policy': dim_policy, 
        'dim_time': dim_time
    }

if __name__ == "__main__":
    if STAGING_FILE.exists():
        df_staging = pd.read_csv(STAGING_FILE)
        print(f"   📊 Lecture de {len(df_staging)} lignes depuis staging")
        dimensions = run_all_dimensions(df_staging)
        print(f"\n✅ Dimensions créées avec succès !")
        print(f"   - dim_date: {len(dimensions['dim_date'])} lignes")
        print(f"   - dim_product: {len(dimensions['dim_product'])} lignes")
        print(f"   - dim_warehouse: {len(dimensions['dim_warehouse'])} lignes")
        print(f"   - dim_policy: {len(dimensions['dim_policy'])} lignes")
        print(f"   - dim_time: {len(dimensions['dim_time'])} lignes")
    else:
        print(f"❌ Fichier staging non trouvé : {STAGING_FILE}")
        print("   Exécutez d'abord 02_extract_to_staging.py")
