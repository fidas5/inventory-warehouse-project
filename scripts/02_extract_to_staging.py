#!/usr/bin/env python3
import sys
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import *

def generate_sample_data():
    import random
    import numpy as np
    
    np.random.seed(42)
    dates = pd.date_range('2024-01-01', '2024-12-31', freq='D')
    skus = [f'SKU_{i:03d}' for i in range(1, 21)]
    warehouses = ['WH_EST', 'WH_OUEST', 'WH_CENTRE']
    regions = {'WH_EST': 'NORD', 'WH_OUEST': 'SUD', 'WH_CENTRE': 'CENTRE'}
    policies = ['minmax', 'base_stock', 'ml_reorder']
    
    data = []
    for date in dates:
        for sku in skus:
            for warehouse in warehouses:
                policy = random.choice(policies)
                on_hand = random.randint(0, 500)
                demand_forecast = random.randint(50, 300)
                demand = random.randint(30, demand_forecast + 50)
                stockout = 1 if demand > on_hand else 0
                fill_rate = max(0, min(1, 1 - (max(0, demand - on_hand) / (demand + 0.01))))
                
                data.append({
                    'date': date.strftime('%Y-%m-%d'),
                    'sku_id': sku,
                    'warehouse': warehouse,
                    'region': regions[warehouse],
                    'policy': policy,
                    'on_hand_units': on_hand,
                    'demand_forecast_units': demand_forecast,
                    'demand_units': demand,
                    'order_quantity': random.randint(0, 200),
                    'stockout': stockout,
                    'fill_rate': round(fill_rate, 3),
                    'safety_stock_units': random.randint(10, 100),
                    'lead_time_days': random.randint(1, 14),
                    'total_cost_usd': round(random.uniform(10, 500), 2),
                    'promo_flag': random.choice([0, 1]),
                    'holiday_flag': random.choice([0, 1]),
                    'price_usd': round(random.uniform(5, 50), 2),
                    'weather_index': round(random.uniform(0, 1), 2)
                })
    return pd.DataFrame(data)

def extract_to_staging():
    print("=" * 60)
    print("📥 ÉTAPE 2 : EXTRACTION VERS STAGING")
    print("=" * 60)
    
    if SOURCE_FILE.exists():
        print(f"   📄 Lecture du fichier : {SOURCE_FILE}")
        df = pd.read_csv(SOURCE_FILE)
        print(f"   ✅ {len(df)} lignes chargées")
    else:
        print("   📝 Génération de données d'exemple...")
        df = generate_sample_data()
        print(f"   ✅ {len(df)} lignes générées")
    
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    df.to_csv(STAGING_FILE, index=False)
    print(f"   💾 Sauvegardé : {STAGING_FILE}")
    return df

if __name__ == "__main__":
    df = extract_to_staging()
    print(f"\n✅ Extraction terminée : {len(df)} lignes")
