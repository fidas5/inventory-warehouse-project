#!/usr/bin/env python3
import sys
import sqlite3
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import *

def create_olap_views():
    print("\n" + "=" * 60)
    print("📈 ÉTAPE 6 : CRÉATION DES VUES OLAP")
    print("=" * 60)
    
    conn = sqlite3.connect(OUTPUT_DB)
    cursor = conn.cursor()
    
    # Supprimer les anciennes vues
    views = ['v_policy_summary', 'v_monthly_trend', 'v_promo_impact', 'v_region_performance']
    for view in views:
        cursor.execute(f"DROP VIEW IF EXISTS {view}")
    
    # Vue 1 : Résumé par politique de réapprovisionnement (CORRIGÉE)
    print("   📊 Création de v_policy_summary...")
    cursor.execute("""
        CREATE VIEW v_policy_summary AS
        SELECT 
            f.policy as policy_name,
            COUNT(*) as total_days,
            ROUND(AVG(f.fill_rate), 3) as avg_fill_rate,
            ROUND(SUM(CASE WHEN f.stockout = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as stockout_rate,
            ROUND(AVG(f.total_cost_usd), 2) as avg_cost,
            ROUND(AVG(f.efficiency_score), 3) as avg_efficiency
        FROM fact_inventory f
        GROUP BY f.policy
        ORDER BY avg_efficiency DESC
    """)
    
    # Vue 2 : Tendances mensuelles (CORRIGÉE)
    print("   📈 Création de v_monthly_trend...")
    cursor.execute("""
        CREATE VIEW v_monthly_trend AS
        SELECT 
            d.year,
            d.month,
            d.month_name,
            ROUND(AVG(f.fill_rate), 3) as avg_fill_rate,
            ROUND(SUM(CASE WHEN f.stockout = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as stockout_rate,
            ROUND(SUM(f.total_cost_usd), 2) as total_cost,
            COUNT(*) as nb_transactions
        FROM fact_inventory f
        JOIN dim_date d ON f.date_key = d.date_key
        GROUP BY d.year, d.month
        ORDER BY d.year, d.month
    """)
    
    # Vue 3 : Impact des promotions et jours fériés (CORRIGÉE)
    print("   🎯 Création de v_promo_impact...")
    cursor.execute("""
        CREATE VIEW v_promo_impact AS
        SELECT 
            CASE f.promo_flag WHEN 0 THEN 'Sans promotion' ELSE 'Avec promotion' END as promo_status,
            CASE f.holiday_flag WHEN 0 THEN 'Jour normal' ELSE 'Jour férié' END as holiday_status,
            COUNT(*) as nb_jours,
            ROUND(AVG(f.fill_rate), 3) as avg_fill_rate,
            ROUND(SUM(CASE WHEN f.stockout = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as stockout_rate,
            ROUND(AVG(f.total_cost_usd), 2) as avg_cost
        FROM fact_inventory f
        GROUP BY f.promo_flag, f.holiday_flag
        ORDER BY stockout_rate DESC
    """)
    
    # Vue 4 : Performance par région (CORRIGÉE)
    print("   🏪 Création de v_region_performance...")
    cursor.execute("""
        CREATE VIEW v_region_performance AS
        SELECT 
            f.region,
            f.warehouse,
            ROUND(AVG(f.fill_rate), 3) as avg_fill_rate,
            ROUND(SUM(CASE WHEN f.stockout = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as stockout_rate,
            ROUND(AVG(f.total_cost_usd), 2) as avg_cost,
            ROUND(AVG(f.lead_time_days), 1) as avg_lead_time
        FROM fact_inventory f
        GROUP BY f.region, f.warehouse
        ORDER BY avg_fill_rate DESC
    """)
    
    # Vue 5 : Produits à ÉVITER (taux de rupture élevé)
    print("   ⚠️ Création de v_products_to_avoid...")
    cursor.execute("""
        CREATE VIEW v_products_to_avoid AS
        SELECT 
            sku_id as product,
            COUNT(*) as total_days,
            ROUND(SUM(CASE WHEN stockout = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as stockout_rate,
            ROUND(AVG(total_cost_usd), 2) as avg_cost
        FROM fact_inventory
        GROUP BY sku_id
        HAVING stockout_rate > 10
        ORDER BY stockout_rate DESC
        LIMIT 10
    """)
    
    # Vue 6 : Produits à RENFORCER (taux de rupture faible)
    print("   ✅ Création de v_products_to_strengthen...")
    cursor.execute("""
        CREATE VIEW v_products_to_strengthen AS
        SELECT 
            sku_id as product,
            COUNT(*) as total_days,
            ROUND(SUM(CASE WHEN stockout = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as stockout_rate,
            ROUND(AVG(fill_rate), 3) as avg_fill_rate,
            ROUND(AVG(efficiency_score), 3) as avg_efficiency
        FROM fact_inventory
        GROUP BY sku_id
        HAVING stockout_rate < 5
        ORDER BY avg_efficiency DESC
        LIMIT 10
    """)
    
    # Vue 7 : Dashboard récapitulatif pour Karim
    print("   📊 Création de v_dashboard...")
    cursor.execute("""
        CREATE VIEW v_dashboard AS
        SELECT 
            'Taux de rupture global' as indicateur,
            ROUND(SUM(CASE WHEN stockout = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as valeur
        FROM fact_inventory
        UNION ALL
        SELECT 
            'Fill rate moyen',
            ROUND(AVG(fill_rate), 3)
        FROM fact_inventory
        UNION ALL
        SELECT 
            'Coût total (USD)',
            ROUND(SUM(total_cost_usd), 2)
        FROM fact_inventory
        UNION ALL
        SELECT 
            'Efficacité moyenne',
            ROUND(AVG(efficiency_score), 3)
        FROM fact_inventory
    """)
    
    conn.commit()
    
    # Vérifier que les vues sont créées
    cursor.execute("SELECT name FROM sqlite_master WHERE type='view' ORDER BY name")
    views_created = cursor.fetchall()
    print(f"\n   ✅ {len(views_created)} vues créées :")
    for view in views_created:
        print(f"      - {view[0]}")
    
    conn.close()

if __name__ == "__main__":
    create_olap_views()
    print("\n✅ Vues OLAP créées avec succès !")
