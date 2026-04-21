#!/usr/bin/env python3
import sys
from pathlib import Path

import pandas as pd
from google.cloud import bigquery

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import *


GCP_PROJECT_ID = "projetdwh-493922"
BQ_DATASET_ID = "inventory_dwh"  


def load_csv_to_bq(df: pd.DataFrame, table_name: str, if_exists: str = "replace"):
    client = bigquery.Client(project=GCP_PROJECT_ID)
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.{table_name}"

    # build job config
    write_disposition = (
        bigquery.WriteDisposition.WRITE_TRUNCATE
        if if_exists in ("replace", "truncate")
        else bigquery.WriteDisposition.WRITE_APPEND
    )

    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # wait for job

    table = client.get_table(table_id)
    print(f"   ✅ Chargé dans {table_id} : {table.num_rows} lignes, {len(table.schema)} colonnes")


def load_all_to_bigquery():
    print("\n" + "=" * 60)
    print("💾 ÉTAPE 5 : CHARGEMENT DANS BIGQUERY")
    print("=" * 60)

    # lire les CSV générés par ton pipeline
    dim_date = pd.read_csv(DIM_DATE_FILE)
    dim_product = pd.read_csv(DIM_PRODUCT_FILE)
    dim_warehouse = pd.read_csv(DIM_WAREHOUSE_FILE)
    dim_policy = pd.read_csv(DIM_POLICY_FILE)
    dim_time = pd.read_csv(DIM_TIME_FILE)
    fact_inventory = pd.read_csv(FACT_INVENTORY_FILE)

    # charger dans BigQuery
    load_csv_to_bq(dim_date, "d_date")
    load_csv_to_bq(dim_product, "d_sku")
    load_csv_to_bq(dim_warehouse, "d_warehouse")
    load_csv_to_bq(dim_policy, "d_policy")
    load_csv_to_bq(dim_time, "d_time")
    load_csv_to_bq(fact_inventory, "f_inventory_daily")

    print("\n✅ Chargement BigQuery terminé !")
    return True


if __name__ == "__main__":
    load_all_to_bigquery()