from pathlib import Path

PROJECT_ROOT = Path(__file__).parent

DATA_SOURCE = PROJECT_ROOT / "data" / "source"
DATA_STAGING = PROJECT_ROOT / "data" / "staging"
DATA_DIMENSIONS = PROJECT_ROOT / "data" / "dimensions"
DATA_FACTS = PROJECT_ROOT / "data" / "facts"
OUTPUT_DIR = PROJECT_ROOT / "output"
OUTPUT_DB = OUTPUT_DIR / "inventory_warehouse.db"

SOURCE_FILE = DATA_SOURCE / "inventory_replenishment_timeseries_10000.csv"
STAGING_FILE = DATA_STAGING / "staging_data.csv"

DIM_DATE_FILE = DATA_DIMENSIONS / "dim_date.csv"
DIM_PRODUCT_FILE = DATA_DIMENSIONS / "dim_product.csv"
DIM_WAREHOUSE_FILE = DATA_DIMENSIONS / "dim_warehouse.csv"
DIM_POLICY_FILE = DATA_DIMENSIONS / "dim_policy.csv"
DIM_TIME_FILE = DATA_DIMENSIONS / "dim_time.csv"

FACT_INVENTORY_FILE = DATA_FACTS / "fact_inventory.csv"

DEFAULT_SOURCE_PATH = "source/inventory_replenishment_timeseries_10000.csv"

for folder in [DATA_SOURCE, DATA_STAGING, DATA_DIMENSIONS, DATA_FACTS, OUTPUT_DIR]:
    folder.mkdir(parents=True, exist_ok=True)
