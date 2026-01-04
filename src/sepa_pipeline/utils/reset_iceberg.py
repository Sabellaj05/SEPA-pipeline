from sepa_pipeline.config import SEPAConfig
from pyiceberg.catalog import load_catalog

def reset_table():
    config = SEPAConfig()
    catalog = load_catalog("default", **config.iceberg_catalog_config)
    
    table_name = "sepa.precios"
    print(f"Dropping table {table_name} to allow schema recreation...")
    try:
        catalog.drop_table(table_name)
        print("Table dropped successfully.")
    except Exception as e:
        print(f"Could not drop table (maybe it doesn't exist): {e}")

if __name__ == "__main__":
    reset_table()
