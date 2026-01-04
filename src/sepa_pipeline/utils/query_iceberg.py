from sepa_pipeline.config import SEPAConfig
from pyiceberg.catalog import load_catalog
import duckdb
import logging

# Silence excessive logs
logging.basicConfig(level=logging.ERROR)

def query_iceberg():
    print("Connecting to Iceberg Catalog...")
    config = SEPAConfig()
    catalog = load_catalog("default", **config.iceberg_catalog_config)
    
    print("Loading table sepa.precios...")
    try:
        table = catalog.load_table("sepa.precios")
        print(f"Table loaded. Schema has {len(table.schema().fields)} fields.")
    except Exception as e:
        print(f"Table load failed: {e}")
        return

    # Convert to Arrow for DuckDB querying
    # Note: For massive tables, we'd use the Iceberg DuckDB extension directly, 
    # but passing the arrow table is fast enough for checking recent loads.
    print("Scanning table to Arrow (this might take a moment if data is large)...")
    arrow_table = table.scan().to_arrow()
    
    print(f"Loaded {arrow_table.num_rows} rows into memory.")
    
    print("\n Aggregation: Row Count by Date")
    duckdb.sql("""
        SELECT 
            fecha_vigencia,
            COUNT(*) as total_rows,
            COUNT(DISTINCT id_producto) as unique_products,
            COUNT(DISTINCT id_sucursal) as unique_branches
        FROM arrow_table 
        GROUP BY fecha_vigencia
        ORDER BY fecha_vigencia DESC
    """).show()

    print("\n Sample Data:")
    duckdb.sql("SELECT * FROM arrow_table LIMIT 5").show()

if __name__ == "__main__":
    query_iceberg()
