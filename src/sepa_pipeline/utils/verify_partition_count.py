from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo
from sepa_pipeline.config import SEPAConfig
from sepa_pipeline.utils.fecha import Fecha
import pyarrow.compute as pc

def verify_partition():
    config = SEPAConfig()
    catalog = load_catalog("default", **config.iceberg_catalog_config)
    table = catalog.load_table("sepa.precios")
    
    # Target date: Today (2026-01-05) or from Fecha class
    # User said "today (2025-01-05)" in prompt?? Wait, prompt said 2026 in logs.
    # The logs say: "Cleaning up existing data for date: 2026-01-05"
    target_date_str = "2026-01-05"
    
    print(f"Scanning table sepa.precios for fecha_vigencia = {target_date_str}...")
    
    # Scan with filter
    # converting string to date might be needed depending on how EqualTo handles it
    # PyIceberg expects string 'YYYY-MM-DD' for Date type usually, or date object.
    
    scan = table.scan(row_filter=EqualTo("fecha_vigencia", target_date_str))
    arrow_table = scan.to_arrow()
    count = len(arrow_table)
    
    print(f"Row count for {target_date_str}: {count}")

if __name__ == "__main__":
    verify_partition()
