from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo
from sepa_pipeline.config import SEPAConfig

def verify_all():
    config = SEPAConfig()
    catalog = load_catalog("default", **config.iceberg_catalog_config)
    table = catalog.load_table("sepa.precios")
    
    for date_str in ["2026-01-05", "2026-01-04"]:
        print(f"Scanning {date_str}...")
        scan = table.scan(row_filter=EqualTo("fecha_vigencia", date_str))
        count = len(scan.to_arrow())
        print(f"Count for {date_str}: {count}")

if __name__ == "__main__":
    verify_all()
