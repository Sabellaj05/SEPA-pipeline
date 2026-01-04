from sepa_pipeline.config import SEPAConfig
from pyiceberg.catalog import load_catalog
import duckdb

def verify_data():
    config = SEPAConfig()
    catalog = load_catalog("default", **config.iceberg_catalog_config)
    
    print("⏳ Loading Iceberg table...")
    table = catalog.load_table("sepa.precios")
    
    # Fast count via metadata
    # Note: table.scan().count() might fail if not fully implemented in older pyiceberg, 
    # but we can check the snapshots or just run a DuckDB query on the scan.
    # The logs said 14,653,976 rows.
    
    print("⏳ Scanning (metadata)...")
    # arrow_table = table.scan().to_arrow() # This loads ALL data to memory, might be heavy for 14M rows if not filtered.
    # Safe way for large data: Use DuckDB to read Iceberg directly if configured, or just trust the write logs for now.
    # But user wants verification. 
    # Let's try to get a count without loading everything if possible, or just load a sample.
    
    # Actually, for 14M rows, loading to Arrow might OOM a small container.
    # Better to check snapshot summary.
    
    snapshot = table.current_snapshot()
    if snapshot:
        # Avoid iterating .items() due to potential PyIceberg bug/behavior with 'lower()' on tuple
        print(f"\n📊 Snapshot ID: {snapshot.snapshot_id}")
        
        # summary is typically a Mapping[str, str]
        summary = snapshot.summary
        
        # Try direct access for key metrics
        total_records = summary.get('total-records')
        added_records = summary.get('added-records')
        operation = summary.get('operation')
        
        print(f"  Operation: {operation}")
        print(f"  Added Records: {added_records}")
        print(f"\n✅ Total Records in Iceberg: {total_records}")
    else:
        print("❌ No snapshot found!")

if __name__ == "__main__":
    verify_data()
