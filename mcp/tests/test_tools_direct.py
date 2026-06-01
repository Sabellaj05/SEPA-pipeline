"""
Test the MCP server tools directly without using the MCP protocol.
"""

from lakehouse_mcp.server import query_bronze_audit, query_silver_audit


def test_audit_tools():
    print("Testing query_bronze_audit...")
    bronze_results = query_bronze_audit(limit=2)
    print(f"Bronze results found: {len(bronze_results)}")
    for r in bronze_results:
        print(
            f" - Date: {r.get('fecha_vigencia')}, "
            f"Type: {r.get('table_type')}, "
            f"Status: {r.get('parquet_row_count')} rows"
        )

    print("\nTesting query_silver_audit...")
    silver_results = query_silver_audit(limit=2)
    print(f"Silver results found: {len(silver_results)}")
    for r in silver_results:
        print(
            f" - Date: {r.get('fecha_vigencia')}, "
            f"Loaded: {r.get('silver_loaded')}, "
            f"Dropped: {r.get('validation_dropped')}"
        )

    print("\nTesting filtering by date (2026-01-01)...")
    filtered = query_silver_audit(fecha_vigencia="2026-01-01")
    print(f"Filtered silver results: {len(filtered)}")

if __name__ == "__main__":
    try:
        test_audit_tools()
    except Exception as e:
        print(f"Test failed: {e}")
