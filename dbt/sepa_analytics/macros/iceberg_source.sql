{% macro iceberg_source(table_name) %}
  {#
    Cross-adapter source macro for Iceberg tables.
    - DuckDB (dev): reads Parquet data files directly from MinIO with Hive partitioning.
      iceberg_scan() is unusable due to PyIceberg metadata format incompatibility
      with DuckDB's Iceberg extension (null parent_snapshot_id, non-standard naming).
    - BigQuery (prod): references the BigLake-managed table via source().
  #}
  {% if target.type == 'duckdb' %}
    read_parquet('s3://{{ env_var("MINIO_BUCKET", "sepa-lakehouse") }}/silver/iceberg/sepa/{{ table_name }}/data/**/*.parquet', hive_partitioning = true, union_by_name = true)
  {% else %}
    {{ source('sepa_silver', table_name) }}
  {% endif %}
{% endmacro %}
