{{ config(
    materialized='incremental',
    partition_by={
      "field": "fecha_vigencia",
      "data_type": "date"
    } if target.type == 'bigquery' else none,
    cluster_by=['id_producto'] if target.type == 'bigquery' else none,
    unique_key=['id_producto', 'fecha_vigencia'],
    incremental_strategy='delete+insert' if target.type == 'duckdb' else none
) }}

SELECT
    p.fecha_vigencia,
    p.id_producto,
    -- Cross-adapter aggregation: ANY_VALUE (BQ) vs MIN (DuckDB)
    {% if target.type == 'bigquery' %}
        ANY_VALUE(d.descripcion)           AS descripcion,
        ANY_VALUE(d.marca)                 AS marca,
    {% else %}
        MIN(d.descripcion)                 AS descripcion,
        MIN(d.marca)                       AS marca,
    {% endif %}
    COUNT(*)                           AS num_observaciones,
    AVG(p.precio_lista)                AS precio_promedio,
    MIN(p.precio_lista)                AS precio_minimo,
    MAX(p.precio_lista)                AS precio_maximo,
    -- NULL means no active promotion -- the signal is intentional, not a defect.
    MIN(p.precio_unitario_promo1) AS mejor_precio_promo
FROM {{ ref('stg_precios') }} p
LEFT JOIN {{ ref('stg_dim_productos') }} d
    ON p.id_producto = d.id_producto
WHERE p.precio_lista > 0
{% if is_incremental() %}
  -- 2-day lookback window handles late-arriving data from delayed ingestion runs
  {% if target.type == 'bigquery' %}
    AND p.fecha_vigencia >= DATE_SUB(
        (SELECT MAX(fecha_vigencia) FROM {{ this }}),
        INTERVAL 2 DAY
    )
  {% else %}
    AND p.fecha_vigencia >= (
        SELECT MAX(fecha_vigencia) - INTERVAL 2 DAY FROM {{ this }}
    )
  {% endif %}
{% endif %}
GROUP BY 1, 2
