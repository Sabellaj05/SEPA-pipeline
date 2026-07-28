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
    {% if target.type == 'bigquery' %}
        ANY_VALUE(p.descripcion)           AS descripcion,
        ANY_VALUE(p.marca)                 AS marca,
    {% else %}
        MIN(p.descripcion)                 AS descripcion,
        MIN(p.marca)                       AS marca,
    {% endif %}
    COUNT(*)                           AS num_observaciones,
    AVG(p.precio_lista)                AS precio_promedio,
    MIN(p.precio_lista)                AS precio_minimo,
    MAX(p.precio_lista)                AS precio_maximo,
    -- NULL means no active promotion -- the signal is intentional, not a defect.
    MIN(p.precio_unitario_promo1) AS mejor_precio_promo
FROM {{ ref('fct_price_quotes') }} p
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
