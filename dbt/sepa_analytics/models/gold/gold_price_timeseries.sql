{{ config(
    materialized='incremental',
    partition_by={
      "field": "fecha_vigencia",
      "data_type": "date"
    } if target.type == 'bigquery' else none,
    cluster_by=['categoria_inferida', 'provincia'] if target.type == 'bigquery' else none,
    unique_key=['fecha_vigencia', 'id_producto', 'provincia'],
    incremental_strategy='delete+insert' if target.type == 'duckdb' else none
) }}

{#
  Gold Model 1: Time Series / IPC Analysis
  ─────────────────────────────────────────
  Pre-aggregated daily price series by product and province.
  Input for: monthly inflation, seasonal decomposition, ADF test, SEPA vs INDEC IPC.
  Grain: (fecha_vigencia, id_producto, provincia)

  Sources:
  - fct_price_quotes: atomic deduped fact, 1 row per (fecha_vigencia, id_comercio,
    id_sucursal, id_producto). Carries descripcion + marca directly — no product
    dimension join needed or used here.
  - dim_sucursales_current: table-materialized, 1 row per (id_sucursal, id_comercio),
    joined only for provincia. Guaranteed 1:1 — no fanout risk.
#}

WITH priced AS (
    SELECT
        f.fecha_vigencia,
        f.id_producto,
        f.id_sucursal,
        f.precio_lista,
        {{ clean_description('f.descripcion') }} AS descripcion_clean,
        f.marca,
        s.provincia
    FROM {{ ref('fct_price_quotes') }} f
    INNER JOIN {{ ref('dim_sucursales_current') }} s
        ON f.id_sucursal = s.id_sucursal
        AND f.id_comercio = s.id_comercio
    WHERE f.precio_lista >= 10.0
      AND s.provincia IS NOT NULL
    {% if is_incremental() %}
      {% if target.type == 'bigquery' %}
        AND f.fecha_vigencia >= DATE_SUB(
            COALESCE((SELECT MAX(fecha_vigencia) FROM {{ this }}), '2000-01-01'),
            INTERVAL 2 DAY
        )
      {% else %}
        AND f.fecha_vigencia >= (
            SELECT COALESCE(MAX(fecha_vigencia), DATE '2000-01-01') - INTERVAL 2 DAY FROM {{ this }}
        )
      {% endif %}
    {% endif %}
)

SELECT
    fecha_vigencia,
    id_producto,
    {% if target.type == 'bigquery' %}
        ANY_VALUE(descripcion_clean) AS descripcion_clean,
        ANY_VALUE(marca) AS marca,
    {% else %}
        MIN(descripcion_clean) AS descripcion_clean,
        MIN(marca) AS marca,
    {% endif %}
    {{ categorize_product('MIN(descripcion_clean)') }} AS categoria_inferida,
    provincia,
    AVG(precio_lista) AS precio_promedio,
    {% if target.type == 'bigquery' %}
        APPROX_QUANTILES(precio_lista, 2)[OFFSET(1)] AS precio_mediana,
    {% else %}
        MEDIAN(precio_lista) AS precio_mediana,
    {% endif %}
    MIN(precio_lista) AS precio_min,
    MAX(precio_lista) AS precio_max,
    STDDEV(precio_lista) AS precio_desvio,
    COUNT(*) AS num_observaciones,
    COUNT(DISTINCT id_sucursal) AS num_sucursales
FROM priced
GROUP BY fecha_vigencia, id_producto, provincia
