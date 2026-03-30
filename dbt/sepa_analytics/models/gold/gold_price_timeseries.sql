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
#}

WITH cleaned_prices AS (
    SELECT
        p.fecha_vigencia,
        p.id_producto,
        p.id_sucursal,
        p.precio_lista,
        {{ clean_description('d.descripcion') }} AS descripcion_clean,
        d.marca,
        s.provincia
    FROM {{ ref('stg_precios') }} p
    LEFT JOIN {{ ref('stg_dim_productos') }} d
        ON p.id_producto = d.id_producto
    LEFT JOIN {{ ref('stg_dim_sucursales') }} s
        ON p.id_sucursal = s.id_sucursal
        AND p.id_comercio = s.id_comercio
    WHERE p.precio_lista >= 10.0
      AND s.provincia IS NOT NULL
    {% if is_incremental() %}
      {% if target.type == 'bigquery' %}
        AND p.fecha_vigencia >= DATE_SUB(
            COALESCE((SELECT MAX(fecha_vigencia) FROM {{ this }}), '2000-01-01'),
            INTERVAL 2 DAY
        )
      {% else %}
        AND p.fecha_vigencia >= (
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
FROM cleaned_prices
GROUP BY fecha_vigencia, id_producto, provincia
