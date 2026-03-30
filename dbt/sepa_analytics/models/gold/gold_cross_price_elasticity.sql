{{ config(
    materialized='incremental',
    partition_by={
      "field": "fecha_vigencia",
      "data_type": "date"
    } if target.type == 'bigquery' else none,
    cluster_by=['categoria_inferida'] if target.type == 'bigquery' else none,
    unique_key=['fecha_vigencia', 'id_producto', 'cadena_a', 'cadena_b'],
    incremental_strategy='delete+insert' if target.type == 'duckdb' else none
) }}

{#
  Gold Model 2: Cross-Price Elasticity
  ─────────────────────────────────────
  Paired chain prices for the same product on the same date.
  Input for: log-log OLS regression, cross-price elasticity estimation.
  Grain: (fecha_vigencia, id_producto, cadena_a, cadena_b)
#}

WITH chain_avg_prices AS (
    -- Aggregate to one price per product per chain per day
    SELECT
        p.fecha_vigencia,
        p.id_producto,
        {{ clean_description('d.descripcion') }} AS descripcion_clean,
        d.marca,
        c.bandera_nombre AS cadena,
        AVG(p.precio_lista) AS precio_promedio,
        COUNT(*) AS num_observaciones
    FROM {{ ref('stg_precios') }} p
    LEFT JOIN {{ ref('stg_dim_productos') }} d
        ON p.id_producto = d.id_producto
    LEFT JOIN {{ ref('stg_dim_comercios') }} c
        ON p.id_comercio = c.id_comercio
        AND p.id_bandera = c.id_bandera
    WHERE p.precio_lista >= 10.0
      AND c.bandera_nombre IS NOT NULL
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
    GROUP BY 1, 2, 3, 4, 5
    HAVING COUNT(*) >= 3  -- minimum observations for meaningful average
)

SELECT
    a.fecha_vigencia,
    a.id_producto,
    a.descripcion_clean,
    a.marca,
    {{ categorize_product('a.descripcion_clean') }} AS categoria_inferida,
    a.cadena AS cadena_a,
    b.cadena AS cadena_b,
    a.precio_promedio AS precio_promedio_a,
    b.precio_promedio AS precio_promedio_b,
    LN(a.precio_promedio) AS ln_precio_a,
    LN(b.precio_promedio) AS ln_precio_b,
    a.num_observaciones AS num_obs_a,
    b.num_observaciones AS num_obs_b,
    -- Pre-computed price ratio for quick analysis
    (b.precio_promedio - a.precio_promedio) / a.precio_promedio * 100 AS diferencia_pct
FROM chain_avg_prices a
INNER JOIN chain_avg_prices b
    ON a.fecha_vigencia = b.fecha_vigencia
    AND a.id_producto = b.id_producto
    AND a.cadena < b.cadena  -- avoid duplicate/mirrored pairs
