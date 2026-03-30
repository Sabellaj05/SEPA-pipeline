{{ config(
    materialized='incremental',
    partition_by={
      "field": "fecha_vigencia",
      "data_type": "date"
    } if target.type == 'bigquery' else none,
    cluster_by=['categoria_inferida', 'provincia'] if target.type == 'bigquery' else none,
    unique_key=['fecha_vigencia', 'id_producto', 'id_sucursal'],
    incremental_strategy='delete+insert' if target.type == 'duckdb' else none
) }}

{#
  Gold Model 3: Hypothesis Testing Dataset
  ─────────────────────────────────────────
  Denormalized flat table at store-level granularity with all dimensions.
  Input for: t-test (CABA vs Cordoba), ANOVA (across chains), Mann-Whitney, Chi-squared.
  Grain: (fecha_vigencia, id_producto, id_sucursal)
#}

SELECT
    p.fecha_vigencia,
    p.id_producto,
    p.id_sucursal,

    -- Product info (cleaned)
    {{ clean_description('d.descripcion') }} AS descripcion_clean,
    d.marca,
    {{ categorize_product(clean_description('d.descripcion')) }} AS categoria_inferida,

    -- Chain info
    c.bandera_nombre,

    -- Geography
    s.provincia,
    s.localidad,

    -- Price data
    p.precio_lista,
    CASE WHEN p.precio_unitario_promo1 IS NOT NULL THEN true ELSE false END AS tiene_promo,
    COALESCE(p.precio_unitario_promo1, p.precio_lista) AS precio_efectivo

FROM {{ ref('stg_precios') }} p
LEFT JOIN {{ ref('stg_dim_productos') }} d
    ON p.id_producto = d.id_producto
LEFT JOIN {{ ref('stg_dim_sucursales') }} s
    ON p.id_sucursal = s.id_sucursal
    AND p.id_comercio = s.id_comercio
LEFT JOIN {{ ref('stg_dim_comercios') }} c
    ON p.id_comercio = c.id_comercio
    AND p.id_bandera = c.id_bandera
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
