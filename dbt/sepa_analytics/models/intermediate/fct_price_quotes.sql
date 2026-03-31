{{ config(
    materialized='incremental',
    partition_by={
      "field": "fecha_vigencia",
      "data_type": "date"
    } if target.type == 'bigquery' else none,
    cluster_by=['id_producto', 'id_comercio'] if target.type == 'bigquery' else none,
    unique_key=['fecha_vigencia', 'id_comercio', 'id_sucursal', 'id_producto'],
    incremental_strategy='delete+insert' if target.type == 'duckdb' else none
) }}

{#
  Atomic price fact: exactly 1 row per (fecha_vigencia, id_comercio, id_sucursal, id_producto).
  When multiple scrapes exist for the same grain on the same day, latest scraped_at wins.
  Carries descripcion and marca from the Silver fact (these are cached on the source row).
  All downstream gold models should reference this instead of stg_precios directly.
#}

SELECT
    fecha_vigencia,
    id_comercio,
    id_bandera,
    id_sucursal,
    id_producto,
    descripcion,
    marca,
    precio_lista,
    precio_unitario_promo1,
    leyenda_promo1,
    precio_unitario_promo2,
    leyenda_promo2,
    scraped_at
FROM {{ ref('stg_precios') }}
WHERE TRUE
{% if is_incremental() %}
  {% if target.type == 'bigquery' %}
    AND fecha_vigencia >= DATE_SUB(
        COALESCE((SELECT MAX(fecha_vigencia) FROM {{ this }}), '2000-01-01'),
        INTERVAL 2 DAY
    )
  {% else %}
    AND fecha_vigencia >= (
        SELECT COALESCE(MAX(fecha_vigencia), DATE '2000-01-01') - INTERVAL 2 DAY FROM {{ this }}
    )
  {% endif %}
{% endif %}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY fecha_vigencia, id_comercio, id_sucursal, id_producto
    ORDER BY scraped_at DESC
) = 1
