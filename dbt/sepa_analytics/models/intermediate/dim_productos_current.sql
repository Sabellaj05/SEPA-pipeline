{{
    config(materialized='table')
}}

{#
  Current-state product dimension: exactly 1 row per id_producto.
  Reads directly from the raw Silver snapshot and applies deterministic dedup —
  latest fecha_vigencia wins. Materialized as TABLE to guarantee uniqueness at
  build time, avoiding the BigLake/QUALIFY view evaluation issue that caused
  fanout in downstream gold models.
#}

SELECT
    CAST(id_producto AS STRING) AS id_producto,
    CAST(id_comercio AS STRING) AS id_comercio,
    CAST(id_bandera AS STRING) AS id_bandera,
    CAST(id_sucursal AS STRING) AS id_sucursal,
    productos_ean AS ean,
    productos_descripcion AS descripcion,
    productos_marca AS marca,
    productos_cantidad_presentacion AS cantidad_presentacion,
    productos_unidad_medida_presentacion AS unidad_medida_presentacion,
    productos_precio_referencia AS precio_referencia,
    productos_cantidad_referencia AS cantidad_referencia,
    productos_unidad_medida_referencia AS unidad_medida_referencia,
    productos_leyenda_promo1 AS leyenda_promo1,
    productos_leyenda_promo2 AS leyenda_promo2
FROM {{ iceberg_source('dim_productos') }}
{% if target.type == 'duckdb' %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id_producto ORDER BY fecha_vigencia_day DESC) = 1
{% else %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id_producto ORDER BY fecha_vigencia DESC) = 1
{% endif %}
