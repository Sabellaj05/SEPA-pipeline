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
    id_producto,
    ean,
    descripcion,
    marca,
    cantidad_presentacion,
    unidad_medida_presentacion
FROM {{ ref('stg_dim_productos') }}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY id_producto
    ORDER BY fecha_vigencia DESC
) = 1
