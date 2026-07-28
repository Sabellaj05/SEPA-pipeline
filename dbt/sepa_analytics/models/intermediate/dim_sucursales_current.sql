{{
    config(materialized='table')
}}

{#
  Current-state store dimension: exactly 1 row per (id_sucursal, id_comercio).
  Reads directly from the raw Silver snapshot and applies deterministic dedup —
  latest fecha_vigencia wins. Materialized as TABLE to guarantee uniqueness.
  The join key (id_sucursal, id_comercio) matches the join used in all gold models.
#}

SELECT
    id_sucursal,
    id_comercio,
    id_bandera,
    nombre,
    tipo,
    calle,
    numero,
    barrio,
    codigo_postal,
    localidad,
    provincia,
    latitud,
    longitud
FROM {{ ref('stg_dim_sucursales') }}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY id_sucursal, id_comercio
    ORDER BY fecha_vigencia DESC
) = 1
