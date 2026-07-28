{{
    config(materialized='table')
}}

{#
  Current-state merchant dimension: exactly 1 row per (id_comercio, id_bandera).
  Reads directly from the raw Silver snapshot and applies deterministic dedup —
  latest fecha_vigencia wins. Materialized as TABLE to guarantee uniqueness.
#}

SELECT
    id_comercio,
    id_bandera,
    cuit,
    razon_social,
    bandera_nombre,
    bandera_url
FROM {{ ref('stg_dim_comercios') }}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY id_comercio, id_bandera
    ORDER BY fecha_vigencia DESC
) = 1
