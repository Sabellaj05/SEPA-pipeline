{{
    config(materialized='table')
}}

{#
  Current-state merchant dimension: exactly 1 row per (id_comercio, id_bandera).
  Reads directly from the raw Silver snapshot and applies deterministic dedup —
  latest fecha_vigencia wins. Materialized as TABLE to guarantee uniqueness.
#}

SELECT
    CAST(id_comercio AS STRING) AS id_comercio,
    CAST(id_bandera AS STRING) AS id_bandera,
    comercio_cuit AS cuit,
    comercio_razon_social AS razon_social,
    comercio_bandera_nombre AS bandera_nombre,
    comercio_bandera_url AS bandera_url,
    comercio_version_sepa AS version_sepa,
    comercio_ultima_actualizacion AS ultima_actualizacion
FROM {{ iceberg_source('dim_comercios') }}
{% if target.type == 'duckdb' %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id_comercio, id_bandera ORDER BY fecha_vigencia_day DESC) = 1
{% else %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id_comercio, id_bandera ORDER BY fecha_vigencia DESC) = 1
{% endif %}
