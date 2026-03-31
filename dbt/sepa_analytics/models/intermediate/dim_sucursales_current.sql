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
    CAST(id_sucursal AS STRING) AS id_sucursal,
    CAST(id_comercio AS STRING) AS id_comercio,
    CAST(id_bandera AS STRING) AS id_bandera,
    sucursales_nombre AS nombre,
    sucursales_tipo AS tipo,
    sucursales_observaciones AS observaciones,
    sucursales_calle AS calle,
    sucursales_numero AS numero,
    sucursales_barrio AS barrio,
    sucursales_codigo_postal AS codigo_postal,
    sucursales_localidad AS localidad,
    sucursales_provincia AS provincia,
    CAST(sucursales_latitud AS {{ dbt.type_float() }}) AS latitud,
    CAST(sucursales_longitud AS {{ dbt.type_float() }}) AS longitud,
    sucursales_lunes_horario_atencion AS horario_lunes,
    sucursales_martes_horario_atencion AS horario_martes,
    sucursales_miercoles_horario_atencion AS horario_miercoles,
    sucursales_jueves_horario_atencion AS horario_jueves,
    sucursales_viernes_horario_atencion AS horario_viernes,
    sucursales_sabado_horario_atencion AS horario_sabado,
    sucursales_domingo_horario_atencion AS horario_domingo
FROM {{ iceberg_source('dim_sucursales') }}
{% if target.type == 'duckdb' %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id_sucursal, id_comercio ORDER BY fecha_vigencia_day DESC) = 1
{% else %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id_sucursal, id_comercio ORDER BY fecha_vigencia DESC) = 1
{% endif %}
