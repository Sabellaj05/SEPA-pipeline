{{ config(
    materialized='incremental',
    partition_by={
      "field": "fecha_vigencia",
      "data_type": "date"
    } if target.type == 'bigquery' else none,
    cluster_by=['provincia'] if target.type == 'bigquery' else none,
    unique_key=['fecha_vigencia', 'provincia'],
    incremental_strategy='delete+insert' if target.type == 'duckdb' else none
) }}

SELECT
    p.fecha_vigencia,
    s.provincia,
    COUNT(DISTINCT p.id_sucursal)  AS sucursales_activas,
    COUNT(DISTINCT p.id_comercio)  AS cadenas_activas,
    COUNT(DISTINCT p.id_producto)  AS productos_reportados,
    COUNT(p.id_producto)           AS total_precios
FROM {{ ref('stg_precios') }} p
LEFT JOIN {{ ref('stg_dim_sucursales') }} s
    ON p.id_sucursal = s.id_sucursal
    AND p.id_comercio = s.id_comercio
WHERE 1=1
{% if is_incremental() %}
  {% if target.type == 'bigquery' %}
    AND p.fecha_vigencia >= DATE_SUB(
        (SELECT MAX(fecha_vigencia) FROM {{ this }}),
        INTERVAL 2 DAY
    )
  {% else %}
    AND p.fecha_vigencia >= (
        SELECT MAX(fecha_vigencia) - INTERVAL 2 DAY FROM {{ this }}
    )
  {% endif %}
{% endif %}
GROUP BY 1, 2
