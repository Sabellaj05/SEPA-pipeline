{{ config(
    materialized='incremental',
    partition_by={
      "field": "fecha_vigencia",
      "data_type": "date"
    },
    unique_key=['fecha_vigencia', 'provincia']
) }}

SELECT
    p.fecha_vigencia,
    s.provincia,
    COUNT(DISTINCT p.id_sucursal)  AS sucursales_activas,
    COUNT(DISTINCT p.id_comercio)  AS cadenas_activas,
    COUNT(DISTINCT p.id_producto)  AS productos_reportados,
    COUNT(*)                       AS total_precios
FROM {{ ref('stg_precios') }} p
LEFT JOIN {{ ref('stg_dim_sucursales') }} s
    ON p.id_sucursal = s.id_sucursal
    AND p.id_comercio = s.id_comercio
WHERE 1=1
{% if is_incremental() %}
  AND p.fecha_vigencia > (SELECT MAX(fecha_vigencia) FROM {{ this }})
{% endif %}
GROUP BY 1, 2
