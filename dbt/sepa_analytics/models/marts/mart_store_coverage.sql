{{ config(
    materialized='incremental',
    partition_by={
      "field": "fecha_vigencia",
      "data_type": "date"
    },
    cluster_by=['provincia'],
    unique_key=['fecha_vigencia', 'provincia']
) }}

SELECT
    p.fecha_vigencia,
    s.provincia,
    COUNT(DISTINCT p.id_sucursal)  AS sucursales_activas,
    COUNT(DISTINCT p.id_comercio)  AS cadenas_activas,
    COUNT(DISTINCT p.id_producto)  AS productos_reportados,
    -- COUNT(p.id_producto) instead of COUNT(*): explicit about what we count,
    -- skips NULL id_producto rows defensively on the LEFT JOIN.
    COUNT(p.id_producto)           AS total_precios
FROM {{ ref('stg_precios') }} p
LEFT JOIN {{ ref('stg_dim_sucursales') }} s
    ON p.id_sucursal = s.id_sucursal
    AND p.id_comercio = s.id_comercio
WHERE 1=1
{% if is_incremental() %}
  -- 2-day lookback window handles late-arriving data from delayed ingestion runs
  AND p.fecha_vigencia >= DATE_SUB(
      (SELECT MAX(fecha_vigencia) FROM {{ this }}),
      INTERVAL 2 DAY
  )
{% endif %}
GROUP BY 1, 2
