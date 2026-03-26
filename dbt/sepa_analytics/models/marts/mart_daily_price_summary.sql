{{ config(
    materialized='incremental',
    partition_by={
      "field": "fecha_vigencia",
      "data_type": "date"
    },
    cluster_by=['id_producto'],
    unique_key=['id_producto', 'fecha_vigencia']
) }}

SELECT
    p.fecha_vigencia,
    p.id_producto,
    -- ANY_VALUE: semantically correct -- one id_producto maps to one descripcion/marca.
    -- Faster than MAX (no string comparison) and communicates intent clearly.
    ANY_VALUE(d.descripcion)           AS descripcion,
    ANY_VALUE(d.marca)                 AS marca,
    COUNT(*)                           AS num_observaciones,
    AVG(p.precio_lista)                AS precio_promedio,
    MIN(p.precio_lista)                AS precio_minimo,
    MAX(p.precio_lista)                AS precio_maximo,
    -- NULL means no active promotion -- the signal is intentional, not a defect.
    MIN(p.precio_unitario_promo1) AS mejor_precio_promo
FROM {{ ref('stg_precios') }} p
LEFT JOIN {{ ref('stg_dim_productos') }} d
    ON p.id_producto = d.id_producto
WHERE p.precio_lista > 0
{% if is_incremental() %}
  -- 2-day lookback window handles late-arriving data from delayed ingestion runs
  AND p.fecha_vigencia >= DATE_SUB(
      (SELECT MAX(fecha_vigencia) FROM {{ this }}),
      INTERVAL 2 DAY
  )
{% endif %}
GROUP BY 1, 2
