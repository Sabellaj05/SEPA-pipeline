{{ config(
    materialized='incremental',
    partition_by={
      "field": "fecha_vigencia",
      "data_type": "date"
    },
    unique_key=['id_producto', 'fecha_vigencia']
) }}

SELECT
    p.fecha_vigencia,
    p.id_producto,
    -- join dimension for product attributes (fact is narrow, no cached columns)
    MAX(d.descripcion)           AS descripcion,
    MAX(d.marca)                 AS marca,
    COUNT(*)                     AS num_observaciones,
    AVG(p.precio_lista)          AS precio_promedio,
    MIN(p.precio_lista)          AS precio_minimo,
    MAX(p.precio_lista)          AS precio_maximo,
    MIN(p.precio_unitario_promo1) AS mejor_precio_promo
FROM {{ ref('stg_precios') }} p
LEFT JOIN {{ ref('stg_dim_productos') }} d
    ON p.id_producto = d.id_producto
WHERE p.precio_lista > 0
{% if is_incremental() %}
  AND p.fecha_vigencia > (SELECT MAX(fecha_vigencia) FROM {{ this }})
{% endif %}
GROUP BY 1, 2
