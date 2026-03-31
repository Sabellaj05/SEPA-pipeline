-- Asserts that fct_price_quotes has no duplicate rows at its declared grain:
-- (fecha_vigencia, id_comercio, id_sucursal, id_producto)
-- Any rows returned = test failure.

SELECT
    fecha_vigencia,
    id_comercio,
    id_sucursal,
    id_producto,
    COUNT(*) AS n
FROM {{ ref('fct_price_quotes') }}
GROUP BY 1, 2, 3, 4
HAVING COUNT(*) > 1
