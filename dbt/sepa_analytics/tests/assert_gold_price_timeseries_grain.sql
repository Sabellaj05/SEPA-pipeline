-- Asserts that gold_price_timeseries has no duplicate rows at its declared grain:
-- (fecha_vigencia, id_producto, provincia)
-- Any rows returned = test failure.

SELECT
    fecha_vigencia,
    id_producto,
    provincia,
    COUNT(*) AS n
FROM {{ ref('gold_price_timeseries') }}
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1
