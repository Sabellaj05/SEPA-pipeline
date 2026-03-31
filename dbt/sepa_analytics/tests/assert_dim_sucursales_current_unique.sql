-- Asserts that dim_sucursales_current has no duplicate rows at its declared grain:
-- (id_sucursal, id_comercio)
-- Any rows returned = test failure.

SELECT
    id_sucursal,
    id_comercio,
    COUNT(*) AS n
FROM {{ ref('dim_sucursales_current') }}
GROUP BY 1, 2
HAVING COUNT(*) > 1
