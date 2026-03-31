-- Asserts that dim_comercios_current has no duplicate rows at its declared grain:
-- (id_comercio, id_bandera)
-- Any rows returned = test failure.

SELECT
    id_comercio,
    id_bandera,
    COUNT(*) AS n
FROM {{ ref('dim_comercios_current') }}
GROUP BY 1, 2
HAVING COUNT(*) > 1
