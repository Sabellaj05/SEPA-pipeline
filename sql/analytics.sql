-- ============================================================================
-- SEPA Data Verification & Analytics Queries
-- ============================================================================

-- 1. Basic Counts
-- Verify the volume of data loaded.
SELECT 'Comercios' as table_name, COUNT(1) as count FROM comercios
UNION ALL
SELECT 'Sucursales', COUNT(1) FROM sucursales
UNION ALL
SELECT 'Productos Master', COUNT(1) FROM productos_master
UNION ALL
SELECT 'Precios (Fact)', COUNT(1) FROM precios;

-- 2. Price Distribution by Banner (Chain)
-- Check which chains are providing the most data.
SELECT 
    c.comercio_bandera_nombre,
    COUNT(1) as price_count
FROM precios p
JOIN comercios c ON p.id_comercio = c.id_comercio AND p.id_bandera = c.id_bandera
GROUP BY c.comercio_bandera_nombre
ORDER BY price_count DESC;

-- 3. Price Outliers Check
-- Find suspicious prices (e.g., extremely high or low).
SELECT 
    pm.productos_descripcion,
    pm.productos_marca,
    p.productos_precio_lista,
    c.comercio_bandera_nombre
FROM precios p
JOIN productos_master pm ON p.id_producto = pm.id_producto
JOIN comercios c ON p.id_comercio = c.id_comercio AND p.id_bandera = c.id_bandera
WHERE p.productos_precio_lista > 1000000 -- Adjust threshold as needed
   OR p.productos_precio_lista < 1
ORDER BY p.productos_precio_lista DESC
LIMIT 50;

-- 4. Product Coverage across Chains
-- See which products are sold in the most chains.
SELECT 
    pm.productos_descripcion,
    COUNT(DISTINCT c.comercio_bandera_nombre) as chain_count
FROM precios p
JOIN productos_master pm ON p.id_producto = pm.id_producto
JOIN comercios c ON p.id_comercio = c.id_comercio AND p.id_bandera = c.id_bandera
GROUP BY pm.productos_descripcion
ORDER BY chain_count DESC
LIMIT 20;

-- 5. Latest Data Check
-- Ensure we are looking at the most recent data.
SELECT MAX(scraped_at) as latest_scrape_date FROM precios;
