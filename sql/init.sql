-- ============================================================================
-- SEPA Database Schema - Optimized for 15M rows/day
-- ============================================================================

-- ============================================================================
-- DIMENSION TABLE: comercios
-- Stores company and brand (bandera) information
-- One row per bandera (a comercio can have multiple banderas)
-- ============================================================================
CREATE TABLE comercios (
    id_comercio VARCHAR(50) NOT NULL,
    id_bandera INTEGER NOT NULL,
    comercio_cuit BIGINT NOT NULL,
    comercio_razon_social VARCHAR(255) NOT NULL,
    comercio_bandera_nombre VARCHAR(255) NOT NULL,
    comercio_bandera_url VARCHAR(500),
    comercio_version_sepa NUMERIC(3,1),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    PRIMARY KEY (id_comercio, id_bandera)
);

CREATE INDEX idx_comercios_cuit ON comercios(comercio_cuit);
CREATE INDEX idx_comercios_bandera_nombre ON comercios(comercio_bandera_nombre);

-- ============================================================================
-- DIMENSION TABLE: sucursales
-- Stores branch/store information
-- Composite key: comercio + bandera + sucursal
-- ============================================================================
CREATE TABLE sucursales (
    id_comercio VARCHAR(50) NOT NULL,
    id_bandera INTEGER NOT NULL,
    id_sucursal INTEGER NOT NULL,
    sucursales_nombre VARCHAR(255) NOT NULL,
    sucursales_tipo VARCHAR(50) NOT NULL, -- Hipermercado, Supermercado, Autoservicio, Tradicional, Web
    sucursales_calle VARCHAR(255),
    sucursales_numero INTEGER,
    sucursales_latitud NUMERIC(10, 6),
    sucursales_longitud NUMERIC(10, 6),
    sucursales_observaciones TEXT,
    sucursales_barrio VARCHAR(255),
    sucursales_codigo_postal INTEGER,
    sucursales_localidad VARCHAR(255) NOT NULL,
    sucursales_provincia VARCHAR(10) NOT NULL, -- ISO 3166-2 format (AR-B, AR-C, etc.)
    sucursales_lunes_horario_atencion VARCHAR(100),
    sucursales_martes_horario_atencion VARCHAR(100),
    sucursales_miercoles_horario_atencion VARCHAR(100),
    sucursales_jueves_horario_atencion VARCHAR(100),
    sucursales_viernes_horario_atencion VARCHAR(100),
    sucursales_sabado_horario_atencion VARCHAR(100),
    sucursales_domingo_horario_atencion VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    PRIMARY KEY (id_comercio, id_bandera, id_sucursal),
    FOREIGN KEY (id_comercio, id_bandera) REFERENCES comercios(id_comercio, id_bandera)
);

CREATE INDEX idx_sucursales_tipo ON sucursales(sucursales_tipo);
CREATE INDEX idx_sucursales_localidad ON sucursales(sucursales_localidad);
CREATE INDEX idx_sucursales_provincia ON sucursales(sucursales_provincia);
CREATE INDEX idx_sucursales_location ON sucursales(sucursales_latitud, sucursales_longitud)
    WHERE sucursales_latitud IS NOT NULL AND sucursales_longitud IS NOT NULL;

-- ============================================================================
-- MASTER TABLE: productos_master
-- Stores product catalog information (normalized)
-- This separates relatively static product info from daily price observations
-- ============================================================================
CREATE TABLE productos_master (
    id_producto BIGINT PRIMARY KEY,
    productos_ean BOOLEAN NOT NULL, -- TRUE if id_producto is EAN/UPC, FALSE if internal code
    productos_descripcion TEXT NOT NULL,
    productos_cantidad_presentacion NUMERIC(10, 2),
    productos_unidad_medida_presentacion VARCHAR(50),
    productos_marca VARCHAR(255),
    productos_cantidad_referencia NUMERIC(10, 2),
    productos_unidad_medida_referencia VARCHAR(50),
    first_seen TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_seen TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_productos_master_marca ON productos_master(productos_marca);
CREATE INDEX idx_productos_master_descripcion_gin ON productos_master
    USING gin(to_tsvector('spanish', productos_descripcion));

-- ============================================================================
-- FACT TABLE: precios (partitioned by date)
-- Daily price observations - THE MAIN TABLE
-- Partitioned by scraped_at for efficient querying and maintenance
-- ~15M rows per day = ~450M rows per month
-- ============================================================================
CREATE TABLE precios (
    id BIGSERIAL NOT NULL,
    id_comercio VARCHAR(50) NOT NULL,
    id_bandera INTEGER NOT NULL,
    id_sucursal INTEGER NOT NULL,
    id_producto BIGINT NOT NULL,

    -- Price information
    productos_precio_lista NUMERIC(10, 2) NOT NULL,
    productos_precio_referencia NUMERIC(10, 2),

    -- Promotional prices (optional)
    productos_precio_unitario_promo1 NUMERIC(10, 2),
    productos_leyenda_promo1 TEXT,
    productos_precio_unitario_promo2 NUMERIC(10, 2),
    productos_leyenda_promo2 TEXT,

    -- Metadata
    scraped_at TIMESTAMP WITH TIME ZONE NOT NULL,
    fecha_vigencia DATE NOT NULL, -- The date these prices are valid for

    -- Denormalized fields for query performance (optional - can be joined instead)
    productos_descripcion TEXT,
    productos_marca VARCHAR(255),

    PRIMARY KEY (id, scraped_at)
) PARTITION BY RANGE (scraped_at);

-- Indexes on parent table (inherited by all partitions)
CREATE INDEX idx_precios_id_producto ON precios(id_producto);
CREATE INDEX idx_precios_sucursal ON precios(id_comercio, id_bandera, id_sucursal);
CREATE INDEX idx_precios_comercio ON precios(id_comercio);
CREATE INDEX idx_precios_fecha_vigencia ON precios(fecha_vigencia);
CREATE INDEX idx_precios_marca ON precios(productos_marca) WHERE productos_marca IS NOT NULL;

-- Composite indexes for common query patterns
CREATE INDEX idx_precios_producto_sucursal ON precios(id_producto, id_sucursal);
CREATE INDEX idx_precios_producto_fecha ON precios(id_producto, fecha_vigencia);


-- ============================================================================
-- PARTITION MANAGEMENT
-- ============================================================================

CREATE OR REPLACE FUNCTION create_precios_partition(partition_date DATE)
RETURNS VOID AS $$
DECLARE
    partition_name TEXT;
    start_ts TIMESTAMPTZ;
    end_ts   TIMESTAMPTZ;
BEGIN
    -- Partition table name
    partition_name := 'precios_' || to_char(partition_date, 'YYYY_MM_DD');

    -- Explicit UTC boundaries
    start_ts := partition_date::timestamptz;            -- 00:00:00+00
    end_ts   := (partition_date + 1)::timestamptz;      -- next day 00:00:00+00

    -- Check if partition exists
    IF NOT EXISTS (
        SELECT 1
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = partition_name
          AND n.nspname = 'public'
    ) THEN

        EXECUTE format(
            'CREATE TABLE %I PARTITION OF precios
             FOR VALUES FROM (%L) TO (%L)',
            partition_name,
            start_ts,
            end_ts
        );

        RAISE NOTICE 'Created partition: % (% → %)',
            partition_name, start_ts, end_ts;

    ELSE
        RAISE NOTICE 'Partition % already exists', partition_name;
    END IF;
END;
$$ LANGUAGE plpgsql;


-- Function to create partitions for a date range
CREATE OR REPLACE FUNCTION create_precios_partitions_range(
    start_date DATE,
    end_date DATE
)
RETURNS VOID AS $$
DECLARE
    curr_date DATE;
BEGIN
    curr_date := start_date;
    WHILE curr_date <= end_date LOOP
        PERFORM create_precios_partition(curr_date);
        curr_date := curr_date + INTERVAL '1 day';
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Create partitions for the next 30 days
SELECT create_precios_partitions_range(
    (CURRENT_DATE - INTERVAL '7 days')::date,
    (CURRENT_DATE + INTERVAL '30 days')::date
);

-- ============================================================================
-- VIEWS FOR EASIER QUERYING
-- ============================================================================

-- View to get latest prices for all products
CREATE OR REPLACE VIEW v_precios_latest AS
SELECT DISTINCT ON (p.id_comercio, p.id_bandera, p.id_sucursal, p.id_producto)
    p.*,
    s.sucursales_nombre,
    s.sucursales_localidad,
    s.sucursales_provincia,
    c.comercio_bandera_nombre
FROM precios p
JOIN sucursales s ON (
    p.id_comercio = s.id_comercio AND
    p.id_bandera = s.id_bandera AND
    p.id_sucursal = s.id_sucursal
)
JOIN comercios c ON (
    p.id_comercio = c.id_comercio AND
    p.id_bandera = c.id_bandera
)
ORDER BY p.id_comercio, p.id_bandera, p.id_sucursal, p.id_producto, p.scraped_at DESC;

-- View for price comparison across branches
CREATE OR REPLACE VIEW v_precio_comparacion AS
SELECT 
    p.id_producto,
    pm.productos_descripcion,
    pm.productos_marca,
    p.id_comercio,
    c.comercio_bandera_nombre,
    p.id_sucursal,
    s.sucursales_nombre,
    s.sucursales_localidad,
    p.productos_precio_lista,
    p.fecha_vigencia,
    AVG(p.productos_precio_lista) OVER (PARTITION BY p.id_producto, p.fecha_vigencia) as precio_promedio,
    MIN(p.productos_precio_lista) OVER (PARTITION BY p.id_producto, p.fecha_vigencia) as precio_minimo,
    MAX(p.productos_precio_lista) OVER (PARTITION BY p.id_producto, p.fecha_vigencia) as precio_maximo
FROM precios p
JOIN productos_master pm ON p.id_producto = pm.id_producto
JOIN sucursales s ON (
    p.id_comercio = s.id_comercio AND
    p.id_bandera = s.id_bandera AND
    p.id_sucursal = s.id_sucursal
)
JOIN comercios c ON (
    p.id_comercio = c.id_comercio AND
    p.id_bandera = c.id_bandera
)
WHERE p.scraped_at >= CURRENT_DATE - INTERVAL '7 days';

-- ============================================================================
-- MAINTENANCE PROCEDURES
-- ============================================================================

-- Function to drop old partitions (for data retention)

CREATE OR REPLACE FUNCTION drop_old_precios_partitions(retention_days INTEGER)
RETURNS VOID AS $$
DECLARE
    part RECORD;
    cutoff DATE := CURRENT_DATE - retention_days;
BEGIN
    FOR part IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
          AND tablename ~ '^precios_\d{4}_\d{2}_\d{2}$'
    LOOP
        -- Extract partition date
        PERFORM NULL;
        DECLARE pdate DATE;
        BEGIN
            pdate := to_date(substring(part.tablename FROM '\d{4}_\d{2}_\d{2}'),
                             'YYYY_MM_DD');

            IF pdate < cutoff THEN
                EXECUTE format('DROP TABLE IF EXISTS %I', part.tablename);
                RAISE NOTICE 'Dropped partition: %', part.tablename;
            END IF;
        END;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
