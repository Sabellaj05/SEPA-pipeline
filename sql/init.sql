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
