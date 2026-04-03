"""
SEPA Data Model — Bronze and Silver Schema Definitions

This module owns the schema contract for all layers of the SEPA pipeline:

  Bronze (Raw CSV)
    COMERCIO_SCHEMA, SUCURSALES_SCHEMA, PRODUCTOS_SCHEMA
    All columns are Utf8; passed to pl.read_csv(schema_overrides=...).

  Silver (Iceberg)
    SILVER_PRECIOS_SCHEMA        Fact table, partitioned Day(fecha_vigencia)
    SILVER_DIM_SUCURSALES_SCHEMA Unpartitioned daily snapshot
    SILVER_DIM_COMERCIOS_SCHEMA  Unpartitioned daily snapshot
    SILVER_DIM_PRODUCTOS_SCHEMA  Unpartitioned daily snapshot

    Column names are clean (no productos_/sucursales_/comercio_ prefixes).
    Types are applied at pipeline load time — dbt staging models do SELECT *.

  Transform utilities
    to_silver_*()  Convert a validated raw DataFrame to Silver-ready shape.
                   fecha_vigencia / scraped_at are NOT added here; the pipeline
                   injects them at load time.
"""

from typing import Dict

import polars as pl

# =============================================================================
# Bronze — Raw CSV Schemas
# =============================================================================

COMERCIO_SCHEMA: Dict[str, type[pl.DataType]] = {
    "id_comercio":               pl.Utf8,
    "id_bandera":                pl.Utf8,
    "comercio_cuit":             pl.Utf8,
    "comercio_razon_social":     pl.Utf8,
    "comercio_bandera_nombre":   pl.Utf8,
    "comercio_bandera_url":      pl.Utf8,
    "comercio_ultima_actualizacion": pl.Utf8,
    "comercio_version_sepa":     pl.Utf8,
}

SUCURSALES_SCHEMA: Dict[str, type[pl.DataType]] = {
    "id_comercio":                        pl.Utf8,
    "id_bandera":                         pl.Utf8,
    "id_sucursal":                        pl.Utf8,
    "sucursales_nombre":                  pl.Utf8,
    "sucursales_tipo":                    pl.Utf8,
    "sucursales_calle":                   pl.Utf8,
    "sucursales_numero":                  pl.Utf8,
    "sucursales_latitud":                 pl.Utf8,
    "sucursales_longitud":                pl.Utf8,
    "sucursales_observaciones":           pl.Utf8,
    "sucursales_barrio":                  pl.Utf8,
    "sucursales_codigo_postal":           pl.Utf8,
    "sucursales_localidad":               pl.Utf8,
    "sucursales_provincia":               pl.Utf8,
    "sucursales_lunes_horario_atencion":  pl.Utf8,
    "sucursales_martes_horario_atencion": pl.Utf8,
    "sucursales_miercoles_horario_atencion": pl.Utf8,
    "sucursales_jueves_horario_atencion": pl.Utf8,
    "sucursales_viernes_horario_atencion": pl.Utf8,
    "sucursales_sabado_horario_atencion": pl.Utf8,
    "sucursales_domingo_horario_atencion": pl.Utf8,
}

PRODUCTOS_SCHEMA: Dict[str, type[pl.DataType]] = {
    "id_comercio":                         pl.Utf8,
    "id_bandera":                          pl.Utf8,
    "id_sucursal":                         pl.Utf8,
    "id_producto":                         pl.Utf8,
    "productos_ean":                       pl.Utf8,
    "productos_descripcion":               pl.Utf8,
    "productos_cantidad_presentacion":     pl.Utf8,
    "productos_unidad_medida_presentacion": pl.Utf8,
    "productos_marca":                     pl.Utf8,
    "productos_precio_lista":              pl.Utf8,
    "productos_precio_referencia":         pl.Utf8,
    "productos_cantidad_referencia":       pl.Utf8,
    "productos_unidad_medida_referencia":  pl.Utf8,
    "productos_precio_unitario_promo1":    pl.Utf8,
    "productos_leyenda_promo1":            pl.Utf8,
    "productos_precio_unitario_promo2":    pl.Utf8,
    "productos_leyenda_promo2":            pl.Utf8,
}


def get_schema_dict(table_type: str) -> Dict[str, type[pl.DataType]]:
    """Return the raw CSV schema for 'comercio', 'sucursales', or 'productos'."""
    schemas: Dict[str, Dict] = {
        "comercio":   COMERCIO_SCHEMA,
        "sucursales": SUCURSALES_SCHEMA,
        "productos":  PRODUCTOS_SCHEMA,
    }
    if table_type not in schemas:
        raise ValueError(
            f"Unknown table type: '{table_type}'. Expected one of {list(schemas.keys())}"
        )
    return schemas[table_type]


# =============================================================================
# Silver — Iceberg Table Schemas
# =============================================================================
#
# Fact: silver.precios           Partitioned Day(fecha_vigencia), ~15M rows/day
# Dims: silver.dim_*             Unpartitioned daily snapshots (append-only)
#
# Design notes:
# - precios carries descripcion + marca from the source row to avoid a dim join
#   on every gold query (each gold aggregation would otherwise need to join ~90K
#   unique products just to get those two string columns).
# - Horario columns (7) are excluded from dim_sucursales: operational, never queried.
# - comercio_ultima_actualizacion / comercio_version_sepa excluded from
#   dim_comercios: operational metadata.

SILVER_PRECIOS_SCHEMA: Dict[str, type[pl.DataType]] = {
    # --- Grain keys ---
    "id_comercio":                    pl.Utf8,
    "id_bandera":                     pl.Utf8,
    "id_sucursal":                    pl.Utf8,
    "id_producto":                    pl.Utf8,
    # --- Product attributes (denormalized onto fact) ---
    "ean":                            pl.Boolean,   # True = standard EAN/UPC-A; False = internal code
    "descripcion":                    pl.Utf8,
    "marca":                          pl.Utf8,
    "cantidad_presentacion":          pl.Float64,
    "unidad_medida_presentacion":     pl.Utf8,
    # --- Price metrics ---
    "precio_lista":                   pl.Float64,
    "precio_referencia":              pl.Float64,
    "cantidad_referencia":            pl.Float64,
    "unidad_medida_referencia":       pl.Utf8,
    # --- Promotions (optional in source) ---
    "precio_promo1":                  pl.Float64,   # nullable
    "leyenda_promo1":                 pl.Utf8,      # nullable
    "precio_promo2":                  pl.Float64,   # nullable
    "leyenda_promo2":                 pl.Utf8,      # nullable
    # --- Partition / audit ---
    "fecha_vigencia":                 pl.Date,      # Iceberg partition key
    "scraped_at":                     pl.Datetime,  # naive, pipeline ingest time
}

SILVER_DIM_SUCURSALES_SCHEMA: Dict[str, type[pl.DataType]] = {
    "id_comercio":    pl.Utf8,
    "id_bandera":     pl.Utf8,
    "id_sucursal":    pl.Utf8,
    "nombre":         pl.Utf8,
    "tipo":           pl.Utf8,     # Hipermercado / Supermercado / Autoservicio / Web
    "calle":          pl.Utf8,
    "numero":         pl.Utf8,
    "latitud":        pl.Float64,  # nullable — web stores have no coordinates
    "longitud":       pl.Float64,  # nullable
    "codigo_postal":  pl.Utf8,
    "localidad":      pl.Utf8,
    "provincia":      pl.Utf8,     # ISO 3166-2 (AR-B, AR-C, …) — primary analytics join key
    "barrio":         pl.Utf8,     # nullable
    "fecha_vigencia": pl.Date,
}

SILVER_DIM_COMERCIOS_SCHEMA: Dict[str, type[pl.DataType]] = {
    "id_comercio":    pl.Utf8,
    "id_bandera":     pl.Utf8,
    "cuit":           pl.Utf8,
    "razon_social":   pl.Utf8,
    "bandera_nombre": pl.Utf8,
    "bandera_url":    pl.Utf8,
    "fecha_vigencia": pl.Date,
}

SILVER_DIM_PRODUCTOS_SCHEMA: Dict[str, type[pl.DataType]] = {
    "id_producto":                pl.Utf8,
    "ean":                        pl.Boolean,
    "descripcion":                pl.Utf8,
    "marca":                      pl.Utf8,
    "cantidad_presentacion":      pl.Float64,
    "unidad_medida_presentacion": pl.Utf8,
    "fecha_vigencia":             pl.Date,
}


def get_silver_schema_dict(table: str) -> Dict[str, type[pl.DataType]]:
    """Return the Silver Iceberg schema for 'precios', 'dim_sucursales', 'dim_comercios', or 'dim_productos'."""
    schemas = {
        "precios":        SILVER_PRECIOS_SCHEMA,
        "dim_sucursales": SILVER_DIM_SUCURSALES_SCHEMA,
        "dim_comercios":  SILVER_DIM_COMERCIOS_SCHEMA,
        "dim_productos":  SILVER_DIM_PRODUCTOS_SCHEMA,
    }
    if table not in schemas:
        raise ValueError(f"Unknown Silver table: '{table}'. Expected one of {list(schemas.keys())}")
    return schemas[table]


# =============================================================================
# Raw → Silver column rename maps
# =============================================================================
# Keys: raw CSV column name. Values: Silver column name.
# id_* keys are identical in both layers and do not appear here.
# Columns absent from the map are dropped at transform time.

PRECIOS_RAW_TO_SILVER: Dict[str, str] = {
    "productos_ean":                        "ean",
    "productos_descripcion":                "descripcion",
    "productos_marca":                      "marca",
    "productos_cantidad_presentacion":      "cantidad_presentacion",
    "productos_unidad_medida_presentacion": "unidad_medida_presentacion",
    "productos_precio_lista":               "precio_lista",
    "productos_precio_referencia":          "precio_referencia",
    "productos_cantidad_referencia":        "cantidad_referencia",
    "productos_unidad_medida_referencia":   "unidad_medida_referencia",
    "productos_precio_unitario_promo1":     "precio_promo1",
    "productos_leyenda_promo1":             "leyenda_promo1",
    "productos_precio_unitario_promo2":     "precio_promo2",
    "productos_leyenda_promo2":             "leyenda_promo2",
}

SUCURSALES_RAW_TO_SILVER: Dict[str, str] = {
    "sucursales_nombre":        "nombre",
    "sucursales_tipo":          "tipo",
    "sucursales_calle":         "calle",
    "sucursales_numero":        "numero",
    "sucursales_latitud":       "latitud",
    "sucursales_longitud":      "longitud",
    "sucursales_codigo_postal": "codigo_postal",
    "sucursales_localidad":     "localidad",
    "sucursales_provincia":     "provincia",
    "sucursales_barrio":        "barrio",
    # sucursales_observaciones: excluded (duplicates bandera_url for web stores)
    # sucursales_*_horario_atencion (7 cols): excluded — operational, not analytics
}

COMERCIOS_RAW_TO_SILVER: Dict[str, str] = {
    "comercio_cuit":           "cuit",
    "comercio_razon_social":   "razon_social",
    "comercio_bandera_nombre": "bandera_nombre",
    "comercio_bandera_url":    "bandera_url",
    # comercio_ultima_actualizacion, comercio_version_sepa: excluded
}


# =============================================================================
# Silver Transform Utilities
# =============================================================================

def _project_to_silver(
    df: pl.DataFrame,
    schema: Dict[str, type[pl.DataType]],
    exclude: tuple[str, ...] = (),
) -> pl.DataFrame:
    """
    Project df to schema columns, in schema order.
    - Columns missing from df are added as typed nulls.
    - Null-typed columns (all-null from read_csv optional fields) are cast to schema type.
    - Columns not in schema are dropped.
    """
    target_cols = [c for c in schema if c not in exclude]
    fix_exprs = []
    for col in target_cols:
        if col not in df.columns:
            fix_exprs.append(pl.lit(None).cast(schema[col]).alias(col))
        elif df.schema[col] == pl.Null:
            fix_exprs.append(pl.col(col).cast(schema[col]).alias(col))
    if fix_exprs:
        df = df.with_columns(fix_exprs)
    return df.select(target_cols)


def to_silver_precios(df: pl.DataFrame) -> pl.DataFrame:
    """
    Transform a validated raw productos DataFrame to the Silver precios schema.
    fecha_vigencia and scraped_at are excluded — the pipeline adds them at load time.

    Normalizations applied:
    - marca null → "S/D"  (standard placeholder in SEPA source for unknown brand)
    """
    df = df.rename({k: v for k, v in PRECIOS_RAW_TO_SILVER.items() if k in df.columns})

    cast_exprs = []
    if "ean" in df.columns:
        cast_exprs.append((pl.col("ean").cast(pl.Utf8).str.strip_chars() == "1").alias("ean"))
    for col in ("cantidad_presentacion", "precio_lista", "precio_referencia",
                "cantidad_referencia", "precio_promo1", "precio_promo2"):
        if col in df.columns:
            cast_exprs.append(pl.col(col).cast(pl.Float64, strict=False))
    if "marca" in df.columns:
        cast_exprs.append(pl.col("marca").fill_null("S/D").alias("marca"))
    if cast_exprs:
        df = df.with_columns(cast_exprs)

    return _project_to_silver(df, SILVER_PRECIOS_SCHEMA, exclude=("fecha_vigencia", "scraped_at"))


def to_silver_sucursales(df: pl.DataFrame) -> pl.DataFrame:
    """
    Transform a validated raw sucursales DataFrame to the Silver dim_sucursales schema.
    fecha_vigencia is excluded — the pipeline adds it at load time.

    Normalizations applied:
    - provincia "Buenos Aires" → "AR-B"  (some stores file full name instead of ISO 3166-2)
    """
    df = df.rename({k: v for k, v in SUCURSALES_RAW_TO_SILVER.items() if k in df.columns})

    cast_exprs = []
    for col in ("latitud", "longitud"):
        if col in df.columns:
            cast_exprs.append(pl.col(col).cast(pl.Float64, strict=False))
    if "provincia" in df.columns:
        cast_exprs.append(
            pl.col("provincia").str.replace_all("^Buenos Aires$", "AR-B").alias("provincia")
        )
    if cast_exprs:
        df = df.with_columns(cast_exprs)

    return _project_to_silver(df, SILVER_DIM_SUCURSALES_SCHEMA, exclude=("fecha_vigencia",))


def to_silver_comercios(df: pl.DataFrame) -> pl.DataFrame:
    """
    Transform a validated raw comercio DataFrame to the Silver dim_comercios schema.
    fecha_vigencia is excluded — the pipeline adds it at load time.
    """
    df = df.rename({k: v for k, v in COMERCIOS_RAW_TO_SILVER.items() if k in df.columns})
    return _project_to_silver(df, SILVER_DIM_COMERCIOS_SCHEMA, exclude=("fecha_vigencia",))


def to_silver_productos(df: pl.DataFrame) -> pl.DataFrame:
    """
    Transform a validated raw productos DataFrame to the Silver dim_productos schema.
    Expects df already deduplicated on id_producto (pipeline responsibility).
    fecha_vigencia is excluded — the pipeline adds it at load time.

    Normalizations applied:
    - marca null → "S/D"  (standard placeholder in SEPA source for unknown brand)
    """
    df = df.rename({k: v for k, v in PRECIOS_RAW_TO_SILVER.items() if k in df.columns})

    cast_exprs = []
    if "ean" in df.columns:
        cast_exprs.append((pl.col("ean").cast(pl.Utf8).str.strip_chars() == "1").alias("ean"))
    if "cantidad_presentacion" in df.columns:
        cast_exprs.append(pl.col("cantidad_presentacion").cast(pl.Float64, strict=False))
    if "marca" in df.columns:
        cast_exprs.append(pl.col("marca").fill_null("S/D").alias("marca"))
    if cast_exprs:
        df = df.with_columns(cast_exprs)

    return _project_to_silver(df, SILVER_DIM_PRODUCTOS_SCHEMA, exclude=("fecha_vigencia",))
