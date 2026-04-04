"""
SEPA Data Validator

Owns all CSV ingestion and data cleaning for the three SEPA source files.
Pipeline.py delegates here for both reading and validation so that the
orchestrator stays free of data-wrangling details.

Public surface:
    SEPAValidator.load_dimensions(all_csv_paths)           → (df_comercios, df_sucursales)
    SEPAValidator.load_productos_chunk(csv_paths, ...)     → df_producto
    SEPAValidator.validate_comercio(df)                    → df
    SEPAValidator.validate_sucursales(df)                  → df
    SEPAValidator.validate_productos(df)                   → df
    SEPAValidator.validate_referential_integrity(...)      → (df_sucursales, df_productos)
"""

from typing import Tuple

import polars as pl

from sepa_pipeline.schema import get_schema_dict
from sepa_pipeline.utils.logger import get_logger

logger = get_logger(__name__)


class SEPAValidator:
    """Validates SEPA data integrity (robust, non-destructive)."""

    # Fixed: Updated to match actual data (no accent on Ultima)
    # We will use a substring check in _drop_footer_rows
    # to handle both "Ultima" and "Última"
    # Expanded sucursales types
    # (realistic list + common variants) - normalized to lowercase
    VALID_SUCURSALES_TYPES = {
        "hipermercado",
        "supermercado",
        "autoservicio",
        "tradicional",
        "web",
        "mini",
        "express",
        "super",
        "mayorista",
        "bazar",
        "hiper",
        "hipermercado local",
        "autoservicio exprés",
        "autoservicio expres",
        "tienda virtual",
        "tienda fisica",
    }

    @staticmethod
    def _read_csv(path: str, schema: dict) -> pl.DataFrame:
        """
        Read a SEPA pipe-delimited CSV and strip BOM / whitespace from column names.
        All three SEPA files share the same encoding and delimiter rules.
        """
        df = pl.read_csv(
            path,
            separator="|",
            encoding="utf8-lossy",
            has_header=True,
            null_values=["", "NULL", "null"],
            schema_overrides=schema,
            truncate_ragged_lines=True,
            ignore_errors=True,
            quote_char=None,
        )
        return df.rename({col: col.lstrip("\ufeff").strip() for col in df.columns})

    def load_dimensions(
        self,
        all_csv_paths: list[dict],
    ) -> Tuple[pl.DataFrame, pl.DataFrame]:
        """
        Read, validate, deduplicate, and referential-integrity-check all
        comercio and sucursales CSVs across every store ZIP for one pipeline run.

        Returns (df_comercios, df_sucursales) in raw Bronze schema.
        A store whose files fail to read is skipped with a warning — the rest proceed.
        """
        comercio_schema = get_schema_dict("comercio")
        sucursales_schema = get_schema_dict("sucursales")
        productos_schema = get_schema_dict("productos")

        all_comercios: list[pl.DataFrame] = []
        all_sucursales: list[pl.DataFrame] = []

        for idx, csv_paths in enumerate(all_csv_paths):
            logger.info(f"Dimensions scan: processing file {idx + 1}/{len(all_csv_paths)}")

            try:
                df_comercio = self._read_csv(csv_paths["comercio"], comercio_schema)
                df_comercio = self.validate_comercio(df_comercio)
                if df_comercio.height > 0:
                    all_comercios.append(df_comercio)
            except Exception as e:
                logger.warning(f"Failed to read/validate comercio {csv_paths['comercio']}: {e}")

            try:
                df_sucursal = self._read_csv(csv_paths["sucursales"], sucursales_schema)
                df_sucursal = self.validate_sucursales(df_sucursal)
                if df_sucursal.height > 0:
                    all_sucursales.append(df_sucursal)
            except Exception as e:
                logger.warning(f"Failed to read/validate sucursales {csv_paths['sucursales']}: {e}")

        logger.info("Concatenating dimensions...")
        df_comercios = (
            pl.concat(all_comercios).unique()
            if all_comercios
            else pl.DataFrame(schema=comercio_schema)
        )
        df_sucursales = (
            pl.concat(all_sucursales).unique()
            if all_sucursales
            else pl.DataFrame(schema=sucursales_schema)
        )

        # Final validation pass after concat
        df_comercios = self.validate_comercio(df_comercios)
        df_sucursales = self.validate_sucursales(df_sucursales)

        # Enforce referential integrity between the two dimensions
        empty_products = pl.DataFrame(schema=productos_schema)
        df_sucursales, _ = self.validate_referential_integrity(
            df_comercios, df_sucursales, empty_products
        )

        return df_comercios, df_sucursales

    def load_productos_chunk(
        self,
        csv_paths: dict,
        df_comercios: pl.DataFrame,
        df_sucursales: pl.DataFrame,
    ) -> pl.DataFrame:
        """
        Read, validate, and referential-integrity-check one store's productos.csv.
        Returns an empty DataFrame (productos schema) on any read or validation failure
        so the pipeline loop can continue without crashing.
        """
        productos_schema = get_schema_dict("productos")
        try:
            df = self._read_csv(csv_paths["productos"], productos_schema)
            df = self.validate_productos(df)
            _, df = self.validate_referential_integrity(df_comercios, df_sucursales, df)
            return df
        except Exception as e:
            logger.warning(f"Failed to read/validate productos {csv_paths['productos']}: {e}")
            return pl.DataFrame(schema=productos_schema)

    @staticmethod
    def _drop_footer_rows(df: pl.DataFrame, first_column: str) -> pl.DataFrame:
        """
        Drop rows that are the footer like 'Ultima actualización: ...'
        Works even if the CSV has only that row.
        """
        if df.height == 0:
            return df
        # If first column contains footer text, filter it out
        # Check for "ltima actualizaci"
        # to handle both "Ultima" and "Última" and case variations
        mask_footer = pl.col(first_column).is_not_null() & pl.col(
            first_column
        ).str.to_lowercase().str.contains("ltima actualizaci")
        # If first column doesn't exist or not string,
        # the expression will be fine because we always cast in callers.
        filtered = df.filter(~mask_footer)
        if filtered.height == 0:
            # keep empty DataFrame with same schema
            return pl.DataFrame(schema={c: df.schema[c] for c in df.columns})
        return filtered

    @staticmethod
    def validate_comercio(df: pl.DataFrame) -> pl.DataFrame:
        """Validate comercio.csv schema and data (soft validation)."""
        required_cols = list(get_schema_dict("comercio").keys())

        missing = set(required_cols) - set(df.columns)
        if missing:
            raise ValueError(f"comercio.csv missing columns: {missing}")

        # Drop footer-like rows if present
        df = SEPAValidator._drop_footer_rows(df, "id_comercio")

        if df.height == 0:
            logger.warning("comercio.csv contains no data rows after footer removal")
            return pl.DataFrame(schema={c: df.schema[c] for c in df.columns})

        # Unexpected null bytes
        df = df.with_columns([pl.col(pl.Utf8).str.replace("\x00", "")])

        # Trim strings and coerce types softly
        df = df.with_columns(
            [
                pl.col("id_comercio")
                .cast(pl.Utf8)
                .str.strip_chars()
                .str.replace(r"\.0+$", "")  # convert things like "1.0" -> "1"
                .str.replace(r"\.00+$", "")  # extra safety for weird decimals
                .alias("id_comercio"),
                # Some files use floats like 1.0, cast float->int leniently
                pl.col("id_bandera")
                .cast(pl.Float64, strict=False)
                .cast(pl.Int32, strict=False)
                .alias("id_bandera"),
                pl.col("comercio_cuit")
                .cast(pl.Float64, strict=False)
                .cast(pl.Int64, strict=False)
                .alias("comercio_cuit"),
                pl.col("comercio_version_sepa")
                .cast(pl.Float32, strict=False)
                .alias("comercio_version_sepa"),
            ],
            allow_rechunk=False,
        )

        # Keep rows even if some fields are null, but log how many lost required keys
        before = df.height
        df = df.filter(
            (pl.col("id_comercio").is_not_null())
            & (pl.col("id_comercio").str.len_bytes() > 0)
            & (pl.col("id_bandera").is_not_null())
        )
        after = df.height
        if after < before:
            logger.warning(
                f"validate_comercio: dropped {before - after}"
                " rows missing id_comercio after soft-cast"
            )

        return df

    @staticmethod
    def validate_sucursales(df: pl.DataFrame) -> pl.DataFrame:
        """Validate sucursales.csv schema and data (soft validation)."""
        required_cols = list(get_schema_dict("sucursales").keys())

        missing = set(required_cols) - set(df.columns)
        if missing:
            raise ValueError(f"sucursales.csv missing columns: {missing}")

        # Drop footer-like rows
        df = SEPAValidator._drop_footer_rows(df, "id_comercio")

        if df.height == 0:
            logger.warning("sucursales.csv contains no data rows after footer removal")
            return pl.DataFrame(schema={c: df.schema[c] for c in df.columns})

        # Soft cast ID columns (float -> int if possible), keep other columns as strings
        df = df.with_columns(
            [
                pl.col("id_comercio")
                .cast(pl.Utf8)
                .str.strip_chars()
                .alias("id_comercio"),
                pl.col("id_bandera")
                .cast(pl.Float64, strict=False)
                .cast(pl.Int32, strict=False)
                .alias("id_bandera"),
                pl.col("id_sucursal")
                .cast(pl.Float64, strict=False)
                .cast(pl.Int32, strict=False)
                .alias("id_sucursal"),
                pl.col("sucursales_codigo_postal")
                .cast(pl.Float64, strict=False)
                .cast(pl.Int32, strict=False)
                .alias("sucursales_codigo_postal"),
            ],
            allow_rechunk=False,
        )

        # Log counts before/after minimal required fields filtering
        before = df.height
        df = df.filter(
            pl.col("id_comercio").is_not_null()
            & pl.col("id_bandera").is_not_null()
            & pl.col("id_sucursal").is_not_null()
            # DB requires these to be not null
            & pl.col("sucursales_localidad").is_not_null()
            & pl.col("sucursales_provincia").is_not_null()
        )
        after = df.height
        if after < before:
            logger.warning(
                f"validate_sucursales: dropped {before - after}"
                " rows missing required fields (id keys or location) after soft-cast"
            )

        # Validate sucursales_tipo but do not drop
        # rows instead log unknowns and keep them
        if "sucursales_tipo" in df.columns:
            # normalize type strings (strip and lowercase)
            df = df.with_columns(
                pl.col("sucursales_tipo")
                .cast(pl.Utf8)
                .str.strip_chars()
                .str.to_lowercase()
                .alias("sucursales_tipo")
            )
            unknown_mask = (
                ~pl.col("sucursales_tipo").is_in(
                    list(SEPAValidator.VALID_SUCURSALES_TYPES)
                )
                & pl.col("sucursales_tipo").is_not_null()
            )
            unknown_count = df.filter(unknown_mask).height
            if unknown_count > 0:
                # sample up to 10 unknown types to log
                samples = (
                    df.filter(unknown_mask)
                    .select("sucursales_tipo")
                    .unique()
                    .limit(10)
                    .to_series()
                    .to_list()
                )
                logger.warning(
                    f"validate_sucursales: {unknown_count}"
                    f"rows with unknown sucursales_tipo (samples: {samples})"
                    " — keeping rows but logging"
                )
        else:
            logger.debug("validate_sucursales: sucursales_tipo not present")

        return df

    @staticmethod
    def validate_productos(df: pl.DataFrame) -> pl.DataFrame:
        """Validate productos.csv schema and data (soft validation)."""
        required_cols = list(get_schema_dict("productos").keys())

        missing = set(required_cols) - set(df.columns)
        if missing:
            raise ValueError(f"productos.csv missing columns: {missing}")

        # Drop footer-like rows using id_producto
        # (some files place the footer in various columns)
        df = SEPAValidator._drop_footer_rows(df, "id_producto")

        if df.height == 0:
            logger.warning("productos.csv contains no data rows after footer removal")
            return pl.DataFrame(schema={c: df.schema[c] for c in df.columns})

        # Soft-cast ids: polars sometimes reads these as float;
        # coerce float -> int where possible
        df = df.with_columns(
            [
                pl.col("id_comercio")
                .cast(pl.Utf8)
                .str.strip_chars()
                .alias("id_comercio"),
                pl.col("id_bandera")
                .cast(pl.Float64, strict=False)
                .cast(pl.Int32, strict=False)
                .alias("id_bandera"),
                pl.col("id_sucursal")
                .cast(pl.Float64, strict=False)
                .cast(pl.Int32, strict=False)
                .alias("id_sucursal"),
                pl.col("id_producto")
                .cast(pl.Float64, strict=False)
                .cast(pl.Int64, strict=False)
                .alias("id_producto"),
                # productos_ean may be '1','0','True','False' etc
                pl.when(pl.col("productos_ean").is_in(["1", "True", "true"]))
                .then(pl.lit(True))
                .when(pl.col("productos_ean").is_in(["0", "False", "false", ""]))
                .then(pl.lit(False))
                .otherwise(pl.lit(None))
                .alias("productos_ean"),
                pl.col("productos_precio_lista")
                .cast(pl.Float64, strict=False)
                .alias("productos_precio_lista"),
            ],
            allow_rechunk=False,
        )

        # Ensure the minimal ids exist; if they do not,
        # keep rows but mark where missing (we keep them)
        before = df.height
        df = df.filter(
            pl.col("id_comercio").is_not_null()
            & pl.col("id_bandera").is_not_null()
            & pl.col("id_sucursal").is_not_null()
            & pl.col("id_producto").is_not_null()
            & pl.col("productos_precio_lista").is_not_null()
            # DB requires these to be not null for master table
            # We enforce strict validation here to prevent downstream database errors
            & pl.col("productos_descripcion").is_not_null()
        )
        after = df.height
        if after < before:
            logger.warning(
                f"validate_productos: filtered out {before - after}"
                " rows missing essential fields after soft-cast"
            )

        # Filter non-positive prices but keep rows with price <= 0 only
        #  as logs (they will be excluded from precio load)
        negatives = df.filter(pl.col("productos_precio_lista") <= 0)
        neg_count = negatives.height
        if neg_count > 0:
            logger.warning(
                f"validate_productos: {neg_count} rows"
                " with non-positive price will be filtered for precios load"
                " (kept in products DF for audit)"
            )

        return df

    @staticmethod
    def validate_referential_integrity(
        df_comercios: pl.DataFrame,
        df_sucursales: pl.DataFrame,
        df_productos: pl.DataFrame,
    ) -> Tuple[pl.DataFrame, pl.DataFrame]:
        """
        Non-destructive referential checks:
        - report orphan counts
        - do NOT drop products if sucursales is empty; instead keep them for insertion
        - return the (possibly cleaned) sucursales and productos dataframes
        """
        logger.info("Validating referential integrity (non-destructive)")

        # commerce keys
        comercio_keys = df_comercios.select(["id_comercio", "id_bandera"]).unique()
        sucursales_keys = (
            df_sucursales.select(["id_comercio", "id_bandera"]).unique()
            if df_sucursales.height > 0
            else pl.DataFrame(schema={"id_comercio": pl.Utf8, "id_bandera": pl.Int32})
        )

        # orphaned sucursales (present in sucursales but not in comercios)
        if sucursales_keys.height > 0 and comercio_keys.height > 0:
            orphaned_sucursales = sucursales_keys.join(
                comercio_keys, on=["id_comercio", "id_bandera"], how="anti"
            )
            orphaned_count = orphaned_sucursales.height
            if orphaned_count > 0:
                logger.warning(
                    f"Found {orphaned_count} sucursales referencing missing comercios"
                    " Dropping them to enforce integrity."
                )
                # Filter out orphaned sucursales
                df_sucursales = df_sucursales.join(
                    comercio_keys, on=["id_comercio", "id_bandera"], how="semi"
                )
        else:
            logger.debug("No sucursales/comercios keys to compare (one side empty)")

        # Check productos -> sucursales (using the potentially filtered df_sucursales)
        sucursal_full_keys = (
            df_sucursales.select(["id_comercio", "id_bandera", "id_sucursal"]).unique()
            if df_sucursales.height > 0
            else pl.DataFrame(
                schema={
                    "id_comercio": pl.Utf8,
                    "id_bandera": pl.Int32,
                    "id_sucursal": pl.Int32,
                }
            )
        )
        productos_sucursal_keys = (
            df_productos.select(["id_comercio", "id_bandera", "id_sucursal"]).unique()
            if df_productos.height > 0
            else pl.DataFrame(
                schema={
                    "id_comercio": pl.Utf8,
                    "id_bandera": pl.Int32,
                    "id_sucursal": pl.Int32,
                }
            )
        )

        if sucursal_full_keys.height > 0 and productos_sucursal_keys.height > 0:
            orphaned_productos = productos_sucursal_keys.join(
                sucursal_full_keys,
                on=["id_comercio", "id_bandera", "id_sucursal"],
                how="anti",
            )
            orphaned_prod_count = orphaned_productos.height
            if orphaned_prod_count > 0:
                logger.warning(
                    f"Found {orphaned_prod_count} productos"
                    " referencing missing sucursales"
                    " dropping them to enforce integrity."
                )
                # Filter out orphaned productos
                df_productos = df_productos.join(
                    sucursal_full_keys,
                    on=["id_comercio", "id_bandera", "id_sucursal"],
                    how="semi",
                )
        else:
            logger.debug("No products/sucursal keys to compare (one side empty)")

        logger.info("✅ Referential integrity validation completed (strict)")
        return df_sucursales, df_productos
