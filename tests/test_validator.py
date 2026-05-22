import pytest
import polars as pl
from pathlib import Path
from sepa_pipeline.validator import SEPAValidator
from .factories import make_comercio_df, make_sucursales_df, make_productos_df

class TestSEPAValidator:

    def test_validate_comercio_soft_cast(self):
        validator = SEPAValidator()
        # id_bandera as string "1.0", comercio_cuit as string "20.0"
        df = pl.DataFrame({
            "id_comercio": ["1"],
            "id_bandera": ["1.0"],
            "comercio_cuit": ["20"],
            "comercio_razon_social": ["R"],
            "comercio_bandera_nombre": ["B"],
            "comercio_bandera_url": ["U"],
            "comercio_ultima_actualizacion": ["U"],
            "comercio_version_sepa": ["1"]
        })
        
        cleaned = validator.validate_comercio(df)
        assert cleaned["id_bandera"].dtype == pl.Int32
        assert cleaned["comercio_cuit"].dtype == pl.Int64

    def test_validate_productos_negative_prices_dropped(self):
        validator = SEPAValidator()
        df = pl.DataFrame({
            "id_comercio": ["1", "1"],
            "id_bandera": ["1", "1"],
            "id_sucursal": ["1", "1"],
            "id_producto": ["100", "200"],
            "productos_ean": ["1", "0"],
            "productos_descripcion": ["A", "B"],
            "productos_cantidad_presentacion": ["1", "1"],
            "productos_unidad_medida_presentacion": ["kg", "kg"],
            "productos_marca": ["M", "M"],
            "productos_precio_lista": ["100", "-50"], # Negative price
            "productos_precio_referencia": ["1", "1"],
            "productos_cantidad_referencia": ["1", "1"],
            "productos_unidad_medida_referencia": ["kg", "kg"],
            "productos_precio_unitario_promo1": ["", ""],
            "productos_leyenda_promo1": ["", ""],
            "productos_precio_unitario_promo2": ["", ""],
            "productos_leyenda_promo2": ["", ""]
        })
        
        cleaned = validator.validate_productos(df)
        assert cleaned.height == 1
        assert cleaned["id_producto"][0] == 100

    def test_validate_referential_integrity(self):
        validator = SEPAValidator()
        
        df_comercios = pl.DataFrame({
            "id_comercio": ["1"],
            "id_bandera": [1]
        })
        
        df_sucursales = pl.DataFrame({
            "id_comercio": ["1", "2"], # "2" is orphaned
            "id_bandera": [1, 1],
            "id_sucursal": [10, 20]
        })
        
        df_productos = pl.DataFrame({
            "id_comercio": ["1", "1"],
            "id_bandera": [1, 1],
            "id_sucursal": [10, 99], # "99" is orphaned
            "id_producto": [100, 200]
        })
        
        clean_sucursales, clean_productos = validator.validate_referential_integrity(
            df_comercios, df_sucursales, df_productos
        )
        
        # Orphaned sucursal should be dropped
        assert clean_sucursales.height == 1
        assert clean_sucursales["id_comercio"][0] == "1"
        
        # Orphaned producto should be dropped
        assert clean_productos.height == 1
        assert clean_productos["id_sucursal"][0] == 10
