import pytest
import polars as pl
from sepa_pipeline.schema import (
    to_silver_precios,
    to_silver_sucursales,
    to_silver_productos,
    get_schema_dict,
    get_silver_schema_dict
)

class TestSchemaTransforms:
    def test_to_silver_precios(self):
        """Verify column renaming, EAN type casting, and dropping unspecified columns."""
        df_raw = pl.DataFrame({
            "id_comercio": ["1"],
            "id_bandera": ["1"],
            "id_sucursal": ["1"],
            "id_producto": ["P1"],
            "productos_ean": ["1"],
            "productos_descripcion": ["Prod A"],
            "productos_marca": ["Marca A"],
            "productos_precio_lista": ["100.5"],
            "extra_column": ["drop_me"]
        })
        
        df_silver = to_silver_precios(df_raw)
        
        assert "extra_column" not in df_silver.columns
        assert df_silver["ean"][0] is True
        assert df_silver["precio_lista"][0] == 100.5
        assert "descripcion" in df_silver.columns

    def test_to_silver_sucursales_provincia_normalization(self):
        """Verify 'Buenos Aires' is normalized to 'AR-B'."""
        df_raw = pl.DataFrame({
            "id_comercio": ["1"],
            "id_bandera": ["1"],
            "id_sucursal": ["1"],
            "sucursales_provincia": ["Buenos Aires"]
        })
        
        df_silver = to_silver_sucursales(df_raw)
        
        assert df_silver["provincia"][0] == "AR-B"

    def test_to_silver_productos_marca_null_fill(self):
        """Verify 'marca' null values are filled with 'S/D'."""
        df_raw = pl.DataFrame({
            "id_producto": ["P1", "P2"],
            "productos_ean": ["1", "0"],
            "productos_marca": ["Marca A", None],
            "productos_descripcion": ["Prod A", "Prod B"],
            "productos_cantidad_presentacion": ["1.0", "2.0"],
            "productos_unidad_medida_presentacion": ["kg", "kg"]
        })
        
        df_silver = to_silver_productos(df_raw)
        
        assert df_silver["marca"][0] == "Marca A"
        assert df_silver["marca"][1] == "S/D"

    def test_get_schema_dict_invalid(self):
        with pytest.raises(ValueError, match="Unknown table type"):
            get_schema_dict("invalid_table")
            
    def test_get_silver_schema_dict_invalid(self):
        with pytest.raises(ValueError, match="Unknown Silver table"):
            get_silver_schema_dict("invalid_table")
