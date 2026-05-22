import pytest
from pathlib import Path
from sepa_pipeline.extractor import SEPAExtractor
from .factories import make_sepa_zip

class TestSEPAExtractor:

    def test_extract_zip_success(self, tmp_path: Path):
        """Test extraction of a valid nested ZIP file."""
        # Create a valid outer zip with 1 nested zip
        zip_bytes = make_sepa_zip(n_nested_zips=1)
        zip_path = tmp_path / "sepa_precios_2026-01-15.zip"
        zip_path.write_bytes(zip_bytes)

        # The function `extract_zip` works on the NESTED zips normally, 
        # but in SEPAExtractor, `extract_zip` actually assumes the input zip CONTAINS the CSVs.
        # Wait, let me check extractor.py:
        # extract_zip expects the nested zip (with comercio.csv, sucursales.csv, productos.csv).
        
        # We need to build a single nested zip bytes:
        import zipfile, io
        from .factories import make_comercio_df, make_sucursales_df, make_productos_df, _write_csv_with_footer
        
        nested_buf = io.BytesIO()
        with zipfile.ZipFile(nested_buf, "w") as nested:
            nested.writestr("comercio.csv", _write_csv_with_footer(make_comercio_df(1), False, "2026-01-15"))
            s_csv = make_sucursales_df(1).write_csv()
            nested.writestr("sucursales.csv", s_csv.encode("utf-8") if isinstance(s_csv, str) else s_csv)
            p_csv = make_productos_df(10).write_csv()
            nested.writestr("productos.csv", p_csv.encode("utf-8") if isinstance(p_csv, str) else p_csv)
            
        nested_zip_path = tmp_path / "nested_comercio.zip"
        nested_zip_path.write_bytes(nested_buf.getvalue())
        
        extract_to = tmp_path / "extracted"
        csv_paths, date_status = SEPAExtractor.extract_zip(nested_zip_path, extract_to)
        
        assert "comercio" in csv_paths
        assert "sucursales" in csv_paths
        assert "productos" in csv_paths
        
        assert csv_paths["comercio"].exists()
        assert csv_paths["sucursales"].exists()
        assert csv_paths["productos"].exists()

    def test_extract_zip_missing_files(self, tmp_path: Path):
        """Test extraction fails when expected CSVs are missing."""
        import zipfile, io
        
        nested_buf = io.BytesIO()
        with zipfile.ZipFile(nested_buf, "w") as nested:
            nested.writestr("sucursales.csv", "id_sucursal\n1")
            
        nested_zip_path = tmp_path / "nested_missing.zip"
        nested_zip_path.write_bytes(nested_buf.getvalue())
        
        extract_to = tmp_path / "extracted"
        with pytest.raises(ValueError, match="missing files"):
            SEPAExtractor.extract_zip(nested_zip_path, extract_to)

    def test_extract_all_zips(self, tmp_path: Path):
        """Test extract_all_zips correctly processes valid and malformed zips."""
        import zipfile, io
        from datetime import date
        
        source_dir = tmp_path
        target_date = date(2026, 1, 15)
        # We need the zip files inside the date directory
        date_dir = source_dir / target_date.isoformat()
        date_dir.mkdir()
        
        zip1_path = date_dir / "sepa_1.zip"
        zip2_path = date_dir / "sepa_2.zip"
        zip3_path = date_dir / "sepa_3.zip"  # malformed
        
        # Valid nested zip
        nested_buf = io.BytesIO()
        with zipfile.ZipFile(nested_buf, "w") as nested:
            from .factories import make_comercio_df, make_sucursales_df, make_productos_df, _write_csv_with_footer
            nested.writestr("comercio.csv", _write_csv_with_footer(make_comercio_df(1), False, "2026-01-15"))
            s_csv = make_sucursales_df(1).write_csv()
            nested.writestr("sucursales.csv", s_csv.encode("utf-8") if isinstance(s_csv, str) else s_csv)
            p_csv = make_productos_df(10).write_csv()
            nested.writestr("productos.csv", p_csv.encode("utf-8") if isinstance(p_csv, str) else p_csv)
            
        zip1_path.write_bytes(nested_buf.getvalue())
        zip2_path.write_bytes(nested_buf.getvalue())
        zip3_path.write_bytes(b"PK\x05\x06not a real zip")
        
        # Test extraction
        all_csv_paths, malformed_count, stale_count, unknown_count = SEPAExtractor.extract_all_zips(source_dir, target_date)
        
        assert len(all_csv_paths) == 2
        assert malformed_count == 1
        assert "comercio" in all_csv_paths[0]
        assert malformed_count == 1
