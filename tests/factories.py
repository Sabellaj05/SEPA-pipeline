import io
import zipfile
import polars as pl
from datetime import datetime

def make_productos_df(n_rows: int = 100) -> pl.DataFrame:
    return pl.DataFrame({
        "id_producto": [str(i) for i in range(n_rows)],
        "descripcion": [f"Producto {i}" for i in range(n_rows)],
        "marca": [f"Marca {i % 10}" for i in range(n_rows)],
        "precio_lista": [100.50 + i for i in range(n_rows)],
        "id_comercio": ["1" for _ in range(n_rows)],
        "id_bandera": ["1" for _ in range(n_rows)],
        "id_sucursal": ["1" for _ in range(n_rows)]
    })

def make_sucursales_df(n_rows: int = 1) -> pl.DataFrame:
    return pl.DataFrame({
        "id_sucursal": [str(i+1) for i in range(n_rows)],
        "id_comercio": [str(i+1) for i in range(n_rows)],
        "id_bandera": [str(i+1) for i in range(n_rows)],
        "sucursal_nombre": [f"Sucursal {i}" for i in range(n_rows)],
        "sucursal_tipo": ["Supermercado" for _ in range(n_rows)],
        "provincia": ["Buenos Aires" for _ in range(n_rows)],
        "direccion": [f"Calle {i}" for i in range(n_rows)]
    })

def make_comercio_df(n_rows: int = 1) -> pl.DataFrame:
    return pl.DataFrame({
        "id_comercio": [str(i+1) for i in range(n_rows)],
        "id_bandera": [str(i+1) for i in range(n_rows)],
        "comercio_nombre": [f"Comercio {i}" for i in range(n_rows)],
        "comercio_razon_social": [f"Razon {i}" for i in range(n_rows)],
        "comercio_cuit": [f"20-12345678-{i}" for i in range(n_rows)]
    })

def _write_csv_with_footer(df: pl.DataFrame, is_stale: bool, target_date: str) -> bytes:
    # Build CSV content
    csv_str = df.write_csv()
    csv_bytes = csv_str.encode("utf-8") if isinstance(csv_str, str) else csv_str


    # Add SEPA footer
    date_to_write = target_date
    if is_stale:
        # Subtract 2 days to make it stale
        dt = datetime.strptime(target_date, "%Y-%m-%d")
        import datetime as dt_module
        stale_dt = dt - dt_module.timedelta(days=2)
        date_to_write = stale_dt.strftime("%Y-%m-%d")

    footer = f"\nUltima actualizacion: {date_to_write}".encode("utf-8")
    return csv_bytes + footer

def make_sepa_zip(
    target_date: str = "2026-01-15",
    n_nested_zips: int = 1,
    n_productos: int = 10,
    corrupt_nested: bool = False,
    stale_majority: bool = False,
    missing_csv: bool = False
) -> bytes:
    """
    Builds a realistic outer ZIP mimicking SEPA's nested structure.
    """
    outer_buf = io.BytesIO()
    with zipfile.ZipFile(outer_buf, "w", zipfile.ZIP_DEFLATED) as outer:

        stale_threshold = n_nested_zips // 2 + 1 if stale_majority else 0

        for i in range(n_nested_zips):
            nested_name = f"sepa_1_comercio-sepa-{i}_{target_date}_01-01-01.zip"

            if corrupt_nested and i == 0:
                # Corrupt the first nested zip
                outer.writestr(nested_name, b"this is not a zip")
                continue

            nested_buf = io.BytesIO()
            with zipfile.ZipFile(nested_buf, "w") as nested:
                is_stale = i < stale_threshold

                # Write comercio
                if not (missing_csv and i == 0):
                    df_comercio = make_comercio_df(1)
                    nested.writestr(
                        f"comercio.csv",
                        _write_csv_with_footer(df_comercio, is_stale, target_date)
                    )

                # Write sucursales
                df_sucursales = make_sucursales_df(1)
                sucursales_csv = df_sucursales.write_csv()
                nested.writestr("sucursales.csv", sucursales_csv.encode("utf-8") if isinstance(sucursales_csv, str) else sucursales_csv)

                # Write productos
                df_productos = make_productos_df(n_productos)
                productos_csv = df_productos.write_csv()
                nested.writestr("productos.csv", productos_csv.encode("utf-8") if isinstance(productos_csv, str) else productos_csv)

            outer.writestr(nested_name, nested_buf.getvalue())

    return outer_buf.getvalue()
