from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table


def get_catalog():
    return load_catalog("default")


def get_table(table_id: str) -> Table:
    catalog = get_catalog()
    return catalog.load_table(table_id)
