import polars as pl
from .iceberg_catalog import catalog_load
from .transform_table import silver,gold

def transform_load_slv_gld(df:pl.DataFrame):
    catalog = catalog_load()
    silver.transform_silver(catalog,df)
