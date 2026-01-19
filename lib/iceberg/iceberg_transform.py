from iceberg_catalog import catalog_load
from transform_table import silver,gold

def transform():
    catalog = catalog_load()
    silver.transform_silver(catalog)
    
transform()