from pyiceberg.catalog import load_catalog
from append_table.append_data import append_raw_data
from create_table import bronze,silver
from iceberg_catalog import catalog_load

def initialize():
    catalog = catalog_load()
    
    catalog.create_namespace_if_not_exists("job_results")

    bronze.create_table(catalog)
    silver.create_table(catalog)
    
    
initialize()