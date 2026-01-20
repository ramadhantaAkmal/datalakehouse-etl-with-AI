from create_table import bronze,silver,gold
from iceberg_catalog import catalog_load

def initialize():
    catalog = catalog_load()
    
    catalog.create_namespace_if_not_exists("job_results")

    # bronze.create_table(catalog)
    # silver.create_table(catalog)
    gold.create_table(catalog)
    catalog.close()
    
    
initialize()