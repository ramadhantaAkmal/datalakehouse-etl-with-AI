from update_table import silver
from iceberg_catalog import catalog_load

def update():
    catalog = catalog_load()
    silver.update_table(catalog)
    
update()