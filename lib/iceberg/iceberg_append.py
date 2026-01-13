from iceberg_catalog import catalog_load
from append_table.append_data import append_raw_data

def append_data():
    catalog = catalog_load()
    
    append_raw_data(catalog)
    
append_data()