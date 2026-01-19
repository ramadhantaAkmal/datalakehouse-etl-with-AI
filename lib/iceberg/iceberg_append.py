import polars as pl
from .iceberg_catalog import catalog_load

def append_data(df: pl.DataFrame):
    catalog = catalog_load()
    
    table = catalog.load_table(("job_results", "jobs_results_bronze"))

    arrow_table = df.to_arrow()

    table.append(arrow_table)