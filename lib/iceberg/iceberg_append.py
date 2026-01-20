import polars as pl
from .iceberg_catalog import catalog_load

def transform_load_brz(df: pl.DataFrame):
    catalog = catalog_load()
    
    table = catalog.load_table(("job_results", "jobs_results_bronze"))

    arrow_table = df.to_arrow()

    table.append(arrow_table)
    catalog.close()