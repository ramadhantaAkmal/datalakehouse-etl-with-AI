from iceberg_catalog import catalog_load

catalog = catalog_load()
table = catalog.load_table("job_results.jobs_results_silver")

df = table.scan().to_polars()
print(df)
