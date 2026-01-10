from pyiceberg.catalog import load_catalog

def append_raw_data(catalog):
    table = catalog.load_table(("default", "jobs_results_bronze"))

    files = [
        "s3://jobs-results-lake/ingestion_date=2026-01-06/00000000.parquet",
        "s3://jobs-results-lake/ingestion_date=2026-01-07/00000000.parquet",
    ]

    table.add_files(files)
