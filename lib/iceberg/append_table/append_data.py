from pyiceberg.catalog import load_catalog

def append_raw_data(catalog):
    table = catalog.load_table(("job_results", "jobs_results_bronze"))

    files = [
        "s3://jobs-results-lake/ingestion_date=2026-01-13/00000000.parquet",
    ]

    table.add_files(files)
