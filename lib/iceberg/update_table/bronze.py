from pyiceberg.types import DateType

def update_table(catalog):
    bronze = catalog.load_table("job_results.jobs_results_bronze")

    with bronze.update_schema() as update:
        update.update_column("ingestion_date", DateType())

    with bronze.update_schema() as update:
        update.make_column_optional("ingestion_date")
        
    print("Bronze table updated successfully!")