from pyiceberg.types import StringType, ListType

def update_table(catalog):
    silver = catalog.load_table("job_results.jobs_results_silver")
    
    with silver.update_schema() as update:
        update.rename_column("responsibilites","responsibilities")
      
    print("Table updated successfully!")