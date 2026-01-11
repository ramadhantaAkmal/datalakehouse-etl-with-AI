from pyiceberg.types import StringType

def update_table(catalog):
    silver = catalog.load_table("job_results.jobs_results_silver")
    
    with silver.update_schema() as update:
        update.add_column("YoE", StringType())
        update.delete_column("years_of_experience")

        
    print("Table updated successfully!")