from pyiceberg.catalog import load_catalog
from create_table import create_table

def main():
    catalog = load_catalog(
        "catalog",
        **{
            "type": "rest",
            "uri": "http://localhost:8181",  
            "warehouse": "s3://warehouse/",

            # MinIO (S3-compatible)
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "minioadmin",
            "s3.secret-access-key": "minioadmin",
            "s3.path-style-access": "true",
            "s3.allow-http": "true",
        }
    )
    
    # if(catalog.table_exists("default.jobs_results_bronze")):
    #     print("Tables already exist!")
    # else:
    create_table(catalog)
main()