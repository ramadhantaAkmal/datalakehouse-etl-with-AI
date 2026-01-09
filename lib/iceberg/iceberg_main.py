from pyiceberg.catalog import load_catalog
from create_table import create_table

def main():
    catalog = load_catalog(
        "catalog",
        type="rest",
        uri="http://localhost:8181",
        warehouse="s3://warehouse/",
        **{
            # MinIO (S3-compatible)
            "fs.native-s3.enabled": "true",
            "s3.endpoint": "http://localhost:9000",
            "s3.region": "us-east-1",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
            "s3.path-style-access": "true",
            "s3.allow-http": "true",
        }
    )
    
    catalog.create_namespace_if_not_exists("default")

    table_id = ("default", "jobs_results_bronze")
    
    tables = catalog.list_tables("default")

    if table_id in tables:
        print("Table already exists!")
    else:
        create_table(catalog)
main()