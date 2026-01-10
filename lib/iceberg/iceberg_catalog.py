from pyiceberg.catalog import load_catalog
from pyiceberg import Catalog

def catalog_load() -> Catalog:
    return load_catalog(
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