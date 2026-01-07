import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, IntegerType, ListType, TimestampType
)  # Adjust types based on your schema

catalog = load_catalog(
    "my_catalog",
    **{
        "type": "hadoop",  # file-based, metadata stored in the warehouse path
        "warehouse": "s3://my-bucket/warehouse/",  # root for Iceberg metadata
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "minioadmin",
        "s3.secret-access-key": "minioadmin",
        "s3.allow-http": "true"
    }
)

# Step 4: Infer or Define Schema (match your Polars DF schema)
# Use PyIceberg types - adjust based on df.schema
schema = Schema(
    NestedField(field_id=1, name="title", field_type=StringType(), required=False),
    NestedField(field_id=2, name="company_name", field_type=StringType(), required=False),
    NestedField(field_id=3, name="location", field_type=StringType(), required=False),
    NestedField(field_id=4, name="description", field_type=StringType(), required=False),
    NestedField(field_id=5, name="apply_options", field_type=ListType(element_id=100, element_type=StringType(), element_required=False), required=False),  # list of dicts? Use struct if nested
    NestedField(field_id=6, name="schedule_type", field_type=StringType(), required=False),
    NestedField(field_id=7, name="qualifications", field_type=ListType(element_id=101, element_type=StringType(), element_required=False), required=False),
    NestedField(field_id=8, name="benefits", field_type=ListType(element_id=102, element_type=StringType(), element_required=False), required=False),
    NestedField(field_id=9, name="responsibilities", field_type=ListType(element_id=103, element_type=StringType(), element_required=False), required=False),
    NestedField(field_id=10, name="tools_requirement", field_type=ListType(element_id=104, element_type=StringType(), element_required=False), required=False),
    NestedField(field_id=11, name="years_of_experience", field_type=StringType(), required=False),  # or IntegerType if numeric
    NestedField(field_id=12, name="date", field_type=TimestampType(), required=True)  # for partitioning
)

# Step 5: Create the Iceberg Table (pointing to your Parquet location)
# Partition by 'date' for dated files
table = catalog.create_table(
    identifier="default.jobs_results",  # namespace.table_name
    schema=schema,
    location="s3://my-bucket/jobs-result-weekly/",  # where Parquets live
    partition_spec=[{"field": "date", "transform": "identity"}]  # or 'day(date)' for daily
)

# Step 6: Add Existing Parquet Files to the Table
# Iceberg will adopt them if schema matches (run for each new file/date)
# Convert Polars to Arrow for append (or use table.add_files() for existing Parquets)
arrow_table = df.to_arrow()  # for new data; for existing, use table.add_files("s3://path/to/file.parquet")

table.append(arrow_table)  # appends and commits a snapshot

# Or to add all existing Parquets at once (use glob or list from MinIO)
# table.add_files(["s3://my-bucket/jobs-result-weekly/date=2026-01-05/data.parquet", ...])

print("Iceberg table created! Metadata at: s3://my-bucket/warehouse/default/jobs_results/metadata/")