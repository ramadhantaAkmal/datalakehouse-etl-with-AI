import polars as pl
from pyiceberg.schema import Schema
from pyiceberg.types import *
from pyiceberg.partitioning import PartitionSpec, PartitionField

def create_table(catalog):
    table_id = ("job_results", "jobs_results_silver")
    tables = catalog.list_tables("job_results")
    
    if table_id in tables:
        print("Silver table already exists!")
    else:
        schema = Schema(
            NestedField(1, "job_id", StringType(), required=True),
            NestedField(2, "title", StringType()),
            NestedField(3, "company_name", StringType()),
            NestedField(4, "location", StringType()),
            NestedField(5, "description", StringType()),
            NestedField(6, "schedule_type", StringType()),
            NestedField(7, "YoE", StringType()),

            NestedField(8, "responsibilities", ListType(107, StringType(), False)),
            NestedField(8, "qualifications", ListType(108, StringType(), False)),
            NestedField(9, "benefits", ListType(109, StringType(), False)),
            NestedField(10, "tools_requirement", ListType(110, StringType(), False)),

            NestedField(11, "ingestion_date", DateType(), required=True),
            NestedField(12, "created_at", TimestampType(), required=True),
        )

        partition_spec = PartitionSpec(
            PartitionField(
                source_id=11,
                field_id=1001,
                transform="identity",
                name="ingestion_date"
            )
        )

        catalog.create_table(
            identifier=("job_results", "jobs_results_silver"),  # namespace.table_name
            schema=schema,
            partition_spec=partition_spec  # or 'day(date)' for daily
        )
        
        print("Iceberg silver table created!")
