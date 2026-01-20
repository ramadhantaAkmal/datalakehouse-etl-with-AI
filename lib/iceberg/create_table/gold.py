from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, IntegerType, DateType, LongType
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform

def create_table(catalog):
    table_id = ("job_results", "jobs_tools_count")
    tables = catalog.list_tables("job_results")
    
    if table_id in tables:
        print("jobs_tools_count table already exists!")
    else:
        tools_count_schema = Schema(
            NestedField(
                field_id=1,
                name="tool_name",
                field_type=StringType(),
                required=True
            ),
            NestedField(
                field_id=2,
                name="count",
                field_type=LongType(),
                required=True
            )
        )

        catalog.create_table(
            identifier="job_results.jobs_tools_count",
            schema=tools_count_schema,
        )
    print("jobs_tools_count table created!")
    
    