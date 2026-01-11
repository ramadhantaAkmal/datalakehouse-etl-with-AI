from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, IntegerType, DateType, LongType
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform

def create_table1(catalog):
    table_id = ("job_results", "jobs_experience_gold")
    tables = catalog.list_tables("job_results")
    
    if table_id in tables:
        print("jobs_experience_gold table already exists!")
    else:
        schema1 = Schema(
            NestedField(1, "job_id", StringType(), False),
            NestedField(3, "exp_min", IntegerType(), True),
            NestedField(4, "exp_max", IntegerType(), True),
            NestedField(5, "experience_bucket", StringType(), False),
            NestedField(6, "ingestion_date", DateType(), False),
        )

        partition_spec1 = PartitionSpec(
            PartitionField(
                source_id=6,
                field_id=1000,
                transform="identity",
                name="ingestion_date"
            )
        )

        catalog.create_table(
            identifier="job_results.jobs_experience_gold",
            schema=schema1,
            partition_spec=partition_spec1
        )
    print("jobs_experience_gold table created!")
        
        
        
def create_table2(catalog):
    table_id = ("job_results", "tools_demand_gold")
    tables = catalog.list_tables("job_results")
    
    if table_id in tables:
        print("tools_demand_gold table already exists!")
    else:
        schema2 = Schema(
            NestedField(1, "tool", StringType(), False),
            NestedField(2, "ingestion_date", DateType(), False),
            NestedField(3, "job_count", LongType(), False),
        )

        partition_spec2 = PartitionSpec(
            PartitionField(
                source_id=2,
                field_id=1000,
                transform="identity",
                name="ingestion_date"
            )
        )

        catalog.create_table(
            identifier="job_results.tools_demand_gold",
            schema=schema2,
            partition_spec=partition_spec2
        )
    print("tools_demand_gold table created!")
    