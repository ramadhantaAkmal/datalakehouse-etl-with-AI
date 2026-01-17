import polars as pl
from pyiceberg.catalog import Catalog

def transform_gold(catalog: Catalog):
    silver = catalog.load_table(("job_results", "jobs_results_silver"))
    gold = catalog.load_table(("job_results", "tools_demand_gold"))
    
    ori_table = pl.from_arrow(silver.scan().to_arrow())
    
    
    df_exploded = (
        ori_table
        .select(["job_id", "tools_requirement", "ingestion_date"])
        .explode("tools_requirement")
        .rename({"tools_requirement": "tool"})
    )
    
    tools_demand_gold = (
        df_exploded
        .group_by(["tool", "ingestion_date"])
        .agg(
            pl.count("job_id").alias("job_count")
        )
    )
    
    print(tools_demand_gold)


