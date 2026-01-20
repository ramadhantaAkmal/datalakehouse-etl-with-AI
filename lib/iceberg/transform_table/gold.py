import polars as pl
from pyiceberg.catalog import Catalog

def transform_gold(df:pl.DataFrame, catalog: Catalog):
    silver = catalog.load_table(("job_results", "jobs_results_silver"))
    ori_table = pl.from_arrow(silver.scan().to_arrow())
    
    df_tool = (ori_table.explode("tools_requirement")
      .filter(pl.col("tools_requirement").is_not_null())
      .group_by("tools_requirement")
      .agg(pl.len().alias("count"))
      .rename({"tools_requirement": "tool_name"})
      .sort("count",descending=True))
    
    arrow_table = df_tool.to_arrow()
    
    for i, field in enumerate(arrow_table.schema):
            if field.name in {"tool_name", "count"}:
                arrow_table = arrow_table.set_column(
                    i,
                    field.with_nullable(False),
                    arrow_table.column(i),
                )  
    
    table = catalog.load_table(("job_results", "jobs_tools_count"))
    table.overwrite(arrow_table)
    
    


