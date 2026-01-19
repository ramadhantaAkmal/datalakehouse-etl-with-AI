import polars as pl
from datetime import datetime
from pyiceberg.expressions import In
from pyiceberg.catalog import Catalog

def transform_silver(catalog: Catalog, df: pl.DataFrame):
    silver = catalog.load_table(("job_results", "jobs_results_silver"))

    # Filter
    df = df.filter(
        pl.col("title")
        .str.to_lowercase()
        .str.contains("data engineer")
    )

    # Deterministic job_id
    df = df.with_columns(
        pl.concat_str(
            [pl.col("title"), pl.col("company_name"), pl.col("location")]
        )
        .hash()
        .cast(pl.Utf8, strict=True)
        .alias("job_id")
    )

    # Enrichment
    df = df.with_columns(
        pl.col("ingestion_date").dt.date(),
        pl.lit(datetime.utcnow()).alias("created_at")
    )

    df = df.rename({"years_of_experience": "YoE"})

    # Collect job_ids for upsert
    job_ids = df.select("job_id").unique().to_series().to_list()

    arrow_table = df.to_arrow()

    # Enforce non-nullable fields
    for i, field in enumerate(arrow_table.schema):
        if field.name in {"job_id", "ingestion_date", "created_at"}:
            arrow_table = arrow_table.set_column(
                i,
                field.with_nullable(False),
                arrow_table.column(i),
            )

    # UPSERT logic
    if job_ids:
        silver.overwrite(
            df=arrow_table,
            overwrite_filter=In("job_id", job_ids),
        )

    print("Silver table upsert success!")
