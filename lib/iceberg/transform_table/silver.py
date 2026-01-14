import polars as pl
from datetime import datetime,timezone

def transform_silver(catalog):
    bronze = catalog.load_table(("job_results", "jobs_results_bronze"))
    silver = catalog.load_table(("job_results", "jobs_results_silver"))

    # 1. Load to Arrow / Polars
    df = pl.from_arrow(bronze.scan().to_arrow())

    # 2. Deduplicate
    df = df.unique(
        subset=["title", "company_name", "location"],
        keep="first"
    )

    # 3. Generate job_id
    df = df.with_columns(
        pl.concat_str(
            [pl.col("title"), pl.col("company_name"), pl.col("location")]
        ).hash().cast(pl.Utf8, strict=True).alias("job_id")
    )
    

    # 4. Clean arrays
    df = df.with_columns(
        pl.col("benefits").list.drop_nulls(),
        pl.col("qualifications").list.drop_nulls(),
    )

    # 5. Normalize date
    df = df.with_columns(
        pl.col("ingestion_date").dt.date(),
        pl.lit(datetime.now()).alias("created_at")
    )


    df = df.with_columns(
        pl.when(
            pl.col("years_of_experience")
            .str.contains(r"\d+\s*[-–]\s*\d+")
        )
        .then(
            pl.col("years_of_experience")
            .str.extract(r"(\d+\s*[-–]\s*\d+)", 1)
            .str.replace_all(r"\s+", "")
        )
        .otherwise(
            pl.col("years_of_experience")
            .str.extract(r"(\d+)", 1)
        )
        .alias("YoE")
    )
    
    df.drop_in_place('years_of_experience')

    arrow_table = df.to_arrow()

    for i, field in enumerate(arrow_table.schema):
        if field.name in {"job_id", "ingestion_date", "created_at"}:
            arrow_table = arrow_table.set_column(
                i,
                field.with_nullable(False),
                arrow_table.column(i),
            )

    # 6. Append to silver
    silver.append(arrow_table)
    
    print("Silver table transform and append success!")
