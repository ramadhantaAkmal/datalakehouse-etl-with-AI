import polars as pl

def extract_essential_data(data: dict) -> pl.DataFrame:
    jobs_results_dict = data["jobs_results"]

    #Convert dict data into DataFrame
    df = pl.from_dicts(jobs_results_dict)

    #Removing unnecessary data
    df = df.drop(['share_link','thumbnail','extensions','job_id','via'])

    #Extracting schedule type data from detected extensions column and create new schedule_type column
    df = df.with_columns(
        pl.col('detected_extensions').struct.field('schedule_type').alias('schedule_type')
    )
    
    #Drop detected extension column because its unused
    df.drop_in_place('detected_extensions')
    
    return df

