import polars as pl
import json
import datetime
from jobs_scrape import scrape_jobs
from lib.extract import extract_essential_data
from utils.api_util import extract_with_n8n

def main():
    i=0
    next_page_token="" 
    df=pl.DataFrame()
    #Limit the ingest data loop to only 3 times
    while i < 3:
        data = scrape_jobs(next_page_token)
        next_page_token=data["serpapi_pagination"]["next_page_token"]
        clean_data = extract_essential_data(data)
        df = pl.concat([df, clean_data], how="vertical")
        i += 1
    df = extract_with_n8n(df)
    current_date = datetime.date.today()
    
    parquet_path = f'/opt/airflow/lib/jobs-result-weekly/jobs-result-{current_date}.parquet'
    json_path = f'/opt/airflow/lib/jobs-result-weekly/jobs-result-{current_date}.json'
    df.write_parquet(parquet_path, compression='snappy')
    
    json_object = json.loads(df.write_json())

    # Write the formatted output to a file
    with open(json_path, "w") as f:
        json.dump(json_object, f, indent=4)
    print(f"Saved file to: {json_path}") 
    