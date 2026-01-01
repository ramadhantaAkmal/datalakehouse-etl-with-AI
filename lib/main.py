import pandas as pd
import datetime
from jobs_scrape import scrape_jobs
from transform import clean_result_data

def main():
    i=0
    next_page_token=""
    df=pd.DataFrame()
    #Limit the ingest data loop to only 3 times
    while i < 3:
        data = scrape_jobs(next_page_token)
        next_page_token=data["serpapi_pagination"]["next_page_token"]
        clean_data = clean_result_data(data)
        df = pd.concat([df, clean_data], axis=0, ignore_index=True)
        i += 1

    current_date = datetime.date.today()
    df.to_json(f'/opt/airflow/lib/jobs-result-weekly/jobs-result-{current_date}.json', orient='records', indent=4)
    