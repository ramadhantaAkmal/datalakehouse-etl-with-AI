import polars as pl
from jobspy import scrape_jobs

#Ingest data
def ingest() -> pl.DataFrame:
    jobs = scrape_jobs(
            site_name=["linkedin"],
            search_term="data engineer",
            location="Jakarta",
            results_wanted=40,
            hours_old=168,
            linkedin_fetch_description=True
        )

    df = pl.from_pandas(jobs)
    df = df["title","company","location","description","job_type","job_url"]
    return df



