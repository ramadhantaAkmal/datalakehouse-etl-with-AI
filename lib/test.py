import polars as pl
from jobspy import scrape_jobs

jobs = scrape_jobs(
    site_name=["linkedin"],
    search_term="data engineer",
    location="Jakarta",
    results_wanted=20,
    hours_old=168,
    linkedin_fetch_description=True
)

df = pl.from_pandas(jobs)
df = df["title","company","location","description","job_type","date_posted"]
print(df)