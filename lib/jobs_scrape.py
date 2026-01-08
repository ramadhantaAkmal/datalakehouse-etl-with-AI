import serpapi
import os

from dotenv import load_dotenv
load_dotenv()

api_key = os.getenv('SERPAPI_KEY') 
client = serpapi.Client(api_key=api_key)

#Ingest data
def scrape_jobs(token) -> dict:
    if token == "":
        results = client.search({
        'engine': "google_jobs",
        'q': 'Junior Data Engineer singapore',
        })
        return results
    
    results = client.search({
        'engine': "google_jobs",
        'q': 'Junior Data Engineer singapore',
        'next_page_token':token
    })
    return results.as_dict()



