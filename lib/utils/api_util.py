import polars as pl
import json
import requests

def extract_with_n8n(df1: pl.DataFrame):
    df1 = df1.with_row_index(name="index",offset=1)
    json_object = json.loads(df1.write_json())
    response = requests.post('http://localhost:5678/webhook/b41fb2f1-e65f-4b02-9e32-3629214ca314', json=json_object)

    extracted_data = response.json()
    df2 = pl.DataFrame(extracted_data["output"])
    df_merged = df1.join(df2, on="index")
    
    df_merged.drop_in_place('index')
    return df_merged