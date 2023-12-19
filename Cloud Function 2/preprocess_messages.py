from google.cloud import storage
from google.oauth2 import service_account
import pandas as pd
import json 
from datetime import datetime

credentials = service_account.Credentials.from_service_account_file("credentials.json")
bucket_name = "stocks-historical-data"

def download_blob(bucket_name, credentials):
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.bucket(bucket_name)
    today_blob_prefix = datetime.today().strftime("%Y-%m-%dT")    
    today_blob_names = bucket.list_blobs(prefix=today_blob_prefix)
    if not today_blob_names:
        raise Exception(f"No blobs found matching '{today_blob_prefix}'")
    blob_list = []
    for blob in today_blob_names:
        blob_list.append(blob.name)
    blob = bucket.get_blob(blob_list[-1])
    contents = blob.download_as_string()
    return contents

def get_dataframe():
    blob = download_blob(bucket_name, credentials)
    data = json.loads(blob)
    extracted_data = []
    for key, value in data.items():
        if 'results' in value:
            row = {
                'ticker': key,
                'v': value['results'][0]['v'],
                'vw': value['results'][0]['vw'],
                'o': value['results'][0]['o'],
                'c': value['results'][0]['c'],
                'h': value['results'][0]['h'],
                'l': value['results'][0]['l'],
                't': value['results'][0]['t'],
                'n': value['results'][0]['n']
            }
            extracted_data.append(row)
    # Convert the extracted data to a DataFrame
    df = pd.DataFrame(extracted_data)
    df = df.rename(columns={"ticker": "stock_symbol", "v": "trading_volume", "vw": "volume_weighted_avg_price", "o": "open_price", "c": "close_price", "h": "highest_price", "l": "lowest_price", "t": "unix_timestamp", "n": "number_of_transactions"})
    return df

