from google.cloud import storage
from google.oauth2 import service_account
import pandas as pd
import json 
from datetime import datetime

credentials = service_account.Credentials.from_service_account_file("credentials.json")
bucket_name = "<BUCKET_NAME>"

def download_blob(bucket_name, credentials):
    today = datetime.today().strftime("%Y-%m-%dT")
    source_blob_prefix = f"{today}"
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=source_blob_prefix))
    if not blobs:
        raise Exception(f"No blobs found matching '{source_blob_prefix}'")
    latest_blob = blobs[-1]
    content = latest_blob.download_as_string()
    return content

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

