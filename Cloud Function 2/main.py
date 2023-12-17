from preprocess_messages import get_dataframe
import datetime
from google.cloud import bigquery
import pandas as pd
import pytz
from google.oauth2 import service_account
import json

credentials = service_account.Credentials.from_service_account_file("credentials.json")
client = bigquery.Client(credentials=credentials)

current_date = datetime.datetime.now().strftime("%Y%m%d")
table_id = f"<PROJECT_ID>.<DATABASE_ID>.{current_date}"
def load_to_bq(event, context):
  dataframe = get_dataframe()

  job_config = bigquery.LoadJobConfig(
      schema=[
          bigquery.SchemaField("stock_symbol", bigquery.enums.SqlTypeNames.STRING),
          bigquery.SchemaField("trading_volume", bigquery.enums.SqlTypeNames.FLOAT),
          bigquery.SchemaField("volume_weighted_avg_price", bigquery.enums.SqlTypeNames.FLOAT),
          bigquery.SchemaField("open_price", bigquery.enums.SqlTypeNames.FLOAT),
          bigquery.SchemaField("close_price", bigquery.enums.SqlTypeNames.FLOAT),
          bigquery.SchemaField("highest_price", bigquery.enums.SqlTypeNames.FLOAT),
          bigquery.SchemaField("lowest_price", bigquery.enums.SqlTypeNames.FLOAT),
          bigquery.SchemaField("unix_timestamp", bigquery.enums.SqlTypeNames.INT64),
          bigquery.SchemaField("number_of_transactions", bigquery.enums.SqlTypeNames.INT64),
      ]
  )

  job = client.load_table_from_dataframe(
      dataframe, table_id, job_config=job_config
  )
