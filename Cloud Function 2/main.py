from preprocess_messages import get_dataframe
import datetime
from google.cloud import bigquery
import pandas as pd
import pytz
from google.oauth2 import service_account
import json

credentials = service_account.Credentials.from_service_account_file("credentials.json")
client = bigquery.Client(credentials=credentials)

today = datetime.date.today()
one_day_prior = today - datetime.timedelta(days=1)
table_name_by_date = one_day_prior.strftime("%Y%m%d")
single_table_id = f"stockswhatsup.stocks_data.{table_name_by_date}"
all_data_table_id = "stockswhatsup.stocks_data_historical.stocks_data_all"

def load_to_bq(event, context):
  dataframe = get_dataframe()

  single_table_job_config = bigquery.LoadJobConfig(
      schema=[
          bigquery.SchemaField("date", bigquery.enums.SqlTypeNames.DATE),
          bigquery.SchemaField("stock_symbol", bigquery.enums.SqlTypeNames.STRING),
          bigquery.SchemaField("trading_volume", bigquery.enums.SqlTypeNames.FLOAT),
          bigquery.SchemaField("volume_weighted_avg_price", bigquery.enums.SqlTypeNames.FLOAT),
          bigquery.SchemaField("open_price", bigquery.enums.SqlTypeNames.FLOAT),
          bigquery.SchemaField("close_price", bigquery.enums.SqlTypeNames.FLOAT),
          bigquery.SchemaField("highest_price", bigquery.enums.SqlTypeNames.FLOAT),
          bigquery.SchemaField("lowest_price", bigquery.enums.SqlTypeNames.FLOAT),
          bigquery.SchemaField("unix_timestamp", bigquery.enums.SqlTypeNames.INT64),
          bigquery.SchemaField("number_of_transactions", bigquery.enums.SqlTypeNames.INT64),
      ],
      write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
  )
  single_table_job = client.load_table_from_dataframe(
      dataframe, single_table_id, job_config=single_table_job_config
  )

  historical_table_job_config = bigquery.LoadJobConfig(
      schema=[
          bigquery.SchemaField("date", bigquery.enums.SqlTypeNames.DATE),
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
  historical_table_job = client.load_table_from_dataframe(
      dataframe, all_data_table_id, job_config=historical_table_job_config
  )

