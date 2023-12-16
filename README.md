# Stocks Data ETL Pipeline on Google Cloud Platform
## Overview
This project sets up an end-to-end data pipeline to extract, load and tranform stocks data for MAANG companies (Meta, Amazon, Apple, Netflix, Google) using Google Cloud Platform infrastructure. The pipeline consists of fetching data from Polygon.io API with Cloud Function, publishing it to Pub/Sub, storing it in Cloud Storage, transforming and finally, loading it into BigQuery table.

[[pipeline_diagram.png]]

## Components
### Cloud Function 1:
- get_stocks_api.py: A script fetching stocks data for MAANG companies in JSON format using Polygon.io API.
- main.py: A script publishing the JSON data into a Pub/Sub topic.

### Cloud Function 2:
- preprocess_messages.py: A script to tranform data from Cloud Storage into dataframe with defined schema.
- main.py: A script responsible for uploading preprocessed data into a new BigQuery table.

### PubSub Topic: stocks-function-trigger
A PubSub Topic that triggers Cloud Function 1.

### PubSub Topic: stocks-data
A PubSub Topic that accepts JSON messages from Cloud Function 1.

### Cloud Scheduler:
Responsible for sending a message into PubSub topic "stocks-function-trigger" every midnight.

### Cloud Storage: stocks-historical-data
Cloud Storage bucket that stores all data passed from PubSub topic "stocks-data".

### BigQuery
Data warehouse to store data for each day in separate tables.  

