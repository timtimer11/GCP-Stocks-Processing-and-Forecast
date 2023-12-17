import time
import random
from google.cloud import pubsub_v1
from google.oauth2 import service_account
import json
from get_stocks_by_api import get_stocks_data

credentials = service_account.Credentials.from_service_account_file("credentials.json")
publisher = pubsub_v1.PublisherClient(credentials=credentials)

project_id = "<PROJECT_ID>"
topic_id = "<TOPIC_ID>"
topic_path = publisher.topic_path(project_id, topic_id)

def callback(future):
    try:
        message_id = future.result()
        print(f"Published message with ID: {message_id}")
    except Exception as ex:
        print(f"Error publishing message: {ex}")

def publish_to_pubsub():
    data = get_stocks_data()
    json_bytes = data.encode("utf-8")

    try:
        future = publisher.publish(topic_path, data=json_bytes)
        future.add_done_callback(callback)
        future.result()
    except Exception as ex:
        print(f"Exception encountered: {ex}")
        
    time.sleep(1)
