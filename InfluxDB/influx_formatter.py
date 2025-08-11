import time
from kafka import KafkaConsumer
import json
from datetime import datetime
import os

from influxdb_client import InfluxDBClient, Point
from influx_writer import influx_write

# Kafka consumer configuration
bootstrap_servers = ['localhost:9092']
topic = 'Customer_data'

# Create Kafka consumer
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    # group_id=None,
    value_deserializer=lambda x: x.decode('utf-8')
)


token = os.environ['influx_token']
org = os.environ['influx_org']
url = os.environ['influx_url']
bucket = os.environ['influx_bucket']

# Define the time window in seconds (e.g., process every 60 seconds)
TIME_WINDOW = 60
start_time = time.time()

# Initialize batch buffer
batch = []
batch_no = 0


try:
    for message in consumer:

        data=json.loads(str(message.value))

        if data['action_type']=='purchase':

            point = Point("ecommerce_transactions") \
                .tag("user_id", data["user_id"]) \
                .tag("campaign_code", data["campaign_code"]) \
                .tag("action_type", data["action_type"]) \
                .tag("category", data["category"]) \
                .tag("payment_method_id", data["payment_method_id"]) \
                .field("session_id", data["session_id"]) \
                .field("product_id", data["product_id"]) \
                .field("quantity", data["quantity"]) \
                .field("total_price", data["total_price"]) \
                .time(datetime.strptime(data["timestamp"], "%Y-%m-%dT%H:%M:%SZ"))

        else:

            point = Point("ecommerce_transactions") \
                .tag("user_id", data["user_id"]) \
                .tag("campaign_code", data["campaign_code"]) \
                .tag("action_type", data["action_type"]) \
                .tag("category", data["category"]) \
                .field("session_id", data["session_id"]) \
                .field("product_id", data["product_id"]) \
                .time(datetime.strptime(data["timestamp"], "%Y-%m-%dT%H:%M:%SZ"))



        batch.append(point)

        # Check if the time window has elapsed
        if time.time() - start_time >= TIME_WINDOW:
            batch_no+=1
            print(f"Batch No : {batch_no}")
            influx_write(batch, url, token, org, bucket)

            batch.clear()
            start_time = time.time()  # Reset the timer for the next window
            
except KeyboardInterrupt:
    # exit on Ctrl+C
    print("Exiting gracefully")
finally:
    # Process any remaining messages in the batch before exiting
    if len(batch) > 0:

        influx_write(batch, url, token, org, bucket)
        batch.clear()
