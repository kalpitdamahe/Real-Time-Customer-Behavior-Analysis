from kafka import KafkaProducer
import json
import requests

producer = KafkaProducer(bootstrap_servers='localhost:9092')

# URL of the Flask server's stream endpoint
url = 'http://127.0.0.1:5000/events'

topic = 'Customer_data'
key = 'data'


# Make a GET request to the stream endpoint with 'stream=True'
with requests.get(url, stream=True) as response:
    if response.status_code == 200:
        # Iterate over the streaming content in chunks
        
        for chunk in response.iter_lines():

            if chunk:  # Filter out keep-alive chunks
                # Process the received data (decode and print it)

                #value=json.dumps(chunk.decode('utf-8')).encode('utf-8')
                value=json.loads(str(json.dumps(chunk.decode('utf-8'))))

                producer.send(
                              topic=topic,
                              value=value.encode('utf-8'),
                              key=str(key).encode('utf-8')
                               ) 
    else:
        print(f"Error: Received status code {response.status_code}")