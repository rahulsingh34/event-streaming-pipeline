from kafka import KafkaProducer
import json
import time
import random

# Function that will generate random data to simulate user activity
def data_generator():
    events = ['play', 'pause', 'stop', 'next', 'previous', 'volume_up', 'volume_down', 'like', 'dislike']
    ip_addresses = [
        "80.186.25.56",
        "234.169.177.211",
        "4.155.1.125",
        "185.112.234.131",
        "191.123.192.133",
        "236.144.89.213",
        "230.157.219.106",
        "237.243.71.207",
        "26.140.16.35",
        "220.161.178.87"
    ]
    return {
        'user_id': random.randint(1000, 9999),
        'activity': random.choice(events),
        'event_timestamp': int(time.time()),
        'ip_address': random.choice(ip_addresses)
    }

# Create producer
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer = lambda v: json.dumps(v).encode('utf-8')
)

# Send random data every second
while True:
    activity = data_generator()
    producer.send('listening-activity', activity)
    print(activity)
    time.sleep(1)