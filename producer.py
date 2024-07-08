from kafka import KafkaProducer
import json
import time
import random

# Function that will generate random data to simulate user activity
def data_generator():
    events = ['play', 'pause', 'stop', 'next', 'previous', 'volume_up', 'volume_down', 'like', 'dislike']
    return {
        'user_id': random.randint(1000, 9999),
        'activity': random.choice(events),
        'timestamp': int(time.time())
    }

# Create producer
producer = KafkaProducer(
    bootstrap_servers = 'localhost:9092', 
    value_serializer = lambda v: json.dumps(v).encode('utf-8')
)

# Send random data every second
while True:
    activity = data_generator()
    producer.send('listening-activity', activity)
    print(activity)
    time.sleep(1)