from kafka import KafkaConsumer
import psycopg2
import json

# Connect to PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="postgres"
)

# Create table if it doesn't exist
cursor = conn.cursor()
cursor.execute("CREATE TABLE IF NOT EXISTS listening_activity1 (user_id INT, activity TEXT, event_timestamp INT)")
conn.commit()

# Function to insert activity data into PostgreSQL
def insert_activity_data(activity):
    cursor.execute("INSERT INTO listening_activity1 (user_id, activity, event_timestamp) VALUES (%s, %s, %s)", (activity['user_id'], activity['activity'], activity['event_timestamp']))
    conn.commit()

# Create consumer
consumer = KafkaConsumer(
    'listening-activity',
    bootstrap_servers = 'localhost:9092',
    value_deserializer = lambda v: json.loads(v.decode('utf-8'))
)

# Consume messages
for message in consumer:
    activity = message.value
    print(activity)
    insert_activity_data(activity)