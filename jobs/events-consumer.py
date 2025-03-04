import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.common.serialization import SimpleStringSchema
import json

# Environment variables
POSTGRES_URL = os.getenv("POSTGRES_URL")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Set up the execution environment
env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)

# Define Kafka Source Table
kafka_source_ddl = f"""
    CREATE TABLE kafka_source (
        user_id INT,
        activity STRING,
        event_timestamp INT,
        ip_address STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'listening-activity',
        'properties.bootstrap.servers' = 'localhost:9092',
        'properties.group.id' = 'user-events-consumer-group',
        'scan.startup.mode' = 'group-offsets',
        'format' = 'json'
    )
"""
table_env.execute_sql(kafka_source_ddl)

# Define PostgreSQL Sink Table
postgres_sink_ddl = f"""
    CREATE TABLE listening_activity (
        user_id INT,
        activity STRING,
        event_timestamp INT,
        ip_address STRING
    ) WITH (
        'connector' = 'jdbc',
        'url' = '{POSTGRES_URL}',
        'table-name' = 'listening_activity',
        'driver' = 'org.postgresql.Driver',
        'username' = '{POSTGRES_USER}',
        'password' = '{POSTGRES_PASSWORD}'
    )
"""
table_env.execute_sql(postgres_sink_ddl)

# TODO: Add geo enrichment

# Insert data to PostgreSQL
table_env.execute_sql("INSERT INTO listening_activity SELECT * FROM kafka_source")

# Execute job
env.execute("Events Consumer Job")