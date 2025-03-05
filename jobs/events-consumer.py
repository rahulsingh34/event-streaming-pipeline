import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
from pyflink.table.udf import udf
import requests
import json

# Environment variables
POSTGRES_URL = os.getenv("POSTGRES_URL")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
IP_CODING_KEY = os.getenv("IP_CODING_KEY")

# Set up the execution environment
env = StreamExecutionEnvironment.get_execution_environment()
env.enable_checkpointing(10 * 1000)
env.set_parallelism(1)

settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
table_env = StreamTableEnvironment.create(env, environment_settings=settings)

table_env.get_config().get_configuration().set_string("table.exec.source.cdc-mode", "true")

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
        'properties.bootstrap.servers' = 'kafka:9092',
        'properties.group.id' = 'user-events-consumer-group',
        'scan.startup.mode' = 'earliest-offset',
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
        ip_address STRING,
        geodata STRING
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

# Define the UDF
@udf(result_type=DataTypes.STRING())
def get_location(ip_address: str) -> str:
        if not ip_address:
            return json.dumps({})
        
        url = "https://api.ip2location.io"
        response = requests.get(url, params={
            'ip': ip_address,
            'key': IP_CODING_KEY
        })
        if response.status_code != 200:
            # Return empty dict if request failed
            return json.dumps({})
        return response.text
    
# Register the function in Flink SQL
table_env.create_temporary_function("get_location", get_location)

# Insert data to PostgreSQL
table_env.execute_sql("""
    INSERT INTO listening_activity
    SELECT user_id, activity, event_timestamp, ip_address, get_location(ip_address)
    FROM kafka_source
""")
