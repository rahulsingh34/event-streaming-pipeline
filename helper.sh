# Access container
docker exec -it <kafka_container_id> /bin/bash

# Create Topic
/usr/bin/kafka-topics --create --topic test-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# List contents with kafka in name
ls /usr/bin | grep kafka

# List topics 
/usr/bin/kafka-topics --list --bootstrap-server localhost:9092

# Produce message
/usr/bin/kafka-console-producer --topic test-topic --bootstrap-server localhost:9092

# Consume messages from beginning
/usr/bin/kafka-console-consumer --topic test-topic --bootstrap-server localhost:9092 --from-beginning