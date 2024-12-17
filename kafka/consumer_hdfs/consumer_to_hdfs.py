import os
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
import pyhdfs

# Kafka consumer configuration
kafka_broker = os.getenv('KAFKA_BROKER', 'kafka:9092')  # Kafka broker URL
topic_name = 'quickstart-events'  # Topic name to consume

consumer_config = {
    'bootstrap.servers': kafka_broker,
    'group.id': 'hdfs-consumer-group',
    'auto.offset.reset': 'earliest'  # Start from the earliest message if no offset is stored
}

today = datetime.now()
# HDFS configuration
hdfs_directory = '/datasets/kafka_data'
hdfs_file = f"/datasets/kafka_data/{today.year}{today.month:02d}{today.day:02d}/messages.json"  # Đường dẫn tệp tin để lưu tất cả message

# Initialize Kafka Consumer
consumer = Consumer(consumer_config)
consumer.subscribe([topic_name])

# Initialize HDFS Client
fs = pyhdfs.HdfsClient('namenode.default.svc.cluster.local:9870', user_name='hdfs')

# Ensure the HDFS directory exists
if not fs.exists(hdfs_directory):
    fs.mkdirs(hdfs_directory)

print(f"Listening for messages on topic '{topic_name}'...")

# Ghi lần đầu tiên để tạo file (có thể là file trống)
fs.create(hdfs_file, overwrite=True, data="")  # Tạo file trống đầu tiên

try:
    while True:
        msg = consumer.poll(1.0)  # Wait for a message up to 1 second
        if msg is None:  # No message received
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"Reached end of partition: {msg.topic()} [{msg.partition()}]")
            else:
                print(f"Kafka error: {msg.error()}")
            continue

        # Process the message
        message_value = msg.value().decode('utf-8')
        print(f"Received message: {message_value}")

        # Save the message to the file
        fs.append(hdfs_file,message_value+"\n") 


except KeyboardInterrupt:
    print("Consumer interrupted by user")

finally:
    # Close the consumer
    consumer.close()
    print("Kafka consumer closed.")
