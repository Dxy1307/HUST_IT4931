import pandas as pd
from confluent_kafka import Producer
import time

def delivery_report(err,msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

producer = Producer({'bootstrap.servers': 'localhost:9092'})

csv_file = 'E:/bigdata/datasets/2019-Nov.csv'
topic_name = 'test'

chunk_size = 1000
previous_time = None

for chunk in pd.read_csv(csv_file, chunksize=chunk_size, parse_dates=['event_time']):
    chunk['event_time'] = chunk['event_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
    for timestamp, rows in chunk.groupby('event_time'):
        if previous_time is not None:
            delay = (pd.to_datetime(timestamp) - pd.to_datetime(previous_time)).total_seconds()
            if delay > 0:
                time.sleep(delay)

        for _, row in rows.iterrows():
            message = row.to_json()
            producer.produce(topic_name, message.encode('utf-8'), callback=delivery_report)
            producer.poll(0)

        previous_time = timestamp
        
producer.flush()
print('Data sent to Kafka')