import pandas as pd
from confluent_kafka import Producer

# Khởi tạo Kafka producer
producer = Producer({'bootstrap.servers': 'localhost:9092'})

# Đọc dữ liệu từ CSV
csv_file = 'E:/bigdata/datasets/2019-Nov.csv'
topic_name = 'test'

# Số lượng bản ghi trong mỗi lần gửi
chunk_size = 1000

# Đọc từng chunk từ file CSV
for chunk in pd.read_csv(csv_file, chunksize=chunk_size, parse_dates=['event_time']):
    chunk['event_time'] = chunk['event_time'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Gửi tất cả các bản ghi trong chunk một lần
    for _, row in chunk.iterrows():
        # Chuyển đổi mỗi dòng thành chuỗi JSON
        message = row.to_json()

        # Gửi message đến Kafka
        producer.produce(topic_name, message.encode('utf-8'))
        # producer.poll(0)  # Đảm bảo callback được gọi
        
    # Gọi flush() sau mỗi chunk để đảm bảo rằng các message đã được gửi
    producer.flush()

print('Data sent to Kafka')
