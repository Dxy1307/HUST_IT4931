from confluent_kafka import Consumer, KafkaException, KafkaError
from hdfs import InsecureClient
import json

# Cấu hình Kafka Consumer
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'
})

# Subscribe vào topic Kafka
topic_name = 'test'
consumer.subscribe([topic_name])

# Cấu hình kết nối HDFS
hdfs_client = InsecureClient('http://localhost:5070', user='hdfs_user')

# Đường dẫn đến thư mục HDFS
hdfs_path = '/user/hdfs/consumer_data/'

try:
    # Kiểm tra và tạo thư mục HDFS nếu chưa tồn tại
    if not hdfs_client.status(hdfs_path, strict=False):
        hdfs_client.makedirs(hdfs_path)

    # Tạo file JSON trên HDFS để lưu dữ liệu
    hdfs_file_path = hdfs_path + 'data_from_kafka.json'

    # Mở file HDFS để ghi
    with hdfs_client.write(hdfs_file_path, overwrite=True) as writer:
        # Mở một danh sách để chứa các bản ghi JSON
        json_data_list = []

        while True:
            # Nhận message từ Kafka
            msg = consumer.poll(1.0)

            if msg is None:
                # Nếu không có message, tiếp tục
                continue
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f'Reached end of partition: {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')
                else:
                    raise KafkaException(msg.error())
            else:
                # Chuyển dữ liệu từ Kafka vào HDFS dưới dạng JSON
                message_data = msg.value().decode('utf-8')
                
                # Chuyển mỗi dòng CSV thành dictionary (lưu ý: giả sử dữ liệu có định dạng CSV)
                csv_columns = ['event_time', 'event_type', 'product_id', 'category_id', 'category_code', 'brand', 'price', 'user_id', 'user_session']
                try:
                    row = dict(zip(csv_columns, message_data.split(',')))  # Tạo dictionary từ dòng CSV
                    json_data_list.append(row)  # Thêm vào danh sách JSON

                    # Ghi dữ liệu vào HDFS dưới dạng JSON sau mỗi batch (hoặc theo cách bạn muốn)
                    if len(json_data_list) >= 100:  # Lưu sau mỗi 100 dòng (có thể điều chỉnh theo nhu cầu)
                        json.dump(json_data_list, writer, indent=4)  # Ghi vào file JSON
                        json_data_list = []  # Reset danh sách sau khi ghi

                except Exception as e:
                    print(f"Error processing message: {e}")

finally:
    consumer.close()
    print("Consumer closed and data written to HDFS as JSON")
