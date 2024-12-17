# Mục tiêu: Xác định người dùng có nhiều hành động nhất trong một khoảng thời gian nhất định.

# Ứng dụng: Giúp xác định những người dùng có khả năng chuyển đổi cao, hoặc những người có hành vi mua sắm đặc biệt.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, FloatType

# Khởi tạo Spark session
spark = SparkSession.builder.appName("TopActiveUsers").getOrCreate()

# Đọc dữ liệu từ Kafka
kafka_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "your_topic") \
    .load()

# Giải mã giá trị JSON từ Kafka
df = kafka_stream.selectExpr("CAST(value AS STRING)").alias("json_value")
df = df.select(from_json(col("json_value"), "event_time STRING, event_type STRING, product_id STRING, category_id STRING, category_code STRING, brand STRING, price FLOAT, user_id STRING, user_session STRING").alias("data"))

# Chuyển đổi cột event_time thành kiểu timestamp và thêm cột thời gian
df_with_time = df.withColumn("event_time", col("data.event_time").cast("timestamp"))

# Thêm watermark để xử lý dữ liệu đến muộn
df_with_time = df_with_time.withWatermark("event_time", "10 minutes")

# Nhóm dữ liệu theo user_id và cửa sổ thời gian 1 giờ
df_user_activity = df_with_time.groupBy(window(col("event_time"), "1 hour"), "data.user_id") \
    .agg(count("*").alias("action_count"))

# Sắp xếp để tìm người dùng có nhiều hành động nhất trong mỗi cửa sổ
df_top_users = df_user_activity.orderBy(col("action_count").desc())

# Ghi dữ liệu ra console
query = df_top_users.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
