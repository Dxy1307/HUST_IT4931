# Mục tiêu: Đếm số lượng lần xem của mỗi sản phẩm (product_id) theo thời gian.

# Ứng dụng: Cung cấp thông tin về các sản phẩm phổ biến, giúp quyết định chiến lược trưng bày sản phẩm, quảng cáo.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, FloatType

# Khởi tạo Spark session
spark = SparkSession.builder.appName("MostViewedProducts").getOrCreate()

# Đọc dữ liệu từ Kafka
kafka_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "your_topic") \
    .load()

# Giải mã dữ liệu JSON từ Kafka
df = kafka_stream.selectExpr("CAST(value AS STRING)").alias("json_value")
df = df.select(from_json(col("json_value"), "event_time STRING, event_type STRING, product_id STRING, category_id STRING, category_code STRING, brand STRING, price FLOAT, user_id STRING, user_session STRING").alias("data"))

# Chuyển đổi cột event_time thành timestamp
df_with_time = df.withColumn("event_time", col("data.event_time").cast("timestamp"))

# Lọc các hành động "view"
view_events = df_with_time.filter(col("data.event_type") == "view")

# Nhóm theo product_id và cửa sổ thời gian (ví dụ: mỗi 1 giờ)
product_views = view_events.groupBy(window(col("event_time"), "1 hour"), "data.product_id") \
    .agg(count("*").alias("view_count"))

# Sắp xếp kết quả theo số lần xem (view_count) giảm dần
sorted_product_views = product_views.orderBy(col("view_count").desc())

# Ghi kết quả ra console
query = sorted_product_views.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
