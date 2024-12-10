# Mục tiêu: Đếm số lượng hành động của người dùng theo từng loại hành động 
# (view, add to cart, purchase) trong các cửa sổ thời gian (ví dụ: 1 phút, 1 giờ).

# Ứng dụng: Có thể áp dụng để phân tích mức độ quan tâm đối với sản phẩm
# hay để đánh giá hiệu quả của chiến dịch quảng cáo trong thời gian thực.
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

# Khởi tạo Spark session
spark = SparkSession.builder \
        .appName("UserActionsByTime") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
        .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType() \
    .add("event_time", StringType()) \
    .add("event_type", StringType()) \
    .add("product_id", IntegerType()) \
    .add("category_id", StringType()) \
    .add("category_code", StringType()) \
    .add("brand", StringType()) \
    .add("price", DoubleType()) \
    .add("user_id", IntegerType()) \
    .add("user_session", StringType())

# Đọc dữ liệu từ Kafka
kafka_stream = spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("subscribe", "test") \
                .load()

# Giải mã dữ liệu JSON
# df = kafka_stream.selectExpr("CAST(value AS STRING)").alias("json_value")
# df = df.select(from_json(col("json_value"), "event_time STRING, event_type STRING, product_id STRING, category_id STRING, category_code STRING, brand STRING, price FLOAT, user_id STRING, user_session STRING").alias("data"))

parse_df = kafka_stream.select(from_json(col("value").cast("string"), schema).alias("data"))

# Định dạng thời gian
df_with_time = parse_df.withColumn("event_time", col("data.event_time").cast("timestamp"))

# Thêm watermark để xử lý dữ liệu đến muộn
df_with_time = df_with_time.withWatermark("event_time", "10 minutes")

# Nhóm theo cửa sổ thời gian 1 phút và loại hành động
df_grouped = df_with_time.groupBy(
    window(col("event_time"), "1 minute"), 
    "data.event_type"
).agg(
    count("*").alias("event_count")
)

# # Ghi dữ liệu ra console để xem kết quả
# query = df_grouped.writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .start()

# Ghi dữ liệu ra console với thông tin chi tiết về thời gian của cửa sổ
query = df_grouped.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("event_type"),
    col("event_count")
).writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="1 minute") \
    .start()

query.awaitTermination()