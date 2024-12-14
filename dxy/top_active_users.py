# Mục tiêu: Xác định người dùng có nhiều hành động nhất trong một khoảng thời gian nhất định.

# Ứng dụng: Giúp xác định những người dùng có khả năng chuyển đổi cao, hoặc những người có hành vi mua sắm đặc biệt.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

# Khởi tạo Spark session
spark = SparkSession.builder \
    .appName("TopActiveUsers") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1") \
    .config("es.nodes", "http://localhost") \
    .config("es.port", "9200") \
    .config("es.net.ssl", "false") \
    .config("es.index.auto.create", "true") \
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
kafka_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test") \
    .load()

parse_df = kafka_stream.select(from_json(col("value").cast("string"), schema).alias("data"))

# Chuyển đổi cột event_time thành kiểu timestamp và thêm cột thời gian
df_with_time = parse_df.withColumn("event_time", col("data.event_time").cast("timestamp"))

# Thêm watermark để xử lý dữ liệu đến muộn
df_with_time = df_with_time.withWatermark("event_time", "10 minutes")

# Nhóm dữ liệu theo user_id và cửa sổ thời gian 1 giờ
df_user_activity = df_with_time.groupBy(window(col("event_time"), "10 minutes"), "data.user_id") \
    .agg(count("*").alias("action_count"))

top_users_display = df_user_activity.select(
    col("user_id"),
    col("action_count"),
)

# Ghi dữ liệu ra console
# query = df_top_users.writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()

query = top_users_display.writeStream \
    .outputMode("update") \
    .format("org.elasticsearch.spark.sql") \
    .option("es.resource", "top_active_users") \
    .option("es.nodes", "http://localhost") \
    .option("es.port", "9200") \
    .option("es.net.ssl", "false") \
    .option("es.mapping.id", "user_id") \
    .option("checkpointLocation", "E:/BachKhoa/20241/BigData/HUST_IT4931/checkpoint/top_active_users") \
    .trigger(processingTime="1 minute") \
    .start()

query.awaitTermination()