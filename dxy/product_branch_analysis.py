# Mục tiêu: Phân tích các hành động theo sản phẩm, giúp hiểu được sản phẩm nào được xem nhiều nhất, 
# được thêm vào giỏ hàng nhiều nhất, hoặc mua nhiều nhất.

# Ứng dụng: Xác định các sản phẩm đang hot hoặc chiến lược marketing cho sản phẩm nào hiệu quả.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

# Khởi tạo Spark session
spark = SparkSession.builder \
        .appName("ProductBrandActionAnalysis") \
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
kafka_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test") \
    .load()

# Giải mã giá trị JSON từ Kafka
# df = kafka_stream.selectExpr("CAST(value AS STRING)").alias("json_value")
# df = df.select(from_json(col("json_value"), "event_time STRING, event_type STRING, product_id STRING, category_id STRING, category_code STRING, brand STRING, price FLOAT, user_id STRING, user_session STRING").alias("data"))

parse_df = kafka_stream.select(from_json(col("value").cast("string"), schema).alias("data"))

# Chuyển đổi cột event_time thành kiểu timestamp và tạo watermark
df_with_time = parse_df.withColumn("event_time", col("data.event_time").cast("timestamp"))

# Thêm watermark để xử lý dữ liệu đến muộn
df_with_time = df_with_time.withWatermark("event_time", "10 minutes")

# Nhóm dữ liệu theo product_id, brand và loại hành động event_type (view, add to cart, purchase)
df_grouped = df_with_time.groupBy(
    window(col("event_time"), "1 minute"), 
    "data.product_id", "data.brand", "data.event_type"
).agg(
    count("*").alias("event_count")
)

# Chọn các cột cần hiển thị và đổi tên cho dễ đọc
df_to_display = df_grouped.select(
    # col("window.start").alias("window_start"),
    # col("window.end").alias("window_end"),
    col("product_id"),
    col("brand"),
    col("event_type"),
    col("event_count")
)

# Hiển thị kết quả ra console (hoặc có thể ghi vào nơi khác như HDFS, S3)
query = df_to_display.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()