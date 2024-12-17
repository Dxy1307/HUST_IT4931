from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

# Khởi tạo Spark session
spark = SparkSession.builder \
        .appName("RecordCountRealTime") \
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
kafka_stream = spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("subscribe", "test") \
                .load()

parse_df = kafka_stream.select(from_json(col("value").cast("string"), schema).alias("data"))

# Định dạng thời gian
df_with_time = parse_df.withColumn("event_time", col("data.event_time").cast("timestamp"))

# Thêm watermark để xử lý dữ liệu đến muộn
df_with_time = df_with_time.withWatermark("event_time", "10 minutes")

record_count = df_with_time.groupBy(
    window(col("event_time"), "1 minute")
).agg(
    count("*").alias("record_count")
)

record_count_display = record_count.withColumn(
    "unique_id",
    expr("uuid()")
).select(
    "unique_id",
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("record_count")
)

query = record_count_display.writeStream \
    .outputMode("update") \
    .format("org.elasticsearch.spark.sql") \
    .option("es.resource", "record_count_realtime") \
    .option("es.nodes", "http://localhost") \
    .option("es.port", "9200") \
    .option("es.net.ssl", "false") \
    .option("es.mapping.id", "unique_id") \
    .option("checkpointLocation", "E:/BachKhoa/20241/BigData/HUST_IT4931/checkpoint/record_count_realtime") \
    .trigger(processingTime="1 minute") \
    .start()

query.awaitTermination()