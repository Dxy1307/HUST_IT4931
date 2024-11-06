from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

spark = SparkSession.builder \
        .appName("KafkaSparkStreaming") \
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

kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "test") \
        .load()

parse_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data"))

final_df = parse_df.select(
    col("data.event_time"),
    col("data.event_type"),
    col("data.product_id"),
    col("data.category_id"),
    col("data.category_code"),
    col("data.brand"),
    col("data.price"),
    col("data.user_id"),
    col("data.user_session")
)

# final_df = final_df.select(
#     *[
#         col(c) if c not in ['event_time', 'event_type', 'category_id', 'category_code', 'brand', 'user_session']
#         else col(c).fillna('')
#         for c in final_df.columns
#     ]
# )

query = final_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

query.awaitTermination()