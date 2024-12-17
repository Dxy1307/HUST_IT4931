# transform_load_task.py
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import SparkConf
from datetime import datetime
import transformations as tf


run_time = "{:%Y%m%d}".format(datetime.now())
month = "{:%m}".format(datetime.now())
year = "{:%Y}".format(datetime.now())
raw_data_path = "hdfs://namenode:8020/extracted_data/year=" + year + "/month=" + month + "/raw_data/" + run_time

schema = StructType([
    StructField("event_time", TimestampType(), nullable=False),
    StructField("event_type", StringType(), nullable=False),
    StructField("product_id", LongType(), nullable=False),
    StructField("category_id", LongType(), nullable=False),
    StructField("category_code", StringType(), nullable=True),
    StructField("brand", StringType(), nullable=True),
    StructField("price", DoubleType(), nullable=False),
    StructField("user_id", LongType(), nullable=False),
    StructField("user_session", StringType(), nullable=False),
])

# conf = SparkConf() \
#     .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
#     .set("spark.kryo.registrationRequired", "true") \
#     .set("spark.kryo.registrator", "org.apache.spark.serializer.KryoRegistrator") \
#     .set("spark.kryo.classesToRegister", "org.apache.spark.sql.types.TimestampType")

def transform_and_load(input_path, keyspace):
    """
    Transform: Thực hiện các phép biến đổi và Load vào Cassandra
    """
    spark = SparkSession.builder \
    .appName("Transform and Load") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0") \
    .getOrCreate()
    # .config(conf=conf) \

    # Đọc dữ liệu từ bước Extract
    df_cleaned = spark.read.parquet(input_path)
    df_raw = spark.read.parquet(raw_data_path)

    # Transform
    df_1_1 = tf.product_events_and_purchase_conversion(df_raw)
    df_1_2 = tf.user_purchase_gap(df_raw)
    df_1_3 = tf.customer_rfm(df_raw)
    df_2_1 = tf.top_brand_by_category(df_cleaned)
    df_2_2 = tf.top_product_by_brand(df_cleaned)
    df_3_1 = tf.category_conversion(df_cleaned)
    df_3_2 = tf.shopping_behavior(df_cleaned)
    df_4_1 = tf.analyze_peak_access_hours(df_raw)

    # Load
    def load_to_cassandra(df, table):
        try:
            print(f"Attempting to write to {keyspace}.{table}")
            print(f"Row count: {df.count()}")
            df.write \
                .format("org.apache.spark.sql.cassandra") \
                .mode("append") \
                .options(keyspace=keyspace, table=table) \
                .save()
            print(f"Successfully wrote to {keyspace}.{table}")
        except Exception as e:
            print(f"Error writing to {keyspace}.{table}: {str(e)}")


    load_to_cassandra(df_1_1, "product_events_and_purchase_conversion")
    load_to_cassandra(df_1_2, "user_purchase_gap")
    load_to_cassandra(df_1_3, "customer_rfm")
    load_to_cassandra(df_2_1, "top_brand_by_category")
    load_to_cassandra(df_2_2, "top_product_by_brand")
    load_to_cassandra(df_3_1, "category_conversion")
    load_to_cassandra(df_3_2, "shopping_behavior")
    load_to_cassandra(df_4_1, "analyze_peak_access_hours")

    spark.stop()

if __name__ == "__main__":
    input_path = "hdfs://namenode:8020/extracted_data/year=" + year + "/month=" + month + "/cleaned_data/" + run_time
    keyspace = "bigdata"
    transform_and_load(input_path, keyspace)
