from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

import transformations as tf


def extract_data(spark, input_path, schema):
    """
    Extract: Đọc dữ liệu từ HDFS
    """
    return spark.read \
        .format("csv") \
        .schema(schema) \
        .load(input_path)

def clean_raw_data(df_raw):
    df_cleaned = df_raw.dropDuplicates() \
        .dropna(subset=["event_time", "product_id", "category_id", "price", "user_id", "user_session"]) \
        .withColumn("category_level_1", split(col("category_code"), "\\.")[0]) \
        .withColumn("category_level_2", split(col("category_code"), "\\.")[1]) \
        .withColumn("category_level_3", split(col("category_code"), "\\.")[2]) \
        .withColumn("category_level_4", split(col("category_code"), "\\.")[3]) \
        .drop("category_code")

    df_cleaned = df_cleaned.dropna(subset=["brand", "category_level_1"])
    return df_cleaned



def load_to_cassandra(df, keyspace, table):
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
    

def main():
    # Tạo SparkSession
    spark = SparkSession.builder \
    .appName("ETL with Spark and Cassandra") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0") \
    .getOrCreate()

    # Định nghĩa schema cho dữ liệu
    schema = StructType(
        [
            StructField("event_time", TimestampType(), nullable=False),
            StructField("event_type", StringType(), nullable=False),
            StructField("product_id", LongType(), nullable=False),
            StructField("category_id", LongType(), nullable=False),
            StructField("category_code", StringType(), nullable=True),
            StructField("brand", StringType(), nullable=True),
            StructField("price", DoubleType(), nullable=False),
            StructField("user_id", LongType(), nullable=False),
            StructField("user_session", StringType(), nullable=False),
        ]
    )

    # Đường dẫn dữ liệu đầu vào
    input_path = "hdfs://namenode:8020/input_data/data_1m/datasets"

    # Bước Extract
    df_raw = extract_data(spark, input_path, schema)
    df_cleaned = clean_raw_data(df_raw)

    # Bước Transform
    df_1_1 = tf.product_events_and_purchase_conversion(df_raw)
    df_1_2 = tf.user_purchase_gap(df_raw)
    df_1_3 = tf.customer_rfm(df_raw)

    df_2_1 = tf.top_brand_by_category(df_cleaned)
    df_2_2 = tf.top_product_by_brand(df_cleaned)

    df_3_1 = tf.category_conversion(df_cleaned)
    df_3_2 = tf.shopping_behavior(df_cleaned)

    # Bước Load
    keyspace = "bigdata"

    load_to_cassandra(df_1_1, keyspace, "product_events_and_purchase_conversion")
    load_to_cassandra(df_1_2, keyspace, "user_purchase_gap")
    load_to_cassandra(df_1_3, keyspace, "customer_rfm")
    load_to_cassandra(df_2_1, keyspace, "top_brand_by_category")
    load_to_cassandra(df_2_2, keyspace, "top_product_by_brand")
    load_to_cassandra(df_3_1, keyspace, "category_conversion")
    load_to_cassandra(df_3_2, keyspace, "shopping_behavior")


    # Dừng SparkSession
    spark.stop()

if __name__ == "__main__":
    main()
