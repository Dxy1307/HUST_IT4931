# extract_task.py
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime

run_time = "{:%Y%m%d}".format(datetime.now())
raw_data_path = "hdfs://namenode:8020/extracted_data/raw_data/" + run_time

def extract_and_clean(input_path, output_path):
    """
    Extract: Đọc dữ liệu từ HDFS và lưu dạng Parquet sau khi làm sạch.
    """
    spark = SparkSession.builder.appName("Extract and Clean Data").getOrCreate()
    
    # Đọc dữ liệu
    df_raw = spark.read.format("json").load(input_path)
    print(df_raw.show())
    print("Read Data!")

    # Làm sạch dữ liệu
    df_cleaned = df_raw.dropDuplicates() \
        .dropna(subset=["event_time", "product_id", "category_id", "price", "user_id", "user_session"]) \
        .withColumn("category_level_1", split(col("category_code"), "\\.")[0]) \
        .withColumn("category_level_2", split(col("category_code"), "\\.")[1]) \
        .withColumn("category_level_3", split(col("category_code"), "\\.")[2]) \
        .withColumn("category_level_4", split(col("category_code"), "\\.")[3]) \
        .drop("category_code") \
        .dropna(subset=["brand", "category_level_1"])

    # Lưu dữ liệu
    df_cleaned.write.parquet(output_path, mode="overwrite")
    df_raw.write.parquet(raw_data_path, mode="overwrite")
    print("Write data to hdfs!")

    print(df_cleaned.show())
    print(df_raw.show())


    spark.stop()

if __name__ == "__main__":
    input_path = "hdfs://namenode:8020/datasets/kafka_data/" + run_time
    output_path = "hdfs://namenode:8020/extracted_data/" + run_time
    extract_and_clean(input_path, output_path)
