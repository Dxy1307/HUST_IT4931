# Mục tiêu: Phân tích hành động "Add to Cart" có thể dẫn đến "Purchase". 
# Dữ liệu này giúp theo dõi tỷ lệ chuyển đổi và tìm hiểu hành vi người dùng.

# Ứng dụng: Tính toán tỷ lệ chuyển đổi từ "Add to Cart" thành "Purchase", có thể giúp tối ưu hóa chiến dịch bán hàng.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType
import logging

# Cấu hình log
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Khởi tạo Spark session
spark = SparkSession.builder \
        .appName("AddToCartToPurchaseConversion") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
        .getOrCreate()

# Ẩn log không cần thiết
spark.sparkContext.setLogLevel("ERROR")

# Cấu hình log cho Spark
log4j = spark._jvm.org.apache.log4j
logger_spark = log4j.LogManager.getLogger("org.apache.spark")

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

try: 

    # Đọc dữ liệu từ Kafka
    kafka_stream = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "test") \
        .load()

    # Giải mã dữ liệu từ Kafka
    parse_df = kafka_stream.select(from_json(col("value").cast("string"), schema).alias("data"))

    # Chuyển đổi cột event_time thành timestamp
    df_with_time = parse_df.withColumn("event_time", col("data.event_time").cast("timestamp"))

    df_with_time = df_with_time.withWatermark("event_time", "10 minutes")

    # Lọc hành động "Add to Cart" và "Purchase"
    add_to_cart = df_with_time \
                    .filter(col("data.event_type") == "cart") \
                    .select("data.user_id", "data.product_id", col("event_time").alias("add_to_cart_time"))
    purchase = df_with_time \
                .filter(col("data.event_type") == "purchase") \
                .select("data.user_id", "data.product_id", col("event_time").alias("purchase_time"))      

    # Alias các DataFrame trước khi join để tránh mẫu thuẫn về tên cột
    add_to_cart_alias = add_to_cart.alias("add_to_cart")
    purchase_alias = purchase.alias("purchase")

    def debug_add_to_cart(batch_df, batch_id):
        print(f"Batch ID: {batch_id} - Add to Cart Data")
        batch_df.show(truncate=False)

    def debug_purchase(batch_df, batch_id):
        print(f"Batch ID: {batch_id} - Purchase Data")
        batch_df.show(truncate=False)  

    add_to_cart_alias.writeStream.foreachBatch(debug_add_to_cart).start()
    purchase_alias.writeStream.foreachBatch(debug_purchase).start()

    # Thêm watermark cho DataFrame sau khi join để tránh lỗi khi sử dụng append
    # add_to_cart_alias = add_to_cart_alias.withWatermark("add_to_cart_time", "10 minutes")
    # purchase_alias = purchase_alias.withWatermark("purchase_time", "10 minutes")

    # Join hành động "Add to Cart" và "Purchase" dựa trên user_id và product_id trong một cửa sổ thời gian
    conversion = add_to_cart_alias.join(
        purchase_alias,
        (add_to_cart_alias["user_id"] == purchase_alias["user_id"]) & 
        (add_to_cart_alias["product_id"] == purchase_alias["product_id"]) & 
        (purchase_alias["purchase_time"] >= add_to_cart_alias["add_to_cart_time"]) & 
        (purchase_alias["purchase_time"] <= add_to_cart_alias["add_to_cart_time"] + expr("INTERVAL 1 HOUR")),
        "inner"
    ).select(
        add_to_cart_alias["user_id"].alias("user_id"),
        add_to_cart_alias["product_id"].alias("product_id"),
        add_to_cart_alias["add_to_cart_time"].alias("add_to_cart_time"),
        purchase_alias["purchase_time"].alias("purchase_time")
    )

    def process_batch(batch_df, batch_id):
        logger.info(f"Processing batch {batch_id}")
        batch_df.show(truncate=False)  # In ra các giá trị trong mỗi batch

    # Áp dụng watermark cho DataFrame conversion trước khi thực hiện aggregation
    # conversion = conversion.withWatermark("add_to_cart_time", "10 minutes") \
    #                         .withWatermark("purchase_time", "10 minutes")

    # Tính tỷ lệ chuyển đổi
    # conversion_count = conversion.groupBy("product_id").agg(count("*").alias("conversion_count"))
    # add_to_cart_count = add_to_cart_alias.groupBy("product_id").agg(count("*").alias("add_to_cart_count"))

    # Join số liệu để tính tỷ lệ chuyển đổi
    # conversion_rate = add_to_cart_count.join(
    #     conversion_count, "product_id", "left_outer"
    # ).withColumn(
    #     "conversion_rate", col("conversion_count") / col("add_to_cart_count")
    # )

    # Ghi kết quả ra console
    # query = conversion_rate.writeStream \
    #     .outputMode("update") \
    #     .format("console") \
    #     .option("truncate", "false") \
    #     .start()

    checkpoint_dir = 'E:/BachKhoa/20241/BigData/HUST_IT4931/checkpoint/add_to_cart_to_purchase_conversion'

    query = conversion.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("checkpointLocation", checkpoint_dir) \
        .foreachBatch(process_batch) \
        .start()

    query.awaitTermination()

except Exception as e:
    logger.error(f"An error occurred: {str(e)}")
    logger_spark.error(f"Spark error: {str(e)}")