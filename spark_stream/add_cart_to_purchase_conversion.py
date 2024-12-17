# Mục tiêu: Phân tích hành động "Add to Cart" có thể dẫn đến "Purchase". 
# Dữ liệu này giúp theo dõi tỷ lệ chuyển đổi và tìm hiểu hành vi người dùng.

# Ứng dụng: Tính toán tỷ lệ chuyển đổi từ "Add to Cart" thành "Purchase", có thể giúp tối ưu hóa chiến dịch bán hàng.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, FloatType

# Khởi tạo Spark session
spark = SparkSession.builder.appName("AddToCartToPurchaseConversion").getOrCreate()

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

# Lọc hành động "Add to Cart" và "Purchase"
add_to_cart = df_with_time.filter(col("data.event_type") == "cart").select("data.user_id", "data.product_id", "event_time")
purchase = df_with_time.filter(col("data.event_type") == "purchase").select("data.user_id", "data.product_id", "event_time")

# Join hành động "Add to Cart" và "Purchase" dựa trên user_id và product_id trong một cửa sổ thời gian
conversion = add_to_cart.join(
    purchase,
    (add_to_cart.user_id == purchase.user_id) & 
    (add_to_cart.product_id == purchase.product_id) & 
    (purchase.event_time >= add_to_cart.event_time) & 
    (purchase.event_time <= add_to_cart.event_time + expr("INTERVAL 1 HOUR")),
    "inner"
).select(
    add_to_cart.user_id.alias("user_id"),
    add_to_cart.product_id.alias("product_id"),
    add_to_cart.event_time.alias("add_to_cart_time"),
    purchase.event_time.alias("purchase_time")
)

# Tính tỷ lệ chuyển đổi
conversion_count = conversion.groupBy("product_id").agg(count("*").alias("conversion_count"))
add_to_cart_count = add_to_cart.groupBy("product_id").agg(count("*").alias("add_to_cart_count"))

# Join số liệu để tính tỷ lệ chuyển đổi
conversion_rate = add_to_cart_count.join(
    conversion_count, "product_id", "left_outer"
).withColumn(
    "conversion_rate", col("conversion_count") / col("add_to_cart_count")
)

# Ghi kết quả ra console
query = conversion_rate.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
