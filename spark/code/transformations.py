from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

import user_defined_function as UDF

# Các hàm biến đổi dữ liệu
# 1. Phân tích dữ liệu mua hàng

# 1.1. Sắp xếp các sản phẩm theo số lượng events, tỷ lệ mua hàng sau khi xem hoặc thêm sản phẩm vào giỏ
def product_events_and_purchase_conversion(df):
# Tính toán sự kiện theo sản phẩm
    df_count_event_by_product = df.groupBy("product_id", "event_type").count() \
        .groupBy("product_id") \
        .pivot("event_type", ["view", "cart", "purchase"]) \
        .sum("count") \
        .fillna(0)

    # Thêm cột tổng số sự kiện và tỷ lệ chuyển đổi
    df_count_event_by_product = df_count_event_by_product.withColumn("total_events", 
                                                                     col("view") + col("cart") + col("purchase"))
    df_count_event_by_product = df_count_event_by_product.withColumn("purchase_conversion", 
                                                                     round((col("purchase") / (col("view") + col("cart"))) * 100, 2))
    # Sắp xếp theo tổng số sự kiện
    df_count_event_by_product = df_count_event_by_product.sort("total_events", ascending=False)
    return df_count_event_by_product




# 1.2. Tính khoảng thời gian trung bình giữa các lần mua hàng của từng người dùng
def user_purchase_gap(df):
    convert_time_udf = udf(UDF.time__seconds_to_string, StringType())

    user_window = Window.partitionBy("user_id").orderBy(col("event_time"))

    # Tính khoảng thời gian trung bình giữa các lần mua hàng
    df_avg_diff_time = df.filter(col("event_type") == "purchase") \
        .withColumn("prev_event_time", lag(col("event_time")).over(user_window)) \
        .withColumn(
            "diff_time", 
            (unix_timestamp(col("event_time")) - unix_timestamp(col("prev_event_time"))).cast("double")
        ) \
        .groupBy("user_id").agg(
            avg("diff_time").alias("avg_diff_time"),
            count("*").alias("number_of_purchases"),
            max(col("event_time")).alias("last_purchase_date")
            ) \
        .filter(col("avg_diff_time").isNotNull()) \
        .withColumn("avg_diff_time", convert_time_udf(col("avg_diff_time")))

    return df_avg_diff_time.orderBy(col("number_of_purchases").desc())




# 1.3. Phân loại khách hàng theo phương pháp RFM Analysis (Recency, Frequency, Monetary)
def customer_rfm(df):
    # Tính tổng tiền mà mỗi khách hàng đã mua
    df_customer_total_revenue = df.filter(col("event_type") == "purchase") \
        .groupBy(col("user_id")) \
        .agg(round(sum(col("price")), 2).alias("total_revenue")) \
        .orderBy(col("total_revenue").desc())
    
    rfm_udf = udf(UDF.classify_rfm, StringType())

    # join với bảng phân tích thời gian mua hàng
    df_classify_customer = df_customer_total_revenue.join(user_purchase_gap(df), on="user_id", how="inner")

    last_date = lit("2019-11-30 23:59:59")

    df_classify_customer = df_classify_customer.withColumn("recency", datediff(last_date, col("last_purchase_date"))) \
                                            .withColumn("customer_segment", rfm_udf(col("recency"), col("number_of_purchases"), col("total_revenue")))

    return df_classify_customer.selectExpr(
        "user_id", 
        "recency", 
        "number_of_purchases as frequency", 
        "total_revenue as monetary", 
        "customer_segment"
    ).orderBy(col("monetary").desc())




# 2. Phân tích các nhãn hiệu và danh mục

# 2.1. Top 3 nhãn hiệu có doanh số cao nhất theo từng phân loại cấp 1
def top_brand_by_category(df_not_null):
    df_revenue_leaderboard = df_not_null.filter(col("event_type") == "purchase")

    df_revenue_leaderboard = df_revenue_leaderboard.groupBy("category_level_1", "brand") \
        .agg(round(sum("price"), 2).alias("total_sales"), 
             count("*").alias("number_of_purchases"))

    windowSpec = Window.partitionBy("category_level_1").orderBy(col("total_sales").desc())

    df_revenue_leaderboard = df_revenue_leaderboard\
            .withColumn("rank", rank().over(windowSpec)) \
            .withColumn("total_sales", format_number(col("total_sales"), 0))

    return df_revenue_leaderboard.filter(col("rank") <= 3)




# 2.2. Các sản phẩm bán chạy nhất theo từng nhãn hiệu
def top_product_by_brand(df_not_null):
    best_sale_window = Window.partitionBy("brand").orderBy(col("number_of_purchases").desc(), col("revenue").desc())

    df_best_sale_per_brand = df_not_null.filter(col("event_type") == "purchase")

    df_best_sale_per_brand = df_best_sale_per_brand.groupBy("brand", "product_id", "price")\
        .agg(
            round(sum("price"), 2).alias("revenue"), 
            count("*").alias("number_of_purchases")
        ) \
        .withColumn("position", rank().over(best_sale_window)) \

    return df_best_sale_per_brand.filter(col("position") == 1) \
                        .filter(col("number_of_purchases") > 100) \
                        .drop("position") \

    


# 3. Phân tích hành vi mua sắm của người dùng

# 3.1. Tính toán tỷ lệ chuyển đổi từ xem sản phẩm sang mua hàng theo từng danh mục
def category_conversion(df_not_null):
    df_view_purchase_conversion_per_category = df_not_null.groupBy("product_id", "category_level_1", "event_type") \
    .count() \
    .groupBy("product_id", "category_level_1") \
    .pivot("event_type", ["view", "purchase"]) \
    .sum("count")

    df_view_purchase_conversion_per_category = df_view_purchase_conversion_per_category \
        .withColumn("purchase_conversion_rate", when(col("view") > 0, col("purchase") / col("view")).otherwise(0)) \
        .groupBy("category_level_1") \
        .agg(
            sum("view").alias("total_views"),
            sum("purchase").alias("total_purchases"),
            round(avg("purchase_conversion_rate") * 100, 2).alias("avg_conversion_rate (%)"),
        ) \
        .orderBy(col("avg_conversion_rate (%)").desc())\

    return df_view_purchase_conversion_per_category



# 3.2. Thống kê hành vi mua sắm của khách hàng theo ngày trong tháng
def shopping_behavior(df):
    df_day = df.withColumn("date", to_date(col("event_time")))

    df_day = df_day.withColumn("day", dayofmonth(col("date"))) \
        .groupBy("day", "event_type") \
        .agg(count("*").alias("number_of_interactions")) \
        .groupBy("day") \
        .pivot("event_type", ["view", "cart", "purchase"]) \
        .sum("number_of_interactions") \
        .sort(col("day").asc()) \
        .fillna(0)

    return df_day



