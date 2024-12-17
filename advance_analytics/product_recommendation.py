# Dự đoán hành động tiếp theo của người dùng dựa trên các sự kiện trước đó (event_type) và các thuộc tính sản phẩm mà họ đã tương tác.
# Ý tưởng:
### 1. Mã hóa event_type (các loại hành động như view, cart, purchase) thành chỉ số.
### 2. Sử dụng các cột như category_id, brand, price, và event_time để làm đặc trưng.
### 3. Nhãn (label): event_type của sự kiện tiếp theo trong chuỗi hành động của người dùng.
# Lợi ích:
### 1. Giúp dự đoán khả năng một người dùng sẽ mua hàng sau khi xem hoặc thêm vào giỏ hàng.
### 2. Ứng dụng trong việc cá nhân hóa và tiếp thị.

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, when, lit
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.ml.evaluation import RegressionEvaluator

# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("UserBehaviorPrediction") \
    .getOrCreate()

# Đọc dữ liệu
df = spark.read.csv("E:/bigdata/datasets/2019-Nov.csv", header=True, inferSchema=True)

# Xử lý giá trị NULL
df = df.fillna({"event_type": "unknown", "product_id": -1, "user_id": -1})

# Loại bỏ dữ liệu không hợp lệ
df = df.filter((df.user_id != -1) & (df.product_id != -1) & (df.event_type != "unknown"))

# Mã hóa event_type thành điểm số
df = df.withColumn("rating", when(col("event_type") == "view", lit(1))
                              .when(col("event_type") == "cart", lit(3))
                              .when(col("event_type") == "purchase", lit(5))
                              .otherwise(lit(0)))

# Chọn các cột cần thiết
interaction_df = df.select("user_id", "product_id", "rating")

# Tách dữ liệu thành train và test
train_data, test_data = interaction_df.randomSplit([0.8, 0.2], seed=42)

# Khởi tạo mô hình ALS
als = ALS(
    maxIter=10,
    regParam=0.1,
    userCol="user_id",
    itemCol="product_id",
    ratingCol="rating",
    coldStartStrategy="drop"
)

# Huấn luyện mô hình
als_model = als.fit(train_data)

# Đánh giá mô hình trên tập kiểm tra
predictions = als_model.transform(test_data)
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print(f"Root Mean Squared Error (RMSE): {rmse}")

# Lưu mô hình
als_model.write().overwrite().save("E:/BachKhoa/20241/BigData/HUST_IT4931/advance_analytics/models/als_product_recommendation")
print("Mô hình đã được lưu tại: E:/BachKhoa/20241/BigData/HUST_IT4931/advance_analytics/models/als_product_recommendation")

# Tải lại mô hình
loaded_model = ALSModel.load("E:/BachKhoa/20241/BigData/HUST_IT4931/advance_analytics/models/als_product_recommendation")
print("Mô hình đã được tải lại.")

# Đề xuất 5 sản phẩm cho mỗi người dùng từ mô hình đã tải
user_recommendations = loaded_model.recommendForAllUsers(5)

# Đề xuất 5 người dùng cho mỗi sản phẩm
product_recommendations = loaded_model.recommendForAllItems(5)

# Nổ mảng recommendations
user_recommendations_exploded = user_recommendations.withColumn("recommendation", explode(col("recommendations")))
user_recommendations_flat = user_recommendations_exploded.select(
    col("user_id"),
    col("recommendation.product_id").alias("product_id"),
    col("recommendation.rating").alias("rating")
)

# Ghi kết quả phẳng vào file CSV
user_recommendations_flat.write.mode("overwrite").csv("E:/BachKhoa/20241/BigData/HUST_IT4931/advance_analytics/results/user_recommendations.csv", header=True)

product_recommendations_exploded = product_recommendations.withColumn("recommendation", explode(col("recommendations")))
product_recommendations_flat = product_recommendations_exploded.select(
    col("product_id"),
    col("recommendation.user_id").alias("user_id"),
    col("recommendation.rating").alias("rating")
)
product_recommendations_flat.write.mode("overwrite").csv("E:/BachKhoa/20241/BigData/HUST_IT4931/advance_analytics/results/product_recommendations.csv", header=True)