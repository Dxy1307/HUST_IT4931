# Phân nhóm người dùng dựa trên hành vi tương tác với sản phẩm (event_type, category_id, brand, price) để hiểu rõ hơn về các nhóm khách hàng.
# Lợi ích:
### 1. Phân khúc khách hàng để xây dựng chiến lược kinh doanh.
### 2. Tùy chỉnh các chương trình khuyến mãi hoặc ưu đãi cho từng nhóm khách hàng.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, avg, sum, lit
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import BisectingKMeans
import matplotlib.pyplot as plt

# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("UserClustering") \
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

# Tính tổng số lần mỗi người dùng tương tác với sản phẩm theo từng event_type
user_interaction = df.groupBy("user_id").agg(
    count(when(col("event_type") == "view", 1)).alias("view_count"),
    count(when(col("event_type") == "cart", 1)).alias("cart_count"),
    count(when(col("event_type") == "purchase", 1)).alias("purchase_count"),
    sum("rating").alias("total_rating"),
    avg("price").alias("avg_price"),
    sum("price").alias("total_spent")
)

# Tạo đặc trưng từ dữ liệu trên
assembler = VectorAssembler(
    inputCols=["view_count", "cart_count", "purchase_count", "total_rating", "avg_price", "total_spent"],
    outputCol="features"
)

user_features = assembler.transform(user_interaction)

# Áp dụng thuật toán BisectingKMeans để phân nhóm người dùng
bisecting_kmeans = BisectingKMeans(k=4, seed=1, featuresCol="features", predictionCol="cluster")

# Fit BisectingKMeans model
model = bisecting_kmeans.fit(user_features)

# Dự đoán phân nhóm cho người dùng
user_clusters = model.transform(user_features)

# Hiển thị các nhóm người dùng
user_clusters.select("user_id", "cluster").show(5)

# Tính toán các trung bình cho mỗi nhóm
cluster_summary = user_clusters.groupBy("cluster").agg(
    avg("view_count").alias("avg_view_count"),
    avg("cart_count").alias("avg_cart_count"),
    avg("purchase_count").alias("avg_purchase_count"),
    avg("total_rating").alias("avg_total_rating"),
    avg("avg_price").alias("avg_avg_price"),
    avg("total_spent").alias("avg_total_spent")
)

cluster_summary.show()

# Trực quan hóa kết quả phân nhóm (ví dụ: chỉ dùng 2 đặc trưng cho dễ trực quan hóa)
import pandas as pd

# Chuyển đổi dữ liệu sang pandas để trực quan hóa
user_clusters_data = user_clusters.select("user_id", "view_count", "cart_count", "purchase_count", "cluster").collect()
user_clusters_pd = pd.DataFrame(user_clusters_data, columns=["user_id", "view_count", "cart_count", "purchase_count", "cluster"])

# Vẽ đồ thị phân nhóm
plt.figure(figsize=(10, 6))
plt.scatter(user_clusters_pd['view_count'], user_clusters_pd['cart_count'], c=user_clusters_pd['cluster'], cmap='viridis', s=50)
plt.title('User Clusters (View Count vs Cart Count)')
plt.xlabel('View Count')
plt.ylabel('Cart Count')
plt.colorbar(label='Cluster')
plt.show()

# Lưu kết quả phân nhóm người dùng vào CSV
user_clusters.select("user_id", "cluster").write.csv("E:/BachKhoa/20241/BigData/HUST_IT4931/advance_analytics/results/user_clusters.csv", header=True)

# Kết quả hiển thị
### 1. Cluster 0 (tím): Người dùng ít tương tác, có thể cần khuyến mãi hoặc quảng cáo để thu hút họ.
### 2. Cluster 1 (xanh lá): Người dùng quan tâm sản phẩm (Cart Count cao) → có thể nhắm mục tiêu với các chương trình giảm giá để tăng mua hàng.
### 3. Cluster 2 (vàng): Người dùng chỉ xem nhiều (View Count cao) nhưng không mua → cần nhắm vào các chiến lược remarketing.
### 4. Cluster 3 (xanh lam): Người dùng có hành vi tương tác trung bình → có thể là nhóm khách hàng tiềm năng ổn định.