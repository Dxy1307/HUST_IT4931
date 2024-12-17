from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct

my_keyspace = "bigdata"

def sync_cassandra_to_elasticsearch(keyspace, table):
    # Tạo SparkSession
    spark = SparkSession.builder \
        .appName("Cassandra To Elasticsearch Synchronization") \
        .config("spark.jars.packages", 
                "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0,"
                "org.elasticsearch:elasticsearch-spark-30_2.12:8.12.0") \
                .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.cassandra.auth.username", "cassandra") \
        .config("spark.cassandra.auth.password", "cassandra") \
        .config("es.nodes", "elasticsearch") \
        .config("es.port", "9200") \
        .getOrCreate()

    # Đọc dữ liệu từ Cassandra
    df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table, keyspace=keyspace) \
        .load()

    # Ghi dữ liệu sang Elasticsearch
    df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.resource", f"{table}") \
        .option("es.nodes", "elasticsearch") \
        .option("es.port", "9200") \
        .mode("overwrite") \
        .save()

    print(f"Đã đồng bộ dữ liệu từ {keyspace}.{table} sang Elasticsearch")

def main():
    # Danh sách keyspace và table cần đồng bộ
    sync_mappings = [
        {"keyspace": my_keyspace, "table": "product_events_and_purchase_conversion"},
        {"keyspace": my_keyspace, "table": "user_purchase_gap"},
        {"keyspace": my_keyspace, "table": "customer_rfm"},
        {"keyspace": my_keyspace, "table": "top_brand_by_category"},
        {"keyspace": my_keyspace, "table": "top_product_by_brand"},
        {"keyspace": my_keyspace, "table": "category_conversion"},
        {"keyspace": my_keyspace, "table": "shopping_behavior"},
        {"keyspace": my_keyspace, "table": "analyze_peak_access_hours"}

    ]

    for mapping in sync_mappings:
        sync_cassandra_to_elasticsearch(
            mapping["keyspace"], 
            mapping["table"]
        )

if __name__ == "__main__":
    main()