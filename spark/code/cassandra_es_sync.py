from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct
from pyspark.sql.utils import AnalysisException

def sync_cassandra_to_elasticsearch(spark, keyspace, table):
    try:
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

    except AnalysisException as e:
        print(f"Lỗi khi đọc dữ liệu từ Cassandra: {str(e)}")
    except Exception as e:
        print(f"Lỗi không xác định khi đồng bộ {keyspace}.{table}: {str(e)}")

def main():
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

    sync_mappings = [
        {"keyspace": "bigdata", "table": "product_events_and_purchase_conversion"},
        {"keyspace": "bigdata", "table": "user_purchase_gap"},
        {"keyspace": "bigdata", "table": "customer_rfm"},
        {"keyspace": "bigdata", "table": "top_brand_by_category"},
        {"keyspace": "bigdata", "table": "top_product_by_brand"},
        {"keyspace": "bigdata", "table": "category_conversion"},
        {"keyspace": "bigdata", "table": "shopping_behavior"},
        {"keyspace": "bigdata", "table": "analyze_peak_access_hours"}
    ]

    for mapping in sync_mappings:
        sync_cassandra_to_elasticsearch(spark, mapping["keyspace"], mapping["table"])

if __name__ == "__main__":
    main()
