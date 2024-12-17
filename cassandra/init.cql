DROP KEYSPACE IF EXISTS bigdata;

CREATE KEYSPACE bigdata WITH REPLICATION = {
  'class': 'SimpleStrategy', 
  'replication_factor': 3
};

USE bigdata;

CREATE TABLE product_events_and_purchase_conversion(
  "product_id" bigint PRIMARY KEY, 
  "view" bigint, 
  "cart" bigint, 
  "purchase" bigint, 
  "total_events" bigint, 
  "purchase_conversion" double
);

CREATE TABLE user_purchase_gap(
  "user_id" bigint PRIMARY KEY, 
  "avg_diff_time" text, 
  "number_of_purchases" bigint, 
  "last_purchase_date" timestamp
);

CREATE TABLE customer_rfm(
  "user_id" bigint PRIMARY KEY, 
  "recency" bigint, 
  "frequency" bigint, 
  "monetary" bigint, 
  "customer_segment" text
);

CREATE TABLE top_brand_by_category(
  "category_level_1" TEXT, 
  "brand" TEXT, 
  "total_sales" DOUBLE, 
  "number_of_purchases" BIGINT, 
  "rank" INT, 
  PRIMARY KEY ("category_level_1", "rank", "brand")
);

CREATE TABLE top_product_by_brand(
  "brand" TEXT PRIMARY KEY, 
  "product_id" BIGINT, 
  "price" DOUBLE, 
  "revenue" DOUBLE, 
  "number_of_purchases" BIGINT
);

CREATE TABLE category_conversion(
  "category_level_1" TEXT PRIMARY KEY, 
  "total_views" BIGINT, 
  "total_purchases" BIGINT, 
  "avg_conversion_rate (%)" DOUBLE
);

CREATE TABLE shopping_behavior(
  "day" INT PRIMARY KEY, 
  "view" BIGINT, 
  "cart" BIGINT, 
  "purchase" BIGINT
);

CREATE TABLE analyze_peak_access_hours(
  "hour_category" TEXT PRIMARY KEY,
  "total_access" BIGINT,
  "unique_users" BIGINT,
  "access_percentage" DOUBLE
);