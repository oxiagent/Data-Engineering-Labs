from pyspark.sql import SparkSession, functions as F, types as T

# SparkSession: connect to Cassandra 
spark = (
    SparkSession.builder
    .appName("AmazonReviews_Spark_Cassandra")
    .config("spark.cassandra.connection.host", "host.docker.internal")
    .config("spark.cassandra.connection.port", "9043")
    .getOrCreate()
)
spark.conf.set("spark.sql.shuffle.partitions", "200")

# Schema CSV 
schema = T.StructType([
    T.StructField("marketplace",        T.StringType(),  True),
    T.StructField("customer_id",        T.LongType(),    True),
    T.StructField("review_id",          T.StringType(),  True),
    T.StructField("product_id",         T.StringType(),  True),
    T.StructField("product_parent",     T.LongType(),    True),
    T.StructField("product_title",      T.StringType(),  True),
    T.StructField("product_category",   T.StringType(),  True),
    T.StructField("star_rating",        T.IntegerType(), True),
    T.StructField("helpful_votes",      T.IntegerType(), True),
    T.StructField("total_votes",        T.IntegerType(), True),
    T.StructField("vine",               T.IntegerType(), True),
    T.StructField("verified_purchase",  T.IntegerType(), True),
    T.StructField("review_headline",    T.StringType(),  True),
    T.StructField("review_body",        T.StringType(),  True),
    T.StructField("review_date",        T.StringType(),  True),
])

# path to db
import os

input_path = os.environ.get("INPUT_PATH", "/app/amazon_reviews.csv")

df = (spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(input_path))


# Cleaning
critical = ["review_id", "product_id", "customer_id", "star_rating", "review_date"]
df = df.dropna(subset=critical)

df = (df
      .withColumn("review_ts", F.to_timestamp("review_date"))
      .withColumn("review_dt", F.to_date("review_ts"))
      .filter(F.col("review_dt").isNotNull())
)

# Additional fields
df = df.withColumn("period_ym", F.date_format("review_dt", "yyyy-MM"))

# reviews_by_product 
reviews_by_product = (
    df.select(
        "product_id",
        F.col("review_dt").cast("date").alias("review_date"),
        "review_id",
        "customer_id",
        "star_rating",
        "verified_purchase",
        "review_headline",
        "review_body",
    )
)

# reviews_by_product_and_rating 
reviews_by_product_and_rating = (
    df.select(
        "product_id",
        "star_rating",
        F.col("review_dt").cast("date").alias("review_date"),
        "review_id",
        "customer_id",
        "verified_purchase",
        "review_headline",
        "review_body",
    )
)

# reviews_by_customer 
reviews_by_customer = (
    df.select(
        "customer_id",
        F.col("review_dt").cast("date").alias("review_date"),
        "review_id",
        "product_id",
        "star_rating",
        "verified_purchase",
        "review_headline",
        "review_body",
    )
)

# top_products_by_period 
top_products_by_period = (
    df.groupBy("period_ym", "product_id")
      .agg(F.count(F.lit(1)).alias("review_count"))
      .select("period_ym", "review_count", "product_id")
)

# top_customers_verified_by_period 
top_customers_verified_by_period = (
    df.filter(F.col("verified_purchase") == 1)
      .groupBy("period_ym", "customer_id")
      .agg(F.count(F.lit(1)).alias("verified_review_count"))
      .select("period_ym", "verified_review_count", "customer_id")
)

# top_haters_by_period 
top_haters_by_period = (
    df.filter(F.col("star_rating").isin(1, 2))
      .groupBy("period_ym", "customer_id")
      .agg(F.count(F.lit(1)).alias("low_rating_count"))
      .select("period_ym", "low_rating_count", "customer_id")
)

# top_backers_by_period (4-5 зірок) 
top_backers_by_period = (
    df.filter(F.col("star_rating").isin(4, 5))
      .groupBy("period_ym", "customer_id")
      .agg(F.count(F.lit(1)).alias("high_rating_count"))
      .select("period_ym", "high_rating_count", "customer_id")
)

# Insert into Cassandra 
def write_cassandra(df_out, table, mode="append"):
    (df_out.write
        .format("org.apache.spark.sql.cassandra")
        .mode(mode)
        .option("keyspace", "amazon")
        .option("table", table)
        .save())

# Write
write_cassandra(reviews_by_product,                "reviews_by_product")
write_cassandra(reviews_by_product_and_rating,     "reviews_by_product_and_rating")
write_cassandra(reviews_by_customer,               "reviews_by_customer")
write_cassandra(top_products_by_period,            "top_products_by_period")
write_cassandra(top_customers_verified_by_period,  "top_customers_verified_by_period")
write_cassandra(top_haters_by_period,              "top_haters_by_period")
write_cassandra(top_backers_by_period,             "top_backers_by_period")

spark.stop()
