from pyspark.sql import SparkSession, functions as F, types as T

# 1) SparkSession + Mongo URIs (контейнер на 27018)
spark = (
    SparkSession.builder
    .appName("AmazonReviews_Spark_Mongo")
    .config("spark.mongodb.write.connection.uri", "mongodb://root:rootpass@host.docker.internal:27018/?authSource=admin")
    .config("spark.mongodb.read.connection.uri",  "mongodb://root:rootpass@host.docker.internal:27018/?authSource=admin")
    .getOrCreate()
)

spark.conf.set("spark.sql.shuffle.partitions", "200")

# 2) Схема CSV
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
    T.StructField("verified_purchase",  T.IntegerType(), True),  # 0/1
    T.StructField("review_headline",    T.StringType(),  True),
    T.StructField("review_body",        T.StringType(),  True),
    T.StructField("review_date",        T.StringType(),  True),  # парсимо нижче
])

# 3) Зчитування CSV
input_path = "/app/amazon_reviews.csv"     
df = (
    spark.read
    .option("header", True)
    .schema(schema)
    .csv(input_path)
)

# 4) Чищення
critical = ["review_id", "product_id", "star_rating", "review_date"]
df = df.dropna(subset=critical)

# review_date 
df = (df
      .withColumn("review_ts", F.to_timestamp("review_date"))
      .withColumn("review_dt", F.to_date("review_ts"))
      .filter(F.col("review_dt").isNotNull())
)

# Лише верифіковані
dfv = df.filter(F.col("verified_purchase") == 1)

# 5) Агрегації
# 5.1 метрики по продукту
product_metrics = (dfv.groupBy("product_id")
    .agg(
        F.count(F.lit(1)).alias("review_count"),
        F.avg(F.col("star_rating").cast("double")).alias("avg_star_rating")
    )
    .withColumnRenamed("product_id", "_id")
)

# 5.2 к-сть перевірених відгуків по клієнту
customer_review_counts = (dfv.groupBy("customer_id")
    .agg(F.count(F.lit(1)).alias("verified_review_count"))
    .withColumnRenamed("customer_id", "_id")
)

# 5.3 місячні тренди по продукту
dfm = (dfv
    .withColumn("year",  F.year("review_dt"))
    .withColumn("month", F.month("review_dt"))
)
product_monthly_reviews = (dfm.groupBy("product_id", "year", "month")
    .agg(F.count(F.lit(1)).alias("monthly_review_count"))
)

# 6) Запис у Mongo
def write_mongo(df_out, collection, mode="overwrite"):
    (df_out.write
        .format("mongodb")
        .mode(mode)
        .option("database", "amazon")
        .option("collection", collection)
        .save())

write_mongo(product_metrics,        "product_metrics")
write_mongo(customer_review_counts, "customer_review_counts")
write_mongo(product_monthly_reviews,"product_monthly_reviews")

spark.stop()
