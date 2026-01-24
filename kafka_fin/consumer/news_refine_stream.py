from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, IntegerType
from datetime import datetime, timezone

# 1. Spark 세션 (UTC 설정)
spark = (
    SparkSession.builder
    .appName("CryptoNewsRefining")
    .config("spark.sql.session.timeZone", "UTC")
    # Spark 3.5.1 + Scala 2.12용 Kafka 패키지
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
        "org.apache.hadoop:hadoop-aws:3.3.2"
    )
    # MinIO 설정 (docker-compose.yml 기준)
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

# 2. Kafka 구독
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")  # 도커 내부 포트
    .option("subscribe", "coindesk-news,coinness-breaking,coinness-newsroom")
    .option("startingOffsets", "latest")
    .load()
)

# 3. 표준 JSON 스키마 정의
json_schema = (
    StructType()
    .add("title", StringType())
    .add("link", StringType())
    .add("published_at", StringType())   # UTC ISO 문자열
    .add("content", StringType())
    .add("content_length", IntegerType())
    .add("crawled_at", StringType())     # UTC ISO 문자열
    .add("source", StringType())
    .add("news_type", StringType())      # coindesk / coinness-breaking / coinness-newsroom 등
)

# 4. Kafka value(JSON) 파싱
parsed_df = (
    df.selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), json_schema).alias("data"))
      .select("data.*")
)

# 5. 정제 및 파생 컬럼 생성
refined_df = (
    parsed_df
    # Coinness 계열만 특수 문구/이메일 제거
    .withColumn(
        "clean_content",
        when(
            col("source").like("coinness%"),
            regexp_replace(
                col("content"),
                r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
                r"|<저작권자.*?>"
                r"|\(.*?=.*?\).*?기자 ="
                r"|\[.*?기자\]",
                ""
            )
        ).otherwise(col("content"))
    )
    .withColumn("clean_content", trim(col("clean_content")))
    # ISO UTC 문자열 → 타임스탬프 (세션 타임존이 UTC라 그대로 UTC)
    .withColumn("published_at_ts", to_timestamp(col("published_at")))
    .withColumn("crawled_at_ts", to_timestamp(col("crawled_at")))
    # source + news_type 합치기
    .withColumn(
        "final_source",
        when(col("news_type").isNotNull(), concat_ws("-", col("source"), col("news_type")))
        .otherwise(col("source"))
    )
    # 파티션용 컬럼들 (UTC 기준)
    .withColumn("year", year(col("published_at_ts")))
    .withColumn("month", month(col("published_at_ts")))
    .withColumn("day", dayofmonth(col("published_at_ts")))
    .withColumn("hour", hour(col("published_at_ts")))
    .withColumn("min", minute(col("published_at_ts")))  # 분석용
)

# 6. 최종 컬럼 선택
final_df = refined_df.select(
    col("title"),
    col("link"),
    col("published_at_ts").alias("published_at"),
    col("clean_content").alias("content"),
    col("content_length"),
    col("crawled_at_ts").alias("crawled_at"),
    col("final_source").alias("source"),
    # 파티션 컬럼들
    "year", "month", "day", "hour", "min"
)

# 7. MinIO(S3) 적재 (year/month/day/hour 파티션)
query = (
    final_df.writeStream
    .partitionBy("year", "month", "day", "hour")
    .format("parquet")
    .option("path", "s3a://news-lake/refined/")
    .option("checkpointLocation", "s3a://news-lake/checkpoints/")
    .outputMode("append")
    .trigger(processingTime="5 minutes")  # 5분마다 배치로 적재
    .start()
)

query.awaitTermination()