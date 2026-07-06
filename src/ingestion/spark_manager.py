import os
import sys
from pyspark.sql import SparkSession
from src.ingestion.config import MINIO_USER, MINIO_PASS, CATALOG_URI, MINIO_ENDPOINT


def create_streaming_spark_session():
    print("[INFO] Building SparkSession with cloud-scale dependencies...")

    spark_packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.apache.spark:spark-avro_2.12:3.5.0",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
    ]

    driver_memory = os.getenv("SPARK_DRIVER_MEMORY", "512m")
    executor_memory = os.getenv("SPARK_EXECUTOR_MEMORY", "512m")
    driver_overhead = os.getenv("SPARK_DRIVER_MEMORY_OVERHEAD", "128m")
    executor_overhead = os.getenv("SPARK_EXECUTOR_MEMORY_OVERHEAD", "128m")
    local_threads = os.getenv("SPARK_LOCAL_THREADS", "2")

    builder = (
        SparkSession.builder
        .appName("BiometricRealtimeIngestionWorker")
        .master(f"local[{local_threads}]")
        .config("spark.driver.memory", driver_memory)
        .config("spark.executor.memory", executor_memory)
        .config("spark.driver.memoryOverhead", driver_overhead)
        .config("spark.executor.memoryOverhead", executor_overhead)
        .config("spark.jars.packages", ",".join(spark_packages))
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
        .config("spark.sql.catalog.lakehouse.uri", CATALOG_URI)
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://warehouse/")
        .config("spark.sql.catalog.lakehouse.s3.endpoint", MINIO_ENDPOINT)
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        .config("spark.sql.catalog.lakehouse.s3.access-key-id", MINIO_USER)
        .config("spark.sql.catalog.lakehouse.s3.secret-access-key", MINIO_PASS)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    print("[SUCCESS] SparkSession successfully created inside Docker Environment.")
    return spark

def initialize_iceberg_table(spark, table_name):
    print(f"[INFO] Checking and initializing Iceberg Lakehouse structures for {table_name}...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.db")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            event_id STRING,
            user_id STRING,
            track_id STRING,
            timestamp LONG,
            heart_rate DOUBLE,
            hrv_rmssd DOUBLE,
            eda_microsiemens DOUBLE,
            motion_status STRING,
            event_time TIMESTAMP,
            stress_score STRING
        )
        USING iceberg
        PARTITIONED BY (days(event_time))
    """)
    print(f"[SUCCESS] Apache Iceberg table '{table_name}' is ready for writes.")