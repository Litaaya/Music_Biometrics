import os
import sys
from dotenv import load_dotenv
from pyspark.sql import SparkSession

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
load_dotenv()

MINIO_USER = os.getenv("MINIO_USER")
MINIO_PASS = os.getenv("MINIO_PASS")
CATALOG_URI = os.getenv("CATALOG_URI")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")


def create_streaming_spark_session():
    print("[INFO] Building SparkSession with cloud-scale dependencies...")

    spark_packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.apache.spark:spark-avro_2.12:3.5.0",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
        "org.apache.hadoop:hadoop-aws:3.3.4"
    ]

    builder = SparkSession.builder \
        .appName("BiometricRealtimeIngestionWorker") \
        .config("spark.jars.packages", ",".join(spark_packages))

    # --- ICEBERG REST CATALOG CONFIGURATIONS ---
    builder = builder \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.db", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.db.catalog-impl", "org.apache.iceberg.rest.RESTCatalog") \
        .config("spark.sql.catalog.db.uri", CATALOG_URI) \
        .config("spark.sql.catalog.db.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.db.warehouse", "s3a://warehouse/") \
        .config("spark.sql.catalog.db.s3.endpoint", MINIO_ENDPOINT) \
        .config("spark.sql.catalog.db.s3.path-style-access", "true") \
        .config("spark.sql.catalog.db.s3.access-key-id", MINIO_USER) \
        .config("spark.sql.catalog.db.s3.secret-access-key", MINIO_PASS)

    # --- HADOOP GLOBAL S3A FILE SYSTEM CONFIGURATIONS ---
    builder = builder \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("[SUCCESS] SparkSession successfully created inside Docker Environment.")
    return spark


def test_kafka_connectivity(spark):
    print(f"[INFO] Attempting connection probe to Kafka Broker at: {KAFKA_BOOTSTRAP_SERVERS}")
    try:
        df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", "biometrics_stream") \
            .option("startingOffsets", "earliest") \
            .option("endingOffsets", "latest") \
            .load()

        print(f"[SUCCESS] Connection verified! Sample schema retrieved from Kafka:")
        df.printSchema()
    except Exception as e:
        print(f"[ERROR] Connection to Kafka failed: {e}", file=sys.stderr)


if __name__ == "__main__":
    spark_session = create_streaming_spark_session()
    test_kafka_connectivity(spark_session)