import os
import sys
from dotenv import load_dotenv

load_dotenv()

MINIO_USER = os.getenv("MINIO_USER")
MINIO_PASS = os.getenv("MINIO_PASS")
CATALOG_URI = os.getenv("CATALOG_URI")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")

TOPIC_NAME = "biometrics_stream"
DLQ_TOPIC_NAME = "biometrics_stream_dlq"
ICEBERG_TABLE = "lakehouse.db.scored_biometric_events"
CHECKPOINT_PATH = "s3a://warehouse/checkpoints/biometrics_stream_worker"

REQUIRED_ENV_VARS = {
    "MINIO_USER": MINIO_USER,
    "MINIO_PASS": MINIO_PASS,
    "CATALOG_URI": CATALOG_URI,
    "MINIO_ENDPOINT": MINIO_ENDPOINT,
    "KAFKA_BOOTSTRAP_SERVERS": KAFKA_BOOTSTRAP_SERVERS,
}

missing_vars = [key for key, value in REQUIRED_ENV_VARS.items() if not value]
if missing_vars:
    print(f"[ERROR] Missing mandatory environment variables: {', '.join(missing_vars)}.", file=sys.stderr)
    sys.exit(1)