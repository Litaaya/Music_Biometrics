import os
import sys
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SAMPLE_DATA_DIR = os.path.join(PROJECT_ROOT, "sample_data")
USER_SOURCE = os.path.join(SAMPLE_DATA_DIR, "user_profiles.parquet")
MUSIC_SOURCE = os.path.join(SAMPLE_DATA_DIR, "music_metadata.parquet")

MINIO_USER = os.getenv("MINIO_USER")
MINIO_PASS = os.getenv("MINIO_PASS")
CATALOG_URI = os.getenv("CATALOG_URI")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")

REQUIRED_ENV_VARS = {
    "MINIO_USER": MINIO_USER,
    "MINIO_PASS": MINIO_PASS,
    "CATALOG_URI": CATALOG_URI,
    "MINIO_ENDPOINT": MINIO_ENDPOINT
}

missing_vars = [key for key, value in REQUIRED_ENV_VARS.items() if not value]
if missing_vars:
    print(f"[ERROR] Missing mandatory environment variables: {', '.join(missing_vars)}.", file=sys.stderr)
    sys.exit(1)