import os
import sys
from pyiceberg.catalog import load_catalog

MINIO_USER = os.getenv("MINIO_USER")
MINIO_PASS = os.getenv("MINIO_PASS")
CATALOG_URI = os.getenv("CATALOG_URI")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")


def fetch_active_user_ids():
    try:
        catalog = load_catalog(
            "default",
            **{
                "type": "rest",
                "uri": CATALOG_URI,
                "s3.endpoint": MINIO_ENDPOINT,
                "s3.access-key-id": MINIO_USER,
                "s3.secret-access-key": MINIO_PASS,
                "s3.path-style-access": "true",
                "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO"
            }
        )

        table = catalog.load_table("db.user_profiles")
        arrow_table = table.scan(selected_fields=("user_id",)).to_arrow()
        user_ids = [str(uid) for uid in arrow_table["user_id"].to_pylist()]

        if not user_ids:
            print("[ERROR] No active users found in db.user_profiles table.", file=sys.stderr)
            sys.exit(1)

        return user_ids

    except Exception as e:
        print(f"[ERROR] Failed to fetch users from Iceberg Catalog: {e}", file=sys.stderr)
        sys.exit(1)