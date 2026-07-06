import os
import sys
from pyiceberg.catalog import load_catalog

MINIO_USER = os.getenv("MINIO_USER")
MINIO_PASS = os.getenv("MINIO_PASS")
CATALOG_URI = os.getenv("CATALOG_URI")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")

def fetch_active_track_ids():
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

        table = catalog.load_table("db.music_tracks")
        arrow_table = table.scan(selected_fields=("track_id",)).to_arrow()
        track_ids = [str(tid) for tid in arrow_table["track_id"].to_pylist()]

        if not track_ids:
            print("[ERROR] No active tracks found in db.music_tracks table.", file=sys.stderr)
            sys.exit(1)

        return track_ids

    except Exception as e:
        print(f"[ERROR] Failed to fetch tracks from Iceberg Catalog: {e}", file=sys.stderr)
        sys.exit(1)