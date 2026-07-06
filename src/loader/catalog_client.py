import sys
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from src.loader import config

def get_iceberg_catalog():
    try:
        return load_catalog(
            "default",
            **{
                "type": "rest",
                "uri": config.CATALOG_URI,
                "s3.endpoint": config.MINIO_ENDPOINT,
                "s3.access-key-id": config.MINIO_USER,
                "s3.secret-access-key": config.MINIO_PASS,
                "s3.path-style-access": "true",
                "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO"
            }
        )
    except Exception as e:
        print(f"[CRITICAL] Failed to connect to Iceberg Catalog: {e}", file=sys.stderr)
        sys.exit(1)

def init_namespace(catalog, namespace_name="db"):
    try:
        catalog.create_namespace(namespace_name)
        print(f"[INFO] Namespace '{namespace_name}' created successfully.")
    except NamespaceAlreadyExistsError:
        print(f"[INFO] Namespace '{namespace_name}' already exists. Skipping initialization.")