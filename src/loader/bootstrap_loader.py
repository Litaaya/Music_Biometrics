import os
import sys
from dotenv import load_dotenv
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError

load_dotenv()

# Path resolution for local sample data storage
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SAMPLE_DATA_DIR = os.path.join(PROJECT_ROOT, "sample_data")
USER_SOURCE = os.path.join(SAMPLE_DATA_DIR, "user_profiles.parquet")
MUSIC_SOURCE = os.path.join(SAMPLE_DATA_DIR, "music_metadata.parquet")

# Retrieve infrastructure configurations strictly from environment variables
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
    print(f"Missing mandatory environment variables: {', '.join(missing_vars)}.", file=sys.stderr)
    sys.exit(1)

def downcast_timestamp_ns_to_us(arrow_table: pa.Table) -> pa.Table:
    new_fields = []

    for field in arrow_table.schema:
        field_type = field.type

        if pa.types.is_timestamp(field_type) and field_type.unit == "ns":
            new_fields.append(
                pa.field(
                    field.name,
                    pa.timestamp("us", tz=field_type.tz),
                    nullable=field.nullable,
                    metadata=field.metadata,
                )
            )
        else:
            new_fields.append(field)

    new_schema = pa.schema(new_fields, metadata=arrow_table.schema.metadata)
    return arrow_table.cast(new_schema)

def init_iceberg_lakehouse():
    # Connects to the Iceberg REST Catalog, provisions the namespace, and seeds the dimension tables.
    print("Connecting to Iceberg REST Catalog")

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
    except Exception as e:
        print(f"Failed to establish connection to Iceberg Catalog: {e}", file=sys.stderr)
        sys.exit(1)

    # Initialize core catalog namespace
    namespace_name = "db"
    try:
        catalog.create_namespace(namespace_name)
        print(f"Namespace '{namespace_name}' created successfully.")
    except NamespaceAlreadyExistsError:
        print(f"Namespace '{namespace_name}' already exists. Skipping initialization.")

    # Validate existence of source data assets
    if not os.path.exists(USER_SOURCE) or not os.path.exists(MUSIC_SOURCE):
        print(f"Source parquet seed files not found in {SAMPLE_DATA_DIR}.", file=sys.stderr)
        sys.exit(1)

    # Ingestion pipeline for user profiles dimension
    print("Executing ingestion for user profiles dimension...")
    df_users = pd.read_parquet(USER_SOURCE)
    arrow_table_users = pa.Table.from_pandas(df_users, preserve_index=False)
    arrow_table_users = downcast_timestamp_ns_to_us(arrow_table_users)

    table_user_identifier = f"{namespace_name}.user_profiles"
    try:
        iceberg_table_user = catalog.create_table(
            identifier=table_user_identifier,
            schema=arrow_table_users.schema
        )
        iceberg_table_user.append(arrow_table_users)
        print(f"Ingested {len(df_users)} records into '{table_user_identifier}'.")
    except TableAlreadyExistsError:
        print(f"Table '{table_user_identifier}' already exists. Skipping seed insert.")

    # Ingestion pipeline for music metadata dimension
    print("Executing ingestion for music metadata dimension...")
    df_music = pd.read_parquet(MUSIC_SOURCE)
    arrow_table_music = pa.Table.from_pandas(df_music, preserve_index=False)
    arrow_table_music = downcast_timestamp_ns_to_us(arrow_table_music)

    table_music_identifier = f"{namespace_name}.music_tracks"
    try:
        iceberg_table_music = catalog.create_table(
            identifier=table_music_identifier,
            schema=arrow_table_music.schema
        )
        iceberg_table_music.append(arrow_table_music)
        print(f"Ingested {len(df_music)} records into '{table_music_identifier}'.")
    except TableAlreadyExistsError:
        print(f"Table '{table_music_identifier}' already exists. Skipping seed insert.")

    print("Bootstrap execution completed. Catalog dimensions are fully operational.")


if __name__ == "__main__":
    init_iceberg_lakehouse()