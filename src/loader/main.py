import os
import sys
import pandas as pd
import pyarrow as pa
from pyiceberg.exceptions import TableAlreadyExistsError

from src.loader import config
from src.loader.utils import normalize_arrow_schema
from src.loader.catalog_client import get_iceberg_catalog, init_namespace


def bootstrap_dimension_table(catalog, source_path, target_table_name):
    if not os.path.exists(source_path):
        print(f"[ERROR] Source parquet seed file not found: {source_path}", file=sys.stderr)
        sys.exit(1)

    print(f"[PROCESS] Executing ingestion for dimension table: {target_table_name}...")

    df = pd.read_parquet(source_path)
    arrow_table = pa.Table.from_pandas(df, preserve_index=False)
    arrow_table = normalize_arrow_schema(arrow_table)

    try:
        iceberg_table = catalog.create_table(
            identifier=target_table_name,
            schema=arrow_table.schema
        )
        iceberg_table.append(arrow_table)
        print(f"[SUCCESS] Ingested {len(df)} records into '{target_table_name}'.")
    except TableAlreadyExistsError:
        print(f"[SKIP] Table '{target_table_name}' already exists. Skipping seed insert.")


def main():

    catalog = get_iceberg_catalog()
    init_namespace(catalog, "db")

    bootstrap_dimension_table(catalog, config.USER_SOURCE, "db.user_profiles")
    bootstrap_dimension_table(catalog, config.MUSIC_SOURCE, "db.music_tracks")

    print("BOOTSTRAP PROCESS COMPLETED SUCCESSFULLY.")

if __name__ == "__main__":
    main()