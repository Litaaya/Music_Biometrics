import sys
import httpx
from src.ingestion.config import SCHEMA_REGISTRY_URL

def fetch_latest_avro_schema(topic):
    subject = f"{topic}-value"
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest"
    try:
        response = httpx.get(url)
        response.raise_for_status()
        return response.json()["schema"]
    except Exception as e:
        print(f"[CRITICAL] Failed to fetch schema: {e}", file=sys.stderr)
        sys.exit(1)