import asyncio
import os
import random
import sys
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
load_dotenv()

from src.ingestion.user_source import fetch_active_user_ids
from src.ingestion.biometric_generator import simulate_user_device
from src.ingestion.kafka_client import BiometricKafkaClient


async def main():
    required_keys = [
        "MINIO_USER", "MINIO_PASS", "CATALOG_URI", "MINIO_ENDPOINT",
        "KAFKA_BOOTSTRAP_SERVERS", "SCHEMA_REGISTRY_URL"
    ]
    missing_keys = [k for k in required_keys if not os.getenv(k)]
    if missing_keys:
        print(f"[ERROR] Missing mandatory environment variables: {', '.join(missing_keys)}.", file=sys.stderr)
        sys.exit(1)

    print("[INFO] Constructing data plane transit routes...")
    kafka_client = BiometricKafkaClient()

    print("[INFO] Syncing domain users with Iceberg Catalog metadata layers...")
    active_users = fetch_active_user_ids()
    print(f"[SUCCESS] Core data state matched. Active base: {len(active_users)} users.")

    user_tasks = []
    for user_id in active_users:
        initial_state = {
            "heart_rate": random.randint(65, 80),
            "hrv_rmssd": random.randint(45, 65),
            "eda_microsiemens": random.uniform(1.0, 5.0),
            "motion_status": "RESTING"
        }
        user_tasks.append(simulate_user_device(user_id, initial_state, kafka_client))

    print(f"[INFO] Activating {len(user_tasks)} device streams on event loop...")

    try:
        await asyncio.gather(*user_tasks)
    except asyncio.CancelledError:
        pass
    finally:
        kafka_client.flush()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[INFO] Streaming process halted by systems administrator.")
        sys.exit(0)