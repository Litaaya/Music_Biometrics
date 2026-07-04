import asyncio
import random
import time
import uuid

async def simulate_user_device(user_id, initial_state, kafka_client):
    current_hr = initial_state["heart_rate"]
    current_hrv = initial_state["hrv_rmssd"]
    current_eda = initial_state["eda_microsiemens"]
    motion_status = initial_state["motion_status"]

    print(f"[INFO] Initialized streaming task for device user: {user_id}")

    # Pre-defined professional motion states for wearable devices
    motion_pool = ["RESTING", "WALKING", "RUNNING", "WORKING"]

    while True:
        current_hr += random.choice([-1.0, 0.0, 1.0])
        current_hr = max(55.0, min(140.0, current_hr))

        current_hrv += random.choice([-1.5, 0.0, 1.5])
        current_hrv = max(15.0, min(120.0, current_hrv))

        current_eda += random.choice([-0.1, 0.0, 0.1])
        current_eda = max(0.1, min(15.0, current_eda))

        if random.random() < 0.05:
            motion_status = random.choice(motion_pool)

        payload = {
            "event_id": str(uuid.uuid4()),
            "user_id": user_id,
            "timestamp": int(time.time() * 1000),
            "heart_rate": round(current_hr, 2),
            "hrv_rmssd": round(current_hrv, 2),
            "eda_microsiemens": round(current_eda, 2),
            "motion_status": motion_status
        }

        kafka_client.publish(payload)

        jitter = random.uniform(-0.05, 0.05)
        sleep_duration = max(0.1, 1.0 + jitter)
        await asyncio.sleep(sleep_duration)