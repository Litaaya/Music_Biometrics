import os
import sys
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

class BiometricKafkaClient:
    def __init__(self):
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        self.schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL")
        self.topic = "biometrics_stream"

        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        schema_path = os.path.join(project_root, "src", "schemas", "biometrics_stream.avsc")

        if not os.path.exists(schema_path):
            print(f"[ERROR] Avro schema file not found at: {schema_path}", file=sys.stderr)
            sys.exit(1)

        with open(schema_path, "r") as f:
            schema_str = f.read()

        try:
            sr_client = SchemaRegistryClient({"url": self.schema_registry_url})

            # Build Avro Serializer bound to the contract schema string
            avro_serializer = AvroSerializer(
                schema_registry_client=sr_client,
                schema_str=schema_str
            )

            producer_config = {
                "bootstrap.servers": self.bootstrap_servers,
                "value.serializer": avro_serializer,
                "acks": "all"
            }
            self.producer = SerializingProducer(producer_config)
            print("[INFO] Kafka Serializing Producer initialized successfully with Avro Registry.")

        except Exception as e:
            print(f"[ERROR] Failed to establish connection with Kafka infra: {e}", file=sys.stderr)
            sys.exit(1)

    def publish(self, payload: dict):
        try:
            self.producer.produce(
                topic=self.topic,
                key=payload["user_id"],
                value=payload,
                on_delivery=self._delivery_report
            )
            # Serve network background delivery queue without blocking thread loops
            self.producer.poll(0)
        except Exception as e:
            print(f"[ERROR] Ingestion drop-out encountered during produce action: {e}", file=sys.stderr)

    def flush(self):
        print("[INFO] Flushing internal Kafka client memory buffer slots...")
        self.producer.flush()

    @staticmethod
    def _delivery_report(err, msg):
        if err is not None:
            print(f"[ERROR] Message delivery failure on transit layer: {err}", file=sys.stderr)