import os
import sys
from pyspark.sql.functions import col, expr
from pyspark.sql.avro.functions import from_avro

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from src.ingestion.config import TOPIC_NAME, DLQ_TOPIC_NAME, KAFKA_BOOTSTRAP_SERVERS, ICEBERG_TABLE, CHECKPOINT_PATH
from src.ingestion.spark_manager import create_streaming_spark_session, initialize_iceberg_table
from src.ingestion.schema_registry import fetch_latest_avro_schema
from src.ingestion.transformations import get_validation_condition, compute_stress_score

def start_realtime_ingestion(spark, avro_schema_json):
    print(f"[INFO] Initializing Real-time Stream from Kafka Topic: {TOPIC_NAME}")

    kafka_raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", "100")
        .load()
    )

    clean_binary_stream = kafka_raw_stream.withColumn(
        "avro_body", expr("substring(value, 6, length(value) - 5)")
    )
    deserialized_stream = clean_binary_stream.withColumn(
        "biometric_data", from_avro(col("avro_body"), avro_schema_json)
    )
    final_clean_df = deserialized_stream.select("biometric_data.*")

    df_with_time = final_clean_df.withColumn(
        "event_time", (col("timestamp") / 1000).cast("timestamp")
    )
    deduplicated_stream = df_with_time \
        .withWatermark("event_time", "2 hours") \
        .dropDuplicates(["event_id"])

    def process_stream_batch(batch_df, batch_id):
        batch_df = batch_df.persist()

        try:
            if batch_df.count() == 0:
                return

            is_valid = get_validation_condition()
            valid_df = batch_df.filter(is_valid)
            invalid_df = batch_df.filter(~is_valid)

            if invalid_df.count() > 0:
                invalid_df.selectExpr(
                    "CAST(event_id AS STRING) as key",
                    "to_json(struct(*)) as value"
                ).write.format("kafka") \
                    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                    .option("topic", DLQ_TOPIC_NAME) \
                    .save()

            if valid_df.count() > 0:
                scored_df = compute_stress_score(valid_df)
                scored_df.write \
                    .format("iceberg") \
                    .mode("append") \
                    .save(ICEBERG_TABLE)

                print(f"[SUCCESS] Batch {batch_id} committed to Lakehouse successfully.")

        finally:
            batch_df.unpersist()

    query = deduplicated_stream.writeStream \
        .foreachBatch(process_stream_batch) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    spark_session = create_streaming_spark_session()
    initialize_iceberg_table(spark_session, ICEBERG_TABLE)
    active_schema = fetch_latest_avro_schema(TOPIC_NAME)
    start_realtime_ingestion(spark_session, active_schema)