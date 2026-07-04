from pyspark.sql.functions import col, when

def get_validation_condition():
    return (
        col("event_id").isNotNull()
        & col("user_id").isNotNull()
        & col("timestamp").isNotNull()
        & col("heart_rate").between(30, 220)
        & col("hrv_rmssd").between(1, 250)
        & col("eda_microsiemens").between(0, 50)
    )

def compute_stress_score(df):
    return df.withColumn(
        "stress_score",
        when((col("heart_rate") > 100) & (col("eda_microsiemens") > 5.0), "HIGH")
        .when((col("heart_rate") > 80) | (col("eda_microsiemens") > 3.0), "MEDIUM")
        .otherwise("LOW")
    )