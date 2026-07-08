{{ config(materialized='view') }}

with source_data as (
    select *
    from {{ source('lakehouse_raw', 'raw_biometric_events') }}
)

select
    cast(event_id as string) as event_id,
    cast(user_id as string) as user_id,
    cast(track_id as string) as track_id,
    cast(heart_rate as double) as heart_rate,
    cast(hrv_rmssd as double) as hrv_rmssd,
    cast(eda_microsiemens as double) as eda_microsiemens,
    cast(event_time as timestamp) as event_time,
    cast("timestamp" as bigint) as event_timestamp_ms,
    cast(motion_status as string) as motion_status

from source_data