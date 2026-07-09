{{ config(
    materialized='incremental',
    unique_key='stress_event_id',
    file_format='parquet'
) }}

with silver_enriched as (
    select * from {{ ref('int_biometric_enriched') }}
)

select
    event_id as stress_event_id,

    event_time,
    user_id,
    username,
    user_age,
    gender,
    cultural_region,
    motion_status,

    stress_score,
    case
        when stress_score >= 80 then 'VERY_HIGH'
        when stress_score >= 70 then 'HIGH'
        when stress_score >= 50 then 'MODERATE'
        else 'LOW'
    end as stress_level,
    heart_rate,
    hrv_rmssd,
    eda_microsiemens,

    is_listening_music,
    track_id,
    track_title,
    artist_name,
    genre as music_genre,

    current_timestamp as processed_at

from silver_enriched