{{ config(
    materialized='table',
    file_format='parquet'
) }}

with silver_enriched as (
    select * from {{ ref('int_biometric_enriched') }}
),

daily_aggregation as (
    select
        cast(event_time as date) as date_day,
        user_id,
        username,
        user_age,
        gender,
        cultural_region,

        -- Stress metrics
        round(avg(stress_score), 2) as avg_stress_score,
        max(stress_score) as max_stress_score,
        min(stress_score) as min_stress_score,

        -- Biometric metrics
        round(avg(heart_rate), 2) as avg_heart_rate,
        max(heart_rate) as max_heart_rate,
        round(avg(hrv_rmssd), 2) as avg_hrv_rmssd,
        round(avg(eda_microsiemens), 2) as avg_eda_microsiemens,

        -- Music engagement
        count(distinct track_id) as unique_tracks_listened,

        -- Time distribution
        count(event_id) as total_log_events,
        sum(case when is_listening_music then 1 else 0 end) as music_listening_events,

        -- Percentage stress event
        sum(case when stress_score >= 70 then 1 else 0 end) as high_stress_events,
        sum(case when stress_score between 40 and 69.99 then 1 else 0 end) as moderate_stress_events,
        sum(case when stress_score < 40 then 1 else 0 end) as relaxed_events

    from silver_enriched
    group by
        cast(event_time as date),
        user_id,
        username,
        user_age,
        gender,
        cultural_region
)

select
    md5(concat_ws('-', cast(date_day as string), user_id)) as daily_summary_id,

    date_day,
    user_id,
    username,
    user_age,
    gender,
    cultural_region,

    avg_stress_score,
    max_stress_score,
    min_stress_score,

    avg_heart_rate,
    max_heart_rate,
    avg_hrv_rmssd,
    avg_eda_microsiemens,

    unique_tracks_listened,
    total_log_events,
    music_listening_events,

    round(
        cast(high_stress_events as double) / nullif(total_log_events, 0) * 100,
        2
    ) as high_stress_time_percentage,

    round(
        cast(moderate_stress_events as double) / nullif(total_log_events, 0) * 100,
        2
    ) as moderate_stress_time_percentage,

    round(
        cast(relaxed_events as double) / nullif(total_log_events, 0) * 100,
        2
    ) as relaxed_time_percentage,

    round(
        cast(music_listening_events as double) / nullif(total_log_events, 0) * 100,
        2
    ) as music_listening_percentage

from daily_aggregation