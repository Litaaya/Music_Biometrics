{{ config(
    materialized='table',
    file_format='parquet'
) }}

with silver_enriched as (
    select * from {{ ref('int_biometric_enriched') }}
),

user_level_aggregation as (
    select
        user_id,
        username,
        user_age,
        gender,
        cultural_region,

        -- Lifetime stress profile
        round(avg(stress_score), 2) as overall_avg_stress_score,
        max(stress_score) as overall_max_stress_score,

        -- Long-term biometrics
        round(avg(heart_rate), 2) as overall_avg_heart_rate,
        round(avg(hrv_rmssd), 2) as overall_avg_hrv,
        round(avg(eda_microsiemens), 2) as overall_avg_eda,

        -- Data volume per user
        count(event_id) as total_tracked_events,
        count(distinct track_id) as total_unique_tracks_heard,
        sum(case when is_listening_music then 1 else 0 end) as total_music_events,

        -- Stability of the ratio
        sum(case when stress_score >= 70 then 1 else 0 end) as total_high_stress_events,
        sum(case when stress_score < 40 then 1 else 0 end) as total_relaxed_events

    from silver_enriched
    group by
        user_id,
        username,
        user_age,
        gender,
        cultural_region
)

select
    md5(user_id) as user_summary_id,

    user_id,
    username,
    user_age,
    gender,
    cultural_region,

    overall_avg_stress_score,
    overall_max_stress_score,
    overall_avg_heart_rate,
    overall_avg_hrv,
    overall_avg_eda,

    total_tracked_events,
    total_unique_tracks_heard,

    round(
        cast(total_music_events as double) / nullif(total_tracked_events, 0) * 100,
        2
    ) as music_engagement_percentage,

    round(
        cast(total_high_stress_events as double) / nullif(total_tracked_events, 0) * 100,
        2
    ) as high_stress_event_percentage,

    round(
        cast(total_relaxed_events as double) / nullif(total_tracked_events, 0) * 100,
        2
    ) as relaxed_event_percentage,

    case
        when overall_avg_stress_score >= 65
             or (
                cast(total_high_stress_events as double)
                / nullif(total_tracked_events, 0) * 100
             ) >= 25
            then 'HIGH_STRESS_PATTERN'

        when overall_avg_stress_score between 40 and 64.99
            then 'MODERATE_STRESS_PATTERN'

        else 'LOW_STRESS_PATTERN'
    end as stress_profile_category

from user_level_aggregation