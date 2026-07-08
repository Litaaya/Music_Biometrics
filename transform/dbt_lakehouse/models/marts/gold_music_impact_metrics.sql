{{ config(
    materialized='table',
    file_format='parquet'
) }}

with silver_enriched as (
    select * from {{ ref('int_biometric_enriched') }}
    where is_listening_music = true
),

music_impact_aggregation as (
    select
        track_id,
        track_title,
        artist_name,
        genre,
        granular_mood,

        -- Correlation
        tempo_bpm,
        energy_rms,
        music_valence,

        -- Psychological impact
        round(avg(stress_score), 2) as avg_stress_score,
        max(stress_score) as max_stress_score_recorded,
        min(stress_score) as min_stress_score_recorded,

        -- Physical impact
        round(avg(heart_rate), 2) as avg_heart_rate,
        round(avg(hrv_rmssd), 2) as avg_hrv_rmssd,
        round(avg(eda_microsiemens), 2) as avg_eda_microsiemens,

        -- Engagement metrics
        count(distinct user_id) as total_unique_listeners,
        count(event_id) as total_listening_events,

        -- High stress during listening
        sum(case when stress_score >= 70 then 1 else 0 end) as high_stress_listening_events

    from silver_enriched
    group by
        track_id,
        track_title,
        artist_name,
        genre,
        granular_mood,
        tempo_bpm,
        energy_rms,
        music_valence
)

select
    md5(track_id) as music_impact_id,

    track_id,
    track_title,
    artist_name,
    genre,
    granular_mood,
    tempo_bpm,
    energy_rms,
    music_valence,

    avg_stress_score,
    max_stress_score_recorded,
    min_stress_score_recorded,

    avg_heart_rate,
    avg_hrv_rmssd,
    avg_eda_microsiemens,

    total_unique_listeners,
    total_listening_events,
    high_stress_listening_events,

    round(
        cast(high_stress_listening_events as double)
        / nullif(total_listening_events, 0) * 100,
        2
    ) as high_stress_listening_percentage,

    case
        when avg_stress_score < 35 then 'LOW_STRESS_ASSOCIATED'
        when avg_stress_score between 35 and 49.99 then 'MILD_STRESS_ASSOCIATED'
        when avg_stress_score between 50 and 69.99 then 'MODERATE_STRESS_ASSOCIATED'
        else 'HIGH_STRESS_ASSOCIATED'
    end as music_response_category

from music_impact_aggregation