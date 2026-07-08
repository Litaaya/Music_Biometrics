{{ config(
    materialized='table',
    file_format='parquet'
) }}

with biometrics as (
    select * from {{ ref('stg_biometric_events') }}
),

users as (
    select * from {{ ref('stg_user_profiles') }}
),

tracks as (
    select * from {{ ref('stg_music_tracks') }}
),

enriched_base as (
    select
        b.event_id,
        b.user_id,
        b.track_id,
        b.event_timestamp_ms,
        b.event_time,
        b.heart_rate,
        b.hrv_rmssd,
        b.eda_microsiemens,
        b.motion_status,

        u.username,
        u.gender,
        u.cultural_region,
        u.baseline_resting_hr,
        u.baseline_hrv_sdnn,
        date_diff('year', u.date_of_birth, current_date) as user_age,

        t.track_title,
        t.artist_name,
        t.genre,
        t.tempo_bpm,
        t.energy_rms,
        t.spectral_flatness,
        t.music_valence,
        t.granular_mood,

        case
            when b.track_id is not null then true
            else false
        end as is_listening_music

    from biometrics b
    left join users u on b.user_id = u.user_id
    left join tracks t on b.track_id = t.track_id
),

calculated_factors as (
    select
        *,
        (heart_rate - baseline_resting_hr) / nullif(baseline_resting_hr, 0) as hr_factor,
        (baseline_hrv_sdnn - hrv_rmssd) / nullif(baseline_hrv_sdnn, 0) as hrv_factor
    from enriched_base
)

select
    event_id,
    user_id,
    track_id,
    event_timestamp_ms,
    event_time,
    user_age,
    gender,
    cultural_region,

    is_listening_music,
    track_title,
    artist_name,
    genre,
    tempo_bpm,
    energy_rms,
    spectral_flatness,
    music_valence,
    granular_mood,

    heart_rate,
    hrv_rmssd,
    eda_microsiemens,
    motion_status,

    greatest(0, least(100, cast(
        round(
            case
                when motion_status in ('WALKING', 'RUNNING') then
                    (greatest(0, hr_factor) * 30)
                    + (greatest(0, hrv_factor) * 50)
                    + (least(eda_microsiemens, 10) * 2)
                else
                    (greatest(0, hr_factor) * 50)
                    + (greatest(0, hrv_factor) * 40)
                    + (least(eda_microsiemens, 10) * 1)
            end
        , 2) as double
    ))) as stress_score

from calculated_factors