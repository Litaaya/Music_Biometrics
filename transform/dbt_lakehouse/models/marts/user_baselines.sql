{{ config(
    materialized='table',
    file_format='parquet'
) }}

with silver_enriched as (
    select * from {{ ref('int_biometric_enriched') }}
),

static_baselines as (
    select distinct
        user_id,
        username,
        gender,
        user_age,
        cultural_region,
        baseline_resting_hr as static_resting_hr,
        baseline_hrv_sdnn as static_hrv_sdnn
    from silver_enriched
),

dynamic_baselines as (
    select
        user_id,
        round(avg(heart_rate), 2) as dynamic_resting_hr,
        round(avg(hrv_rmssd), 2) as dynamic_hrv_rmssd,
        count(event_id) as resting_tracked_events
    from silver_enriched
    where motion_status = 'RESTING'
    group by user_id
),

final as (
    select
        md5(s.user_id) as baseline_id,
        s.user_id,
        s.username,
        s.gender,
        s.user_age,
        s.cultural_region,

        s.static_resting_hr,
        s.static_hrv_sdnn,

        coalesce(d.dynamic_resting_hr, s.static_resting_hr) as calculated_resting_hr,
        coalesce(d.dynamic_hrv_rmssd, s.static_hrv_sdnn) as calculated_hrv_rmssd,
        coalesce(d.resting_tracked_events, 0) as resting_tracked_events,

        case
            when coalesce(d.resting_tracked_events, 0) >= 30
                then d.dynamic_resting_hr
            else s.static_resting_hr
        end as effective_resting_hr,

        case
            when coalesce(d.resting_tracked_events, 0) >= 30
                then d.dynamic_hrv_rmssd
            else s.static_hrv_sdnn
        end as effective_hrv_rmssd,

        case
            when coalesce(d.resting_tracked_events, 0) >= 30
                then 'DYNAMIC_RESTING_EVENTS'
            else 'STATIC_FALLBACK'
        end as baseline_source,

        round(
            coalesce(d.dynamic_resting_hr, s.static_resting_hr) - s.static_resting_hr,
            2
        ) as hr_deviation,

        round(
            coalesce(d.dynamic_hrv_rmssd, s.static_hrv_sdnn) - s.static_hrv_sdnn,
            2
        ) as hrv_deviation,

        current_timestamp as calculated_at

    from static_baselines s
    left join dynamic_baselines d
        on s.user_id = d.user_id
)

select
    *,

    case
        when abs(hr_deviation) >= 15
             or abs(hrv_deviation) >= 20
            then 'SIGNIFICANT_BASELINE_SHIFT'

        when abs(hr_deviation) >= 7
             or abs(hrv_deviation) >= 10
            then 'MODERATE_BASELINE_SHIFT'

        else 'STABLE_BASELINE'
    end as baseline_status_category

from final