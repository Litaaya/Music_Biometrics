{{ config(materialized='view') }}

with source_data as (
    select * from {{ source('lakehouse_raw', 'user_profiles') }}
)

select
    cast(user_id as string) as user_id,
    cast(username as string) as username,
    cast(dob as date) as date_of_birth,
    cast(gender as string) as gender,
    cast(cultural_region as string) as cultural_region,
    cast(baseline_resting_hr as double) as baseline_resting_hr,
    cast(baseline_hrv_sdnn as double) as baseline_hrv_sdnn,
    cast(created_at as timestamp) as created_at,
    cast(updated_at as timestamp) as updated_at
from source_data