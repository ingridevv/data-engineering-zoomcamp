{{
    config(
        materialized='incremental',
        unique_key='tripid'
    )
}}

with green_tripdata as (
    select *,
        'Green' as service_type
    from {{ ref('stg_green_tripdata') }}
    {% if is_incremental() %}
        where pickup_datetime > (select max(pickup_datetime) from {{ this }} where service_type = "Green") 
    {% endif %}
),
yellow_tripdata as (
    select *,
    'Yellow' as service_type
    from {{ ref('stg_yellow_tripdata') }}
    {% if is_incremental() %}
        where pickup_datetime > (select max(pickup_datetime) from {{ this }} where service_type = "Yellow")
    {% endif %}
),
trips_unioned as (
    select * from green_tripdata
        union all 
    select * from yellow_tripdata
)

select * from trips_unioned