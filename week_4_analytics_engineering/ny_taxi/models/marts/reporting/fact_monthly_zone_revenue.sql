{{
    config(
        materialized='table'
        
    )
}}


with trips as (
    select * from {{ ref('fact_trips') }}
),

monthly_revenue as (
    select 
        -- Truncate date to month
        date_trunc('month', pickup_datetime) as monthly_revenue,

        -- Location identifiers
        pickup_borough,
        pickup_zone,
        pickup_locationid,

        -- Revenue Metrics
        sum(total_amount) as monthly_total_revenue,
        count(tripid) as total_monthly_trips,
        avg(trip_distance) avg_monthly_trips_distance,
        sum(tip_amount) as total_monthly_tips
    from trips
    group by 1, 2, 3, 4
)

select * from monthly_revenue
order by monthly_revenue desc, monthly_total_revenue desc