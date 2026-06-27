-- Hourly event fact mart with conversion-rate KPI per hour.

with hourly as (

    select *
    from read_parquet('../lakehouse/events_hourly/*/*.parquet')

),

pivoted as (

    select
        event_hour,
        sum(event_count)                                              as total_events,
        sum(case when event_type = 'purchase' then event_count else 0 end)   as purchases,
        sum(case when event_type = 'page_view' then event_count else 0 end)  as page_views,
        sum(total_amount)                                             as revenue
    from hourly
    group by event_hour

)

select
    event_hour,
    total_events,
    purchases,
    page_views,
    revenue,
    round(
        case when page_views > 0 then purchases * 100.0 / page_views else 0 end, 2
    ) as conversion_rate_pct
from pivoted
order by event_hour
