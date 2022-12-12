{{ config(enabled=var('ad_reporting__facebook_ads_enabled', True)) }}

with intermediate as (

    select *
    from {{ ref('int_facebook_ads__ad_set_report') }}

), 

backfill as (

    select * 
    from {{ ref('stg_facebook_ads__bpi_backfill__ad_set_report') }}
),

unioned as (

    select * 
    from intermediate

    union 

    select * 
    from backfill 
)

select * 
from unioned 
order by date_day 