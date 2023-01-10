{{ config(enabled=var('ad_reporting__facebook_ads_enabled', True)) }}

with ad_report as (

    select
        date_day
        ,account_name
	    ,campaign_name
	    ,ad_set_name
	    ,platform
	    ,sum(impressions) as impressions
	    ,sum(spend) as spend
    from {{ ref('facebook_ads__ad_platform_report') }}
     {{ dbt_utils.group_by(5) }}

), 

backfill as (

    select 	
        date_day
        ,'ACLU Liberty Watch (BPI)' as account_name
	    ,campaign_name
	    ,ad_set_name
	    ,platform
	    ,sum(impressions) as impressions
	    ,sum(spend) as spend
    from {{ ref('stg_facebook_ads__bpi_backfill__ad_set_platform_report') }}
    {{ dbt_utils.group_by(5) }}
),

unioned as (

    select *
    from ad_report 

    union 

    select * 
    from backfill 

)

select * from unioned