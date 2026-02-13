{{ config (
    alias = target.database + '_bingads_campaign_performance'
)}}

SELECT 
account_id,
campaign_name,
campaign_id,
campaign_status,
campaign_type_default,
date,
date_granularity,
spend,
impressions,
clicks,
conversions as purchases,
revenue,
view_through_conversions
FROM {{ ref('bingads_performance_by_campaign') }}
