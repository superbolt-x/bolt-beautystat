{{ config (
    alias = target.database + '_googleads_asset_group_performance'
)}}

SELECT 
account_id,
campaign_name,
campaign_id,
campaign_status,
campaign_type_default,
asset_group_name,
asset_group_id,
asset_group_status,
date,
date_granularity,
spend,
impressions,
clicks,
conversions as purchases,
conversions_value as revenue
FROM {{ ref('googleads_performance_by_asset_group') }}
