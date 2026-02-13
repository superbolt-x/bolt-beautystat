{{ config (
    alias = target.database + '_pinterest_ad_group_performance'
)}}

SELECT 
advertiser_name,
campaign_name,
campaign_id,
campaign_status,
campaign_type_default,
ad_group_name,
ad_group_id,
ad_group_status,
audience,
date,
date_granularity,
spend,
impression_1+impression_2 as impressions,
clickthrough_1+clickthrough_2 as clicks,
total_view_add_to_cart+total_click_add_to_cart as add_to_cart,
total_checkout as purchases,
total_checkout_value as revenue
FROM {{ ref('pinterest_performance_by_ad_group') }}
