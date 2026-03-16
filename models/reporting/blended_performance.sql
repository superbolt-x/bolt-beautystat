{{ config (
    alias = target.database + '_blended_performance'
)}}

WITH orders AS (

    SELECT order_id, date as order_date
    FROM reporting.beautystat_shopify_daily_sales_by_order

)

, refund_order_data AS (

    -- Sales rows
    SELECT 
        date,
        order_id,
        customer_order_index,
		    1 AS order_count,
        gross_revenue,
        total_revenue,
        subtotal_discount,
        shipping_price,
        total_tax,
        shipping_discount,
        0 AS subtotal_refund,
        0 AS shipping_refund,
        0 AS tax_refund
    FROM reporting.beautystat_shopify_daily_sales_by_order
    WHERE order_id IN (SELECT order_id FROM orders)

    UNION ALL

    -- Refund rows
    SELECT 
        date,
        order_id,
        customer_order_index,
        0,0,0,0,0,0,0,
        subtotal_refund,
        shipping_refund,
        tax_refund
    FROM reporting.beautystat_shopify_daily_refunds
    WHERE order_id IN (SELECT order_id FROM orders)

)

, tw_data AS (

    SELECT 
        order_id,
		order_date,
		click_date,

        CASE 
            WHEN source = 'facebook-ads' AND campaign_id != '' THEN 'Facebook'
            WHEN source = 'google-ads' THEN 'Google Ads'
            ELSE 'Other'
        END AS channel,

        CASE 
            WHEN source IN ('facebook-ads','google-ads')
                 AND campaign_id != ''
            THEN campaign_id
            ELSE '(not set)'
        END AS campaign_id,
		index,
		count(*) over (partition by order_id) as order_index

    FROM triplewhale_raw.orders_attribution
	LEFT JOIN orders using(order_id)
    WHERE "_fivetran_deleted" = FALSE
      AND type = 'ORDERS_LINEAR_ALL'
	  and click_date::date >= order_date::date - 28
	order by order_id desc, index asc

)

, attributed_data AS (

    SELECT
        r.date,
        COALESCE(t.channel,'Other') AS channel,
        COALESCE(t.campaign_id,'(not set)') AS campaign_id,

        r.order_id,
        r.customer_order_index,
        COALESCE(t.order_index,1) AS order_index,
		
		-- Order amount 
		r.order_count,

        -- Sales amount
        (
            COALESCE(r.gross_revenue,0)
            - COALESCE(r.subtotal_discount,0)
            + COALESCE(r.total_tax,0)
            + COALESCE(r.shipping_price,0)
            - COALESCE(r.shipping_discount,0)
        ) AS sales_amount,

        -- Refund amount
        (
            COALESCE(r.subtotal_refund,0)
            - COALESCE(r.shipping_refund,0)
            + COALESCE(r.tax_refund,0)
        ) AS refund_amount

    FROM refund_order_data r
    LEFT JOIN tw_data t USING(order_id)

)

, final_sho_data AS (

    SELECT
        date,
        channel,
        campaign_id,
        SUM(
            sales_amount::float / order_index::float
        ) AS revenue,

        SUM(
            CASE 
                WHEN customer_order_index = 1
                THEN sales_amount::float / order_index::float
                ELSE 0
            END
        ) AS new_revenue,
        SUM(
            refund_amount::float / order_index::float
        ) AS refunds,

        SUM(
            CASE 
                WHEN customer_order_index = 1
                THEN refund_amount::float / order_index::float
                ELSE 0
            END
        ) AS new_refunds,
        SUM(
            (sales_amount - refund_amount)::float
            / order_index::float
        ) AS net_revenue,
        SUM(order_count::float / order_index::float) AS purchases,

        SUM(
            CASE 
                WHEN customer_order_index = 1
                THEN order_count::float / order_index::float
                ELSE 0
            END
        ) AS new_purchases

    FROM attributed_data
    GROUP BY 1,2,3

)

, paid_objects as (
	SELECT 'Facebook' as channel, campaign_id::varchar as campaign_id, campaign_name, count(*) as nb
	FROM reporting.beautystat_facebook_ad_performance 
	group by 1,2,3
	union all
	SELECT 'Google Ads' as channel, campaign_id::varchar as campaign_id, campaign_name, count(*) as nb
	FROM reporting.beautystat_googleads_campaign_performance
	group by 1,2,3
)

, conversion_data as (
	select * from final_sho_data left join paid_objects using(channel,campaign_id)
)

, paid_data as (
	SELECT 'Facebook' as channel, campaign_id::varchar as campaign_id, campaign_name,
	date, sum(spend) as spend, sum(impressions) as impressions, sum(link_clicks) as clicks, sum(purchases) as paid_purchases, sum(revenue) as paid_revenue
	FROM reporting.beautystat_facebook_ad_performance 
	where date_granularity = 'day'
	and (spend > 0 or link_clicks > 0 or impressions > 0)
	group by 1,2,3,4
	union all
	SELECT 'Google Ads' as channel, campaign_id::varchar as campaign_id, campaign_name,
	date, sum(spend) as spend, sum(impressions) as impressions, sum(clicks) as clicks, sum(purchases) as paid_purchases, sum(revenue) as paid_revenue
	FROM reporting.beautystat_googleads_campaign_performance
	where date_granularity = 'day'
	and (spend > 0 or clicks > 0 or impressions > 0)
	group by 1,2,3,4
)

, ga4_data as (
    SELECT 
        CASE 
            WHEN session_source_medium = 'facebook / cpc' THEN 'Facebook'
            WHEN session_source_medium = 'google / cpc' THEN 'Google Ads'
            ELSE 'Other'
        END AS channel,
        session_campaign_id::varchar as campaign_id,
        sum(sessions) as sessions, sum(engaged_sessions) as engaged_sessions, sum(conversions_purchase) as ga4_purchases, sum(purchase_revenue) as ga4_revenue
    FROM ga4_raw.traffic_sources_session
    GROUP BY 1,2

, final_data AS (
SELECT 
	date::date as date, channel, campaign_id, campaign_name,
	coalesce(spend,0) as spend, coalesce(impressions,0) as impressions, coalesce(clicks,0) as clicks, coalesce(paid_purchases,0) as paid_purchases, 
  coalesce(paid_revenue,0) as paid_revenue, coalesce(net_revenue,0) as net_revenue, coalesce(revenue,0) as revenue, 
  coalesce(new_revenue,0)-coalesce(new_refunds,0) as net_new_revenue, coalesce(new_revenue,0) as new_revenue, coalesce(purchases,0) as purchases, 
  coalesce(new_purchases,0) as new_purchases, coalesce(sessions,0) as sessions, coalesce(engaged_sessions,0) as engaged_sessions, coalesce(ga4_purchases,0) as ga4_purchases, 
  coalesce(ga4_revenue,0) as ga4_revenue
FROM conversion_data 
FULL OUTER JOIN paid_data USING(date,channel,campaign_id,campaign_name)
FULL OUTER JOIN paid_data USING(date,channel,campaign_id)
ORDER BY date desc )

select *
from final_data
