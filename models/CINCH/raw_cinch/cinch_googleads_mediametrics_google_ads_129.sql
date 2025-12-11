{{ config(
    schema = 'raw_cinch_test',
    tags = ['raw_cinch_test'],
    materialized='view',
    alias = 'cinch_googleads_mediametrics_google_ads_129',
    copy_grants = true
    ) }}


Select
dt_created AS dt_created,
dt_updated AS dt_updated,
dt_filename AS dt_filename,
ad_group_name AS ad_group_name,
ad_group_id AS ad_group_id,
ad_group_state AS ad_group_state,
ad_group_type AS ad_group_type,
advertising_channel AS advertising_channel,
campaign_name AS campaign_name,
campaign_id AS campaign_id,
clicks AS clicks,
costs AS costs,
account_id AS account_id,
account_name AS account_name,
day AS day,
device_type AS device_type,
impressions AS impressions,
network AS network,
search_impression_share AS search_impression_share
from {{ source('bigquery_source', 'cinchGoogleAdsMediaMetrics_google_ads_129') }}