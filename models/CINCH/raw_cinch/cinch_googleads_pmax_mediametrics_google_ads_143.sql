{{ config(
    schema = 'raw_cinch_test',
    tags = ['raw_cinch_test'],
    materialized='view',
    alias = 'cinch_googleads_pmax_mediametrics_google_ads_143',
    copy_grants = true
    ) }}


Select 
dt_created AS dt_created,
dt_updated AS dt_updated,
dt_filename AS dt_filename,
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
network AS network
from {{ source('bigquery_source', 'cinchGoogleAdspMaxMediaMetrics_google_ads_143') }}


