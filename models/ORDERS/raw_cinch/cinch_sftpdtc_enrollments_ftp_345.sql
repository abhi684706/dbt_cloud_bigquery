
{{ config(
    schema = 'raw_cinch_test',
    tags = ['raw_cinch_test'],
    materialized='view',
    alias = 'cinch_sftpdtc_enrollments_ftp_345',
    copy_grants = true
    ) }}


--Select * from {{ source('bigquery_source', 'cinchSFTPDTCEnrollments_ftp_345') }}

Select
dt_created AS dt_created,
dt_updated AS dt_updated,
dt_filename AS dt_filename,
date AS date,
unique_identifier AS unique_identifier,
client_id AS client_id,
contract_enroll_date AS contract_enroll_date,
contract_source_code AS contract_source_code,
contract_number AS contract_number,
cell_code AS cell_code,
dtc_utm_channel_group AS dtc_utm_channel_group,
dtc_cellcode_channel_group AS dtc_cellcode_channel_group,
marketing_channel AS marketing_channel,
product_coverage_summary_name AS product_coverage_summary_name,
product_name AS product_name,
product_description AS product_description,
dtc_first_utm_source AS dtc_first_utm_source,
dtc_first_utm_medium AS dtc_first_utm_medium,
dtc_first_utm_campaign AS dtc_first_utm_campaign,
dtc_first_utm_content AS dtc_first_utm_content,
dtc_return_utm_source AS dtc_return_utm_source,
dtc_return_utm_medium AS dtc_return_utm_medium,
dtc_return_utm_campaign AS dtc_return_utm_campaign,
dtc_return_utm_content AS dtc_return_utm_content,
contract_agent_name AS contract_agent_name,
customer_first_name AS customer_first_name,
customer_last_name AS customer_last_name,
customer_email_address AS customer_email_address,
customer_contact_phone_full AS customer_contact_phone_full,
customer_contact_state_full AS customer_contact_state_full,
customer_contact_zip_full AS customer_contact_zip_full,
ga4_cid AS ga4_cid,
google_tracking_id AS google_tracking_id,
gbb_lead AS gbb_lead,
session_id AS session_id,
tot_prem_price AS tot_prem_price,
metrics AS metrics,
dtc_base_prem_price AS dtc_base_prem_price
from
{{ source('bigquery_source', 'cinchSFTPDTCEnrollments_ftp_345') }}