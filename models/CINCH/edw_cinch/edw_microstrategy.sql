{{ config(
    schema = 'edw_cinch_test',
    tags = ['edw_cinch_test'],
    materialized='view',
    alias = 'edw_microstrategy',
    copy_grants = true
    ) }}




with microstrategy_enrolls AS (
Select 
'microstrategy enrolls' AS data_source, 
CAST(NULL AS DATE) AS date,
NULL AS unique_identifier,
CASE
    WHEN enrollment_type IN ('Call', 'CCHS', 'TPDOTCOM') THEN 'phone'
    ELSE enrollment_type
    END
    AS enrollment_type,
--dt_created AS dt_created,
--dt_updated AS dt_updated,
--dt_filename AS dt_filename,
contract_number AS contract_number,
CAST(NULL AS String) AS cell_code,
CAST(NULL AS String) AS dtc_utm_channel_group,
CAST(NULL AS String) AS dtc_cellcode_channel_group,
REPLACE(CAST(NULL AS String), '\n', 'string') AS product_coverage_summary_name,
REPLACE(CAST(NULL AS String), '\n', 'string') AS product_name,
REPLACE(CAST(NULL AS String), '\n', 'string') AS product_description,
CAST(NULL AS String) AS dtc_first_utm_source,
CAST(NULL AS String) AS dtc_first_utm_medium,
CAST(NULL AS String) AS dtc_first_utm_campaign,
CAST(NULL AS String) AS dtc_first_utm_content,
CAST(NULL AS String) AS dtc_return_utm_source,
CAST(NULL AS String) AS dtc_return_utm_medium,
CAST(NULL AS String) AS dtc_return_utm_campaign,
CAST(NULL AS String) AS dtc_return_utm_content,
--CAST(enroll_date AS DATE) AS enroll_date,
--cdr_lead_id AS cdr_lead_id,
--client_id AS client_id,
-- salesbook_client_name AS salesbook_client_name,
-- cellcode_channel AS cellcode_channel,
-- utm_channel AS utm_channel,
-- first_utm_medium AS first_utm_medium,
-- first_utm_source AS first_utm_source,
-- first_utm_content AS first_utm_content,
-- first_utm_campaign AS first_utm_campaign,
-- return_utm_medium AS return_utm_medium,
-- return_utm_source AS return_utm_source,
-- return_utm_content AS return_utm_content,
-- return_utm_campaign AS return_utm_campaign,
CAST(NULL AS String) AS customer_contact_state_full,
CAST(NULL AS INTeger) AS customer_contact_zip_full,
CAST(NULL AS String) AS gbb_lead,
CAST(NULL AS FLOAT64) AS tot_prem_price,
CAST(NULL AS String) AS dtc_base_prem_price,
'1' AS enrollments
-- sub_channel AS sub_channel,
-- hs_sub_seq AS hs_sub_seq,
--hs_source_code AS hs_source_code,
-- z_create_user AS z_create_user,
--agent_name AS agent_name,
--state AS state,
--zipcode AS zipcode,
--last_6_customer_phone_number AS last_6_customer_phone_number,
-- campaign AS campaign,
--utm_cellcode AS utm_cellcode,
-- enrollment_gcl_id AS enrollment_gcl_id,
--metrics AS metrics,
-- enrollments AS enrollments,
from 
{{ ref('cinch_microstrategy_enrollments_mailgun_180') }}
where enroll_date <= '2024-12-31'
),

sftpdtc_enrolls AS (
Select
'microstrategy enrolls with product' AS data_source,
date AS date,
unique_identifier AS unique_identifier,
--dt_created AS dt_created,
--dt_updated AS dt_updated,
--dt_filename AS dt_filename,
CASE
    WHEN contract_source_code = 'CLNTNET' THEN 'Web'
    WHEN contract_source_code = 'TOG' THEN 'Winback'
    ELSE 'Phone'
    END
    AS enrollment_type,
contract_number AS contract_number,
cell_code AS cell_code,
--client_id AS client_id,
--contract_enroll_date AS contract_enroll_date,
--contract_source_code AS contract_source_code,
LOWER(dtc_utm_channel_group) AS dtc_utm_channel_group,
LOWER(dtc_cellcode_channel_group) AS dtc_cellcode_channel_group,
--marketing_channel AS marketing_channel,
REPLACE(product_coverage_summary_name, '\n', 'string') AS product_coverage_summary_name,
REPLACE(product_name, '\n', 'string') AS product_name,
REPLACE(product_description, '\n', 'string') AS product_description,
LOWER(dtc_first_utm_source) AS dtc_first_utm_source,
LOWER(dtc_first_utm_medium) AS dtc_first_utm_medium,
LOWER(dtc_first_utm_campaign) AS dtc_first_utm_campaign,
LOWER(dtc_first_utm_content) AS dtc_first_utm_content,
LOWER(dtc_return_utm_source) AS dtc_return_utm_source,
LOWER(dtc_return_utm_medium) AS dtc_return_utm_medium,
LOWER(dtc_return_utm_campaign) AS dtc_return_utm_campaign,
LOWER(dtc_return_utm_content) AS dtc_return_utm_content,
--contract_agent_name AS contract_agent_name,
--customer_first_name AS customer_first_name,
--customer_last_name AS customer_last_name,
--customer_email_address AS customer_email_address,
--customer_contact_phone_full AS customer_contact_phone_full,
customer_contact_state_full AS customer_contact_state_full,
customer_contact_zip_full AS customer_contact_zip_full,
--ga4_cid AS ga4_cid,
--google_tracking_id AS google_tracking_id,
gbb_lead AS gbb_lead,
--session_id AS session_id,
tot_prem_price AS tot_prem_price,
--metrics AS metrics,
dtc_base_prem_price AS dtc_base_prem_price,
'1' AS enrollments   
from 
{{ ref('cinch_sftpdtc_enrollments_ftp_345') }}
WHERE product_name NOT LIKE '%SURGE%' -- Exclude 'SURGE' products
)
select * from microstrategy_enrolls
union all
select * from sftpdtc_enrolls
order by date desc


