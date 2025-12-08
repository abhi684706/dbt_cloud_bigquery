{{ config(
    schema = 'edw_cinch_test',
    tags = ['edw_cinch_test'],
    materialized='view',
    alias = 'edw_microstrategy',
    copy_grants = true
    ) }}




 

Select  
dt_created AS dt_created,
dt_updated AS dt_updated,
dt_filename AS dt_filename,
contract_number AS contract_number,
CASE
    WHEN enrollment_type IN ('Call', 'CCHS', 'TPDOTCOM') THEN 'phone'
    ELSE enrollment_type
    END
    AS enrollment_type,
CAST(enroll_date AS DATE) AS enroll_date,
cdr_lead_id AS cdr_lead_id,
client_id AS client_id,
salesbook_client_name AS salesbook_client_name,
cellcode_channel AS cellcode_channel,
utm_channel AS utm_channel,
first_utm_medium AS first_utm_medium,
first_utm_source AS first_utm_source,
first_utm_content AS first_utm_content,
first_utm_campaign AS first_utm_campaign,
return_utm_medium AS return_utm_medium,
return_utm_source AS return_utm_source,
return_utm_content AS return_utm_content,
return_utm_campaign AS return_utm_campaign,
sub_channel AS sub_channel,
hs_sub_seq AS hs_sub_seq,
hs_source_code AS hs_source_code,
z_create_user AS z_create_user,
agent_name AS agent_name,
state AS state,
zipcode AS zipcode,
last_6_customer_phone_number AS last_6_customer_phone_number,
campaign AS campaign,
utm_cellcode AS utm_cellcode,
enrollment_gcl_id AS enrollment_gcl_id,
metrics AS metrics,
enrollments AS enrollments,
'microstrategy enrolls' AS data_source
from 
{{ ref('cinch_microstrategy_enrollments_mailgun_180') }}
where enroll_date <= '2024-12-31'