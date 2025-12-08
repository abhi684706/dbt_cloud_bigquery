{{
    config(
        schema="raw_cinch_test",
        tags=["raw_cinch_test"],
        materialized="view",
        alias="cinch_gsheet_bandaid_enroll_classification_overwritedrive_395",
        copy_grants=true,
    )
}}
-- Select * from {{ source('bigquery_source', 'cinchGSheetBandAidEnrollClassificationOverwritedrive_395') }}
select
    dt_created as dt_created,
    dt_updated as dt_updated,
    dt_filename as dt_filename,
    return_utm_medium2 as return_utm_medium2,
    contract_id as contract_id,
    total as total,
    mapped_channel as mapped_channel,
    period_start as period_start,
    period_end as period_end
from
    {{
        source(
            "bigquery_source",
            "cinchGSheetBandAidEnrollClassificationOverwritedrive_395",
        )
    }}
