{{ config(
    materialized='incremental',
    unique_key='permit_si_no',
    partition_by={
        'field': 'issuance_date',
        'data_type': 'timestamp',
        'granularity': 'day'
    },
    cluster_by=['gis_nta_name', 'job_type', 'borough', 'block']
) }}

SELECT * FROM {{ ref('stg_permits') }}

{% if is_incremental() %}
WHERE permit_si_no NOT IN (SELECT permit_si_no FROM {{ this }})
{% endif %}
