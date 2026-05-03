SELECT
    borough,
    job_type,
    block,
    lot,
    PARSE_TIMESTAMP('%m/%d/%Y', issuance_date) AS issuance_date,
    CAST(permit_si_no AS INT64) AS permit_si_no,
    CASE
        WHEN gis_latitude IS NOT NULL AND gis_longitude IS NOT NULL
        THEN CONCAT(gis_latitude, ',', gis_longitude)
        ELSE NULL
    END AS location_coordinates,
    gis_nta_name
FROM {{ source('building_permits', 'external_table') }}
