WITH airports_reorder AS (
    SELECT
        country,                 -- 1st
        region,                  -- 2nd
        faa,
        name           
        city,
        lat,
        lon,
        alt,
        tz,
        dst
    FROM {{ ref('staging_airports') }}
)
SELECT *
FROM airports_reorder
