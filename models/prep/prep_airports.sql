WITH airports_reorder AS (
    SELECT
        country,
        region,
        city,
        state,
        faa,
        name,
        lat,
        lon,
        alt,
        tz,
        dst
    FROM {{ ref('staging_airports') }}
)
SELECT *
FROM airports_reorder

