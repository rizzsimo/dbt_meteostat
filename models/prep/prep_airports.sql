WITH airports_reorder AS (

    SELECT
          country
        , region
        , faa
        , name
        , city
        , state
        , lat
        , lon
        , alt
        , tz
        , dst
    FROM {{ ref('staging_airports') }}

)

SELECT *
FROM airports_reorder;
