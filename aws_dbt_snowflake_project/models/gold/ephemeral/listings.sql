{{
  config(
    materialized = 'ephemeral',
    )
}}

WITH listings AS 
(
    SELECT 
        listing_id,
        PROPERTY_TYPE,
        CITY,
        COUNTRY,
        PRICE_PER_NIGHT_TAG,
        LISTING_CREATED_AT
    FROM 
        {{ ref('obt') }}
)
SELECT * FROM listings