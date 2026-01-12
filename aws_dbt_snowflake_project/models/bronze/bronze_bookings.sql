--{%set incremental_load = 1%}
--{%set incremental_col = 'CREATED_AT'%}

{{ config (materialized = 'incremental')}}

SELECT * FROM {{ source('staging','bookings') }} 

--{% if incremental_load == 1 %}
{% if is_incremental()%}
--WHERE {{ incremental_col}} > (SELECT COALESCE(MAX({{incremental_col}}), '1900-01-01') FROM {{ this }})
--{% endif %}

    WHERE CREATED_AT > (SELECT COALESCE(MAX({{incremental_col}}), '1900-01-01') FROM {{ this }})
{% endif %}