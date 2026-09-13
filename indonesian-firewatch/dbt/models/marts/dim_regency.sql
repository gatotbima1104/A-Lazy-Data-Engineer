select

    regency_id, province_id, regency_name

from {{ ref('stg_regency') }}