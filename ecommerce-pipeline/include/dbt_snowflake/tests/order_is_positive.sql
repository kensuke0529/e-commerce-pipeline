select * 
from {{ ref('stg_events') }} 
where 
    total_amount < 0
    or tax_amount < 0
    or shipping_amount < 0
