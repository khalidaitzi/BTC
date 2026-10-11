
-- Use the `ref` function to select from other models

select *
from {{ ref('khalid') }}
where id = 1
