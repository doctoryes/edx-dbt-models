with enrolls as (

    select * from {{ ref('enrolls_jan') }}
)
SELECT
    verified,
    saw_progress,
    count(*) as cnt_enrolls
from
    enrolls
group by 1,2
