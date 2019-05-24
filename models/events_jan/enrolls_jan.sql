select
    jer.user_id,
    jer.course_id,
    d_user_course.first_verified_enrollment_time is not null as verified,
    to_boolean(sum(case when url like '%/progress%' then 1 else 0 end)) as saw_progress
from
    {{ ref('events_jan') }} as jer
left join
    production.production.d_user_course d_user_course
on
    jer.user_id = d_user_course.user_id::varchar
and
    jer.course_id = d_user_course.course_id
where
    jer.received_at >= '2018-01-01'
and
    jer.event_type = 'page'
and
    jer.user_id is not null
and
    jer.course_id is not null
group by 1,2,3
