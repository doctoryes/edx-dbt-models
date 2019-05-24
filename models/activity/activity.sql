--User activities within a course.
with enterprise_users as
(
  select a.user_id, a.enterprise_customer_id as enterprise_id, b.name as enterprise_name
  from app_data.wwc.enterprise_enterprisecustomeruser a
  left join app_data.wwc.enterprise_enterprisecustomer b on a.enterprise_customer_id = b.uuid
)
select
  f.user_id, e.enrollment_id, e.enroll_timestamp, u.enterprise_id, u.enterprise_name,
  f.course_id, f.date, f.cnt_active_activity, f.cnt_engaged_activity, f.cnt_video_activity,
  {{ is_gt_zero('cnt_active_activity') }} as has_active_activity
from business_intelligence.production.user_activity_engagement_daily f
--Filter to only activities from enterprise enrollments.
inner join {{ ref('enrollment') }} e using(user_id, course_id)
left join enterprise_users u using(user_id)
where date between '2019-01-01' and '2019-01-07'
