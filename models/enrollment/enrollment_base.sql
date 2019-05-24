{{ config(materialized='ephemeral')}}

select
  sce.id as enrollment_id, ecu.user_id, ece.enterprise_customer_user_id as enterprise_user_id,
  ece.course_id, ece.created as enroll_timestamp
from app_data.wwc.enterprise_enterprisecourseenrollment ece
left join app_data.wwc.enterprise_enterprisecustomeruser ecu on ecu.id = ece.enterprise_customer_user_id
left join app_data.wwc.student_courseenrollment sce on sce.user_id = ecu.user_id and sce.course_id = ece.course_id
