--Enterprise learner enrollments and related information.
with enterprise_enrollments as
(
  select * from {{ ref('enrollment_base') }}
),
enterprise_users as
(
  select a.user_id, a.enterprise_customer_id as enterprise_id, b.name as enterprise_name
  from app_data.wwc.enterprise_enterprisecustomeruser a
  left join app_data.wwc.enterprise_enterprisecustomer b on a.enterprise_customer_id = b.uuid
),
enterprise_offers as
(
  select
    ent_cond_off.id as offer_id, ent_cond.id as condition_id, order_type,
    ent_cond.enterprise_customer_name, ent_cond.enterprise_customer_uuid
  from app_data.ecommerce.offer_conditionaloffer ent_cond_off
  inner join
  (--Only keep enterprise conditions
   --Offer conditions
    select id, enterprise_customer_name, enterprise_customer_uuid, 'offer' as order_type
    from app_data.ecommerce.offer_condition
    where enterprise_customer_uuid is not null
    union
    --Coupon conditions
    select
      a.id, c.name as enterprise_customer_name, c.uuid as enterprise_customer_uuid,
      'coupon' as order_type
    from app_data.ecommerce.offer_condition a
    inner join app_data.ecommerce.offer_range b
    on a.range_id = b.id
    inner join app_data.wwc.enterprise_enterprisecustomer c
    on b.enterprise_customer = c.uuid
  ) ent_cond on ent_cond_off.condition_id = ent_cond.id
),
enterprise_discounts as
(
  select order_id, offer_id, amount as discount_amount
  from app_data.ecommerce.order_orderdiscount ord_dis
  where offer_id in (select offer_id from enterprise_offers)
),
enterprise_orders as
(
  select
    orders.id as order_id, orders.total_incl_tax as order_paid_amount,
    orders.date_placed as order_timestamp, enroll_map.enrollment_id
  from app_data.ecommerce.order_order orders
  left join
  (--Get enrollment ID associated with the order
    select enrollment_id, value as order_number --IDs here start with partner code, like 'EDX-'
    from app_data.wwc.student_courseenrollmentattribute
    where namespace = 'order' and name = 'order_number' and regexp_like(value, '^EDX-')
  ) enroll_map on orders.number = enroll_map.order_number
  where
    orders.id in (select order_id from enterprise_discounts)
    and orders.status = 'Complete' and orders.currency = 'USD'
)
select
  e.enrollment_id, e.enroll_timestamp,
  e.user_id, e.enterprise_user_id,
  e.course_id,
  u.enterprise_name, u.enterprise_id,
  o.order_id, o.order_timestamp, o.order_paid_amount, d.discount_amount,
  o.order_paid_amount + d.discount_amount as order_total_amount
from enterprise_enrollments e
left join enterprise_orders o using(enrollment_id)
left join enterprise_users u using(user_id)
left join enterprise_discounts d using(order_id)
where
  enterprise_name != 'edX, Inc.'
  --Exclude partners
  and u.enterprise_id not in (
    '6bf0bf67f09d4147bea3b10707a5e611', --Georgia Tech
    'cd6feafff2444e0e8520e2f541dfe34c', --RWTH Aachen
    'efe2bce75f164e7eb6a9728e53245201' --RWTH Aachen (Staging)
  )
