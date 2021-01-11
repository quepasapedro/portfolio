-- This query finds the total number of users enrolled on a given date.
-- Uses dim_date as the from-entry table, since some days may have 0 enrolled users. 
--  Grain for dim_date: one row per day.

select 
  *  
from (
  select 
    dd.date, 
    count(distinct du.user_id) as num_users,
    sum(count(distinct du.user_id)) over(order by date asc rows unbounded preceding) as cumulative_users
  from curated.dim_date dd
  left join curated.dim_user du 
    on du.customer_created_date = dd.date
    and du.is_test_user = False -- Excludes internal accounts; employees, investors, etc. 
  where dd.date >= '2017-01-01'
  group by 1)
where cumulative_users > 0
  and date <= current_date
order by 1 asc
;
