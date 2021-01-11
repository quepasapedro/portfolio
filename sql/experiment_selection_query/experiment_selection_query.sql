-- Peter King
-- 2020-02-19 

-- In order to collect anecdotal/qualitative usability feedback on a new feature, this query identifies a list of experienced feature users. 
-- List criteria:
--   * Users are not fraudsters, frozen, locked, etc. 
--   * Users are not employees
--   * Users have been enrolled at least 6 months as of date of list pull
--   * In the previous month (in this case, Jan 2020), the user had:
--     * At least THREE Expenses which were:
--       * Un-archived
--       * Spent from at least once
--       * Has a funding schedule


with goal_transaction_months as (
  select distinct
      du.user_ref,
      dg.goal_ref,
      dg.purpose,
      dg.created_ts,
      dg.name,
      date_trunc('month', fgt.created_ts)::date as transaction_month
    from curated.goal_table dg
    join curated.user_table du on du.user_ref = dg.primary_user_ref
    join curated.goal_transaction_table fgt on fgt.goal_ref = dg.goal_ref
    where du.cohort_date <= date_add('month', -6, date_trunc('month', current_date)) -- Exclude users enrolled w/in last 6 months.
      and dg.has_named_frequency = True
      and is_archived = False   -- Active, unarchived Expenses
      and purpose = 'EXPENSE'   -- It's an Expense, rather than a Goal
      and is_associated = True  -- Include goal transactions which are spending associations
  )

select --count(distinct user_ref) -- Uncomment to get full user count
  dup.user_ref,
  dup.tracking_id,
  dup.email,
  dup.preferred_name,
  dup.first_name
from curated.dim_user_pii dup
join (
    select
      fum.user_ref,
      fum.month_date,
      count(distinct gtl.goal_ref) as goals,
      case 
        when sum((goals >= 3)::int) over(partition by fum.user_ref order by fum.month_date rows 2 preceding) >= 3 
          then true 
          else false 
      end as rolling_expense_use
    from curated.monthly_user_table fum
    left join goal_transaction_months gtl
        on gtl.user_ref = fum.user_ref
        and gtl.transaction_month = fum.month_date
    where fum.num_open_accounts > 0
    group by 1, 2) a using(user_ref)
where rolling_expense_use = True                                                -- Meets the criteria for selection
  and month_date = date_add('month', -1, date_trunc('month', current_date))     -- Last month
  and is_employee = False                                                       -- Don't want employees
  and is_ach_velocity_locked = FALSE                                            -- Exclude frozen/fraudster, etc.
  and is_ach_linked_account_locked = FALSE
  and is_fraudster = FALSE
  and is_frozen = FALSE
order by random()                                                               -- Randomize the list
limit 1000                                                                      -- Limit output to 1,000 randomly-ordered users.
;
