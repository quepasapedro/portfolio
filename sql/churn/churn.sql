DROP TABLE public.peter_churn CASCADE;
DROP VIEW peter_churn_rate;
DROP VIEW churn_summary_peter;
DROP VIEW peter_churn_status;
DROP VIEW peter_churn_revenue;

CREATE TEMP TABLE closed_accounts
DISTKEY (user_id)
SORTKEY (user_id, month)
AS (
    SELECT
        DATE_TRUNC('month', change_timestamp)::DATE AS month,
        user_id,
        1 AS closed
    FROM current_user_status_table
    WHERE account_state_id = 'retired'
);

CREATE TEMP TABLE revenue
DISTKEY (user_id)
SORTKEY (user_id, month)
AS (
    SELECT
        DATE_TRUNC('month', date)::DATE AS month,
        user_id,
        SUM(revenue_est) AS revenue_est
    FROM daily_financial_activity_table
    WHERE date < DATE_TRUNC('month', current_date)::DATE
    GROUP BY 1, 2
);

CREATE TEMP TABLE ma_revenue
DISTKEY (user_id)
SORTKEY (user_id, month)
AS (
    SELECT
        month,
        user_id,
        revenue_est,
        AVG(revenue_est) OVER (
            PARTITION BY user_id
            ORDER BY month ASC
            ROWS 2 PRECEDING
        ) AS ma_revenue,
        STDDEV_POP(revenue_est) OVER (
            PARTITION BY user_id 
            ORDER BY month ASC 
            ROWS 2 PRECEDING) as stddev_revenue,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY month ASC
        ) AS months_enrolled,
        COALESCE(closed, 0) AS closed
    FROM revenue
    LEFT JOIN closed_accounts USING (month, user_id)
);

CREATE TEMP TABLE ma_delta
DISTKEY (user_id)
SORTKEY (user_id, month)
AS (
    SELECT
        month,
        user_id,
        months_enrolled,
        revenue_est,
        LAG(revenue_est, 1) OVER (
            PARTITION BY user_id
            ORDER BY month
        ) AS prev_revenue_est,
        ma_revenue,
        LAG(ma_revenue, 1) OVER (
            PARTITION BY user_id
            ORDER BY month
        ) AS prev_ma_revenue,
        stddev_revenue,
        LAG(stddev_revenue, 1) OVER (
            PARTITION BY user_id
            ORDER BY month
        ) AS prev_stddev_revenue,
        (ma_revenue - (2*stddev_revenue)) as low_bound,
        closed
    FROM ma_revenue
);

CREATE TABLE public.peter_churn
DISTKEY (user_id)
SORTKEY (user_id, month)
AS (
    SELECT 
        month, user_id, months_enrolled, revenue_est, prev_revenue_est,
        delta_revenue_est, retained_revenue_est, revenue_category, status,
        --stddev_revenue, low_bound, stddev_diff,
        /*
         * When a customer closes their account and hasn't previously churned then consider them churned
         * When a customer closed their account but has previously churned do not consider their account closing as another churn event
         */
        churned,
        LAG(status, 1) OVER(PARTITION BY user_id ORDER BY month) as prev_status,
        CASE
            WHEN closed = 1 AND churn_events = 0 THEN 1
            WHEN status = 'churned'
              AND churn_events = SUM(churned) OVER (PARTITION BY user_id) THEN 1
            ELSE 0
        END AS last_churn
    FROM (
        SELECT month, user_id, months_enrolled, revenue_est, prev_revenue_est,
            delta_revenue_est, retained_revenue_est, revenue_category, status,
            churned, closed,
            /*
             * Calculate the number of churn events for users that might have
             * churned, reactivated, and churned again
             */
            SUM(churned) OVER (
                PARTITION BY user_id
                ORDER BY month
                ROWS UNBOUNDED PRECEDING
            ) AS churn_events
        FROM (
            SELECT
                month,
                user_id,
                months_enrolled,
                revenue_est,
                COALESCE(prev_revenue_est, 0) AS prev_revenue_est,
                revenue_est - COALESCE(prev_revenue_est, 0) AS delta_revenue_est,
                CASE
                    WHEN months_enrolled <= 3 THEN NULL
                    WHEN months_enrolled > 3 AND revenue_est >= prev_revenue_est THEN prev_revenue_est
                    ELSE revenue_est
                END AS retained_revenue_est,
                CASE
                    WHEN months_enrolled <= 3 THEN 'new revenue'
                    WHEN months_enrolled > 3 AND revenue_est >= prev_revenue_est THEN 'expansion revenue'
                    WHEN months_enrolled > 3 AND revenue_est <  prev_revenue_est THEN 'churn revenue'
                    ELSE NULL
                END AS revenue_category,
                CASE
                    WHEN months_enrolled <= 3 THEN 'new'
                    WHEN months_enrolled > 3 AND ma_revenue = 0 AND prev_ma_revenue != 0 THEN 'churned'
                    WHEN months_enrolled > 3 AND ma_revenue = 0 AND prev_ma_revenue = 0 THEN 'dormant'
                    ELSE 'in use'
                END AS status,
                CASE
                    WHEN months_enrolled > 3 AND ma_revenue = 0 AND prev_ma_revenue != 0
                    THEN 1 ELSE 0
                END AS churned,
                closed
            FROM ma_delta
        )
    )
);

CREATE VIEW peter_churn_rate
AS (
    SELECT month, AVG(churned::DECIMAL(6, 5)) AS churn_rate
    FROM public.peter_churn 
    WHERE status != 'dormant'
    GROUP BY month
);

CREATE VIEW peter_churn_status AS (
    SELECT 
      month,
      SUM(new_customers) AS new_customers,
      SUM(retained_customers) AS retained_customers,
      SUM(churned_customers) AS churned_customers,
      SUM(dormant_customers) AS dormant_customers,
      SUM(users) AS total_customers
    FROM (
        SELECT 
            month, 
            users,
            CASE WHEN status = 'new'     THEN users ELSE 0 END AS new_customers,
            CASE WHEN status = 'in use'  THEN users ELSE 0 END AS retained_customers,
            CASE WHEN status = 'churned' THEN users ELSE 0 END AS churned_customers,
            CASE WHEN status = 'dormant' THEN users ELSE 0 END AS dormant_customers
        FROM (
            SELECT 
              month, 
              status, 
              COUNT(DISTINCT user_id) AS users
            FROM mtr_churn
            GROUP BY month, status)
    )
    GROUP BY month
);

CREATE VIEW peter_churn_revenue AS (
    SELECT
        month,
        SUM(revenue_est) AS revenue_est,
        SUM(retained_revenue_est) AS retained_revenue_est,
        SUM(new_revenue) AS new_revenue,
        SUM(expansion_revenue) AS expansion_revenue,
        SUM(churn_revenue) AS churn_revenue
    FROM (
        SELECT 
            month, 
            revenue_est, 
            retained_revenue_est,
            CASE
                WHEN revenue_category = 'new revenue'
                THEN revenue_est ELSE 0
            END AS new_revenue,
            CASE
                WHEN revenue_category = 'expansion revenue'
                THEN delta_revenue_est ELSE 0
            END AS expansion_revenue,
            CASE
                WHEN revenue_category = 'churn revenue'
                THEN delta_revenue_est ELSE 0
            END AS churn_revenue
        FROM mtr_churn
    )
    GROUP BY month
);

CREATE VIEW peter_churn_summary
AS (
    SELECT 
        month, churn_rate, new_customers, retained_customers,
        churned_customers, dormant_customers, revenue_est, retained_revenue_est,
        new_revenue, expansion_revenue, churn_revenue
    FROM peter_churn_rate
    JOIN peter_churn_status USING (month)
    JOIN peter_churn_revenue USING (month)
);

GRANT SELECT ON public.peter_churn TO GROUP moderaterisk;
GRANT SELECT ON peter_churn_summary TO GROUP moderaterisk;
GRANT SELECT ON peter_churn_summary TO GROUP financemetrics;

DROP TABLE closed_accounts;
DROP TABLE revenue;
DROP TABLE ma_revenue;
DROP TABLE ma_delta;
