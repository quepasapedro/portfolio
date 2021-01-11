__author__ = 'peterking'

import math
import numpy as np
import pandas as pd
import random
from scipy import stats
from simply import redshift
from sys import argv


def pull_random_sample():

    user_query = """
        SELECT user_ref as user_id
        FROM curated.dim_user du
        WHERE user_ref NOT IN (select user_id FROM fraudsters_view)
            AND du.num_open_accounts > 0
            AND DATE_TRUNC('week', du.first_account_open_date)::date
                = DATE_TRUNC('week', DATE_ADD('day', -7, current_date))"""

    # Send query to Redshift, return user_ids in a DataFrame.
    df = redshift(user_query)

    # Split list length in half, floor to account for odd numbers of users.
    experimental_num = math.floor(len(df.user_id.unique())/2)
    control_num = math.floor(len(df.user_id.unique())/2)

    # Create DataFrame of unique user_refs.
    possible = pd.DataFrame(df.user_id.unique(), columns=['user_id'])

    # Pull lists of random users.
    experimental_users = random.sample(list(possible.user_id), experimental_num)
    remaining = possible[~possible.user_id.isin(experimental_users)]
    control_users = random.sample(list(remaining.user_id), control_num)
    # unused = possible[(~possible.user_id.isin(experimental_users))&(~possible.user_id.isin(control_users))]

    return experimental_users, control_users


def get_baseline_activity(experimental_users, control_users):

    """
    :rtype : DataFrame
    """
    activity_baseline_query = """
    SELECT
        user_id,
        avg(balance) AS avg_balance,
        sum(deposit_volume) AS deposits_past90,
        sum(swipe_volume) AS swipe_vol_past90,
        sum(swipe_count) AS swipe_count_past90
    FROM mtr_daily_customer_metrics
    WHERE user_id IN :users
        AND date > DATEADD(day, -90, GETDATE())
    GROUP BY user_id"""

    experimental_tuple = tuple(experimental_users)
    control_tuple = tuple(control_users)

    def get_activity(ids):
        return redshift(activity_baseline_query, params={'users': ids})

    experimental_baseline = get_activity(experimental_tuple)
    control_baseline = get_activity(control_tuple)

    return experimental_baseline, control_baseline


def get_baseline_contact(experimental_users, control_users):

    contact_baseline_query = """
    WITH temp_population AS (
    SELECT
        user_id
    FROM customer_table
    WHERE user_id NOT IN (select user_id FROM fraudsters_view)
        AND user_id in :ids),

    weekly_population AS (
    SELECT DISTINCT
        user_id
    FROM daily_customer_metrics_table
    WHERE user_id IN (SELECT user_id FROM temp_population)
        AND date > DATEADD(day, -30, GETDATE())),

    touches AS (
    SELECT
      t.user_id,
      created_at,
      contact_type,
      thread_id,
      touch_id,
      ROW_NUMBER () OVER(
                    PARTITION BY DATE_TRUNC('week', created_at), thread_id
                    ORDER BY created_at) AS order_within_thread
    FROM(
      SELECT
        CASE WHEN cc.user_id = '' OR cc.user_id IS NULL THEN 'call_id: ' + call_id ELSE cc.user_id END AS user_id,
        c.created_at,
        'call' AS contact_type,
        call_id AS thread_id,
        call_id AS touch_id
      FROM support_phone_call_table c
        JOIN agent_phone_hours_table p ON DATE_TRUNC('hour', c.created_at) = p.hour
        LEFT JOIN customer_contact_table cc ON call_id = contact_uuid
        LEFT JOIN phone_number_lookup x ON c.uniqueid = x.uniqueid
      WHERE COALESCE(c.duration, datediff(seconds, c.created_at, c.updated_at)) > 30
        AND c.src != '8882480632'
        AND (x.lastdata IS NULL OR x.lastdata NOT SIMILAR TO '%(apple|android)%')
        --AND (interface IS NOT NULL AND interface != '') -- answered calls ONLY, remove this line to include all calls
            AND cc.user_id IN (SELECT user_id FROM temp_population)

      UNION ALL

      SELECT
        m.user_uuid AS user_id,
        m.created_at,
        'chat' AS contact_type,
        m.chat_uuid AS thread_id,
        m.uuid AS touch_id
      FROM support_message_table m
      LEFT JOIN support_message_chat_table c on m.chat_uuid = c.uuid
      WHERE subject NOT IN('Apple Pay Verification', 'Android Pay Verification')
        AND "from" IS NULL
            AND m.user_uuid IN (SELECT user_id FROM temp_population)
      ) t
    JOIN population	USING(user_id)
    LEFT JOIN customer_first_activity_table USING(user_id)
    LEFT JOIN monthly_customer_active_state_table s
      ON t.user_id = s.user_id
      AND DATEADD(month, -1, DATE_TRUNC('month', created_at)) = month
    WHERE created_at > DATEADD(day, -30, GETDATE())),

    touches_by_customers AS(
    SELECT
      user_id,
      SUM(CASE WHEN contact_type = 'call' THEN 1 ELSE 0 END) AS count_calls,
      SUM(CASE WHEN contact_type = 'chat' AND order_within_thread = 1 THEN 1 ELSE 0 END) AS count_chats,
      SUM(CASE WHEN contact_type = 'chat' THEN 1 ELSE 0 END) AS count_messages,
      SUM(CASE
        WHEN contact_type = 'chat' THEN 1 ELSE NULL END)/SUM(CASE WHEN contact_type = 'chat'
        AND order_within_thread = 1 THEN 1 ELSE NULL END) AS messages_per_chat
    FROM touches
    WHERE created_at > DATEADD(day, -30, GETDATE())
    GROUP BY 1),

    total_contact_by_user_by_week AS (
    SELECT
        user_id,
        SUM(COALESCE(count_calls, 0) + COALESCE(count_messages, 0)) AS total_contact
    FROM touches_by_customers
    GROUP BY 1)

    SELECT DISTINCT
        n.user_id,
        CASE WHEN total_contact IS NULL THEN 0 ELSE 1 END AS unique_contact,
        CASE WHEN total_contact IS NULL THEN 0 ELSE total_contact END AS total_contact
    FROM weekly_population	n
    LEFT JOIN total_contact_by_user_by_week	w	on n.user_id = w.user_id
    ORDER BY 1
    """

    experimental_tuple = tuple(experimental_users)
    control_tuple = tuple(control_users)

    def get_contact(ids):
        return redshift(contact_baseline_query, params={'ids': ids})

    experimental_contact = get_contact(experimental_tuple)
    control_contact = get_contact(control_tuple)

    return experimental_contact, control_contact


def test_for_equality(experimental_users, control_users):

    experimental_activity, control_activity = get_baseline_activity(experimental_users, control_users)
    print("Pulled baseline activity.")

    balance_p_value = stats.ttest_ind(experimental_activity.avg_balance, control_activity.avg_balance)[1]
    deposits_p_value = stats.ttest_ind(experimental_activity.deposits_past90, control_activity.deposits_past90)[1]
    swipe_vol_p_value = stats.ttest_ind(experimental_activity.swipe_vol_past90, control_activity.swipe_vol_past90)[1]
    swipe_count_p_value = stats.ttest_ind(experimental_activity.swipe_count_past90,
                                          control_activity.swipe_count_past90)[1]
    print("Ran activity stats tests.")

    exp_contact, con_contact = get_baseline_contact(experimental_users, control_users)
    print("Pulled baseline contact.")

    obs = np.array([[exp_contact[exp_contact.unique_contact == 1].user_id.count(),
                     exp_contact[exp_contact.unique_contact == 0].user_id.count()],
                    [con_contact[con_contact.unique_contact == 1].user_id.count(),
                     con_contact[con_contact.unique_contact == 0].user_id.count()]])

    contact_rate_p = stats.chi2_contingency(obs)[1]

    total_contact_p_value = stats.ttest_ind(exp_contact.total_contact, con_contact.total_contact)[1]
    print("Ran contact stats tests.")

    return tuple([balance_p_value, deposits_p_value, swipe_vol_p_value, swipe_count_p_value,
                  contact_rate_p, total_contact_p_value])


def __main__(output_prefix):
    print("Initializing...")

    done = False

    while not done:
        experimental_users, control_users = pull_random_sample()
        print("Pulled {} random users.".format(len(experimental_users)))

        p_value_tuple = test_for_equality(experimental_users, control_users)
        print("Ran all stats tests.")

        redo = False

        print(p_value_tuple)

        for p in p_value_tuple:
            if p <= 0.1:
                redo = True
                print("P-value of {} is too low; re-pulling lists.".format(p))
            else:
                continue

        if not redo:
            done = True
            print("Statistically equivalent lists selected. Saving to CSVs.")
        else:
            continue

    user_query = """
    SELECT
        user_ref,
        full_name,
        email
    FROM curated.dim_user_pii
    WHERE user_ref IN :ids
    """

    experimental_csv = redshift(user_query, params={'ids': tuple(experimental_users)})
    control_csv = redshift(user_query, params={'ids': tuple(control_users)})

    experimental_csv.to_csv("../output/{}_Experimental_Users.csv".format(output_prefix))
    control_csv.to_csv("../output/{}_Control_Users.csv".format(output_prefix))

try:
    output_prefix = argv[1]
except:
    output_prefix = 'Random'

__main__(output_prefix)