WITH report_date AS (
    SELECT toDate('2024-09-01') AS start_date,
           toDate('2024-09-31') AS end_date
),
base AS (
    SELECT DISTINCT
        user_id,
        CASE WHEN number_of_f4_dep = 1 THEN 1 ELSE NULL END AS deposit_1,
        CASE WHEN number_of_f4_dep = 2 THEN 1 ELSE NULL END AS deposit_2,
        CASE WHEN number_of_f4_dep = 3 THEN 1 ELSE NULL END AS deposit_3,
        CASE WHEN number_of_f4_dep = 4 THEN 1 ELSE NULL END AS deposit_4
    FROM enriched.payments
    WHERE toDate(time_confirm) BETWEEN (SELECT start_date FROM report_date) AND (SELECT end_date FROM report_date)
        AND status = 1
        AND country = 'Филиппины'
        AND event = 'DEPOSIT'
        AND is_real_operation = TRUE
        AND number_of_f4_dep IN (1, 2, 3, 4)
        AND NOT hasAny(users_marks, [1, 2, 3, 25, 4, 26, 27, 29])
        AND tenant_id = 1
),

base_bet AS (
    SELECT
        user_id,
        game_id,
        SUM(CASE
            WHEN event = 'BET' AND is_refund != 1 THEN amount_converted
            ELSE 0
        END) AS bets_amount,
        SUM(CASE
            WHEN event = 'WIN' AND is_refund != 1 THEN amount_converted
            ELSE 0
        END) AS wins_amount,

        ROUND(wins_amount / NULLIF(bets_amount, 0), 2) AS RTP,

        SUM(CASE
            WHEN event = 'BET' AND is_refund != 1 THEN 1
            ELSE 0
        END) AS bets,

        SUM(CASE
            WHEN event = 'WIN' AND is_refund != 1 THEN 1
            ELSE 0
        END) AS wins
    FROM enriched.casino
    WHERE toDate(datetime) BETWEEN (SELECT start_date FROM report_date) AND (SELECT end_date FROM report_date)
        AND country = 'Филиппины'
        AND event IN ('BET', 'WIN')
        AND status = 1
        AND is_refund != 1
        AND NOT hasAny(users_marks, [1, 2, 3, 25, 4, 26, 27, 29])
        AND tenant_id = 1
    GROUP BY
        user_id,
        game_id
),

next_dep AS (SELECT DISTINCT user_id
            FROM enriched.payments
            WHERE toDate(time_confirm) BETWEEN addDays((SELECT start_date FROM report_date), 30) AND addDays((SELECT end_date FROM report_date), 30)
            AND status = 1
            AND country = 'Филиппины'
            AND event = 'DEPOSIT'
            AND is_real_operation = TRUE
            AND NOT hasAny(users_marks, [1, 2, 3, 25, 4, 26, 27, 29])
            AND tenant_id = 1),

next_bet AS (SELECT DISTINCT user_id
             FROM enriched.casino
             WHERE toDate(datetime) BETWEEN addDays((SELECT start_date FROM report_date), 30) AND addDays((SELECT end_date FROM report_date), 30)
               AND country = 'Филиппины'
               AND event = 'BET'
               AND status = 1
               AND is_refund != 1
               AND NOT hasAny(users_marks, [1, 2, 3, 25, 4, 26, 27, 29])
               AND tenant_id = 1)

    SELECT
    *
    FROM base bs
    LEFT JOIN base_bet b ON bs.user_id = b.user_id

    WHERE
        user_id GLOBAL NOT IN (SELECT user_id FROM next_dep)
        OR
        user_id GLOBAL NOT IN (SELECT user_id FROM next_bet)

SETTINGS max_execution_time = 36000000000;
