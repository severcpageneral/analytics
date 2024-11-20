WITH
    digitain_bets AS (
        SELECT
            toUInt64(user_id) AS user_id,
            toDate(add_time) AS add_time,
            uniq(order_number) AS bets,
            'digitain' AS source_group,
            ROUND(SUM(toDecimal32(usdt_ggr, 5)), 5) AS ggr
        FROM (
            SELECT DISTINCT
                user_id,
                order_number,
                MIN(time_open) OVER (PARTITION BY user_id) AS add_time,
                (bet_amount * cr.course) AS usdt_bet_amount,
                SUM(coefficient) OVER (PARTITION BY bet_id ORDER BY time_open DESC) AS total_coefficient,
                (profit * cr.course) AS usdt_user_profit_amount,
                usdt_bet_amount * coefficient / total_coefficient AS usdt_bet_amount_per_selection,
                usdt_user_profit_amount * coefficient / total_coefficient AS usdt_user_profit_amount_per_selection,
                usdt_bet_amount_per_selection - usdt_user_profit_amount_per_selection AS usdt_ggr
            FROM
                digitain_bets_analytics_history
            LEFT JOIN
                currency_rates AS cr ON cr.date = toDate(time_open) AND cr.currency = digitain_bets_analytics_history.currency
            WHERE
                time_open >= '2024-03-13'
                AND status <> 0
                AND toUInt64(user_id) NOT IN (
                    SELECT DISTINCT toUInt64(user_id)
                    FROM ma_users_meta_1win
                    WHERE
                    fake_account = true 
                    OR withdrawal_block = true 
                    OR `1win_tester` = true 
                    OR sb_users = true 
                    OR cash_test = true 
                    OR cash_agent = true
                    OR user_demo_withdrawal = true 
                    OR dd_fm_partner_advertising_accounts = true
                )
        )
        GROUP BY
            user_id, add_time
    ),
    analytics_bets AS (
        SELECT 
            toUInt64(user_id) AS user_id,
            toDate(add_time) AS add_time,
            uniq(bet_id) AS bets,
            'bets_analytics' AS source_group,
            SUM(IF(status <> 3, toDecimal64(bet_amount - profit, 10), 0)) AS ggr
        FROM (
             SELECT
                    DISTINCT user_id,
                    bet_id,
                    status,
                    MIN(time_open) OVER (PARTITION BY user_id) AS add_time,
                    MAX(bet_amount_converted) OVER (PARTITION BY bet_id) AS bet_amount,
                    MAX(profit_converted) OVER (PARTITION BY bet_id) AS profit
                FROM bets_analytics
                WHERE time_open >= '2024-03-13'
                    AND user_id > 0
                    AND status <> 0
                     AND toUInt64(user_id) NOT IN (
                         SELECT DISTINCT toUInt64(user_id)
                         FROM holistic.ma_users_meta_1win
                         WHERE
                              withdrawal_block OR
                             fake_account OR
                             1win_tester OR
                             user_demo_withdrawal OR
                             sb_users OR
                             cash_test OR
                            payment_scammers OR
                            dd_fm_partner_advertising_accounts
                     )
            )

    GROUP BY 
        user_id, 
        add_time
    ),
    
    all_bets AS (
        SELECT *
        FROM digitain_bets
        UNION ALL
        SELECT *
        FROM analytics_bets
    ),
    user_registrations AS (
        SELECT DISTINCT
            user_id,
            country,
            CASE
                WHEN time_registration <= '2024-03-12' THEN 'Old Users'
                WHEN time_registration > '2024-03-12' THEN 'New Users'
                ELSE 'undefined'
            END AS user_type
        FROM ma_users_1win
    )
-- Main query
SELECT

    ur.country,
    ab.source_group AS group,
    CASE
        WHEN ab.bets = 0 THEN '0 bets'
        WHEN ab.bets = 1 THEN '1 bets'
        WHEN ab.bets = 2 THEN '2 bets'
        WHEN ab.bets = 3 THEN '3 bets'
        WHEN ab.bets >= 4 THEN '3+ bets'
    END AS users_by_bets_count,

    sum(ab.bets) AS all_bets,
    uniqExact(ab.user_id) AS users
FROM
    all_bets ab
LEFT JOIN
    user_registrations ur ON ab.user_id = ur.user_id
GROUP BY
    ur.country,
    ab.source_group,
    users_by_bets_count

SETTINGS
    max_execution_time = 360000000; 
