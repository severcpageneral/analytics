WITH
    page_view AS (
        SELECT
            toUInt64(user_id) AS user_id,
            min(toDate(server_upload_time)) AS first_page_view_date
            --count(event_type) AS count_page_view
        FROM holistic.amplitude_1win
        WHERE
            server_upload_time >= '2024-04-10'
            AND event_type = 'bets_page_view'
            AND toUInt64(user_id) NOT IN (
                SELECT user_id
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
        GROUP BY
            user_id
    ),

    digitain_bets AS (
        SELECT
            toUInt64(user_id) AS user_id,
            uniq(order_number) AS bets,
            'digitain' as bets_group
        FROM
            digitain_bets_analytics_history AS dg
        LEFT JOIN
            holistic.currency_rates AS cr ON cr.date = toDate(dg.time_open) AND cr.currency = dg.currency
        WHERE
            time_open >= '2024-04-10'
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
        GROUP BY
            user_id
    ),

    analytics_bets AS (
        SELECT
            toUInt64(user_id) AS user_id,
            uniq(bet_id) AS bets,
            'bet_analytics' as bets_group
        FROM
            bets_analytics AS dg
        WHERE
            time_open >= '2024-04-10'
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
        GROUP BY
            user_id
    ),

    all_bets AS (
        SELECT *
        FROM digitain_bets
        UNION ALL
        SELECT *
        FROM analytics_bets
    ),

    test_users AS (
        SELECT DISTINCT
            user_id,
            group,
            test_name
        FROM ab_tests_tags
        WHERE test_name LIKE 'retention_digitain_%'
    ),

    user_registrations AS (
        SELECT DISTINCT
            user_id,
            --country,
            CASE
                WHEN time_registration <= '2024-03-12' THEN 'Old Users'
                WHEN time_registration > '2024-03-12' THEN 'New Users'
                ELSE 'undefined'
            END AS user_type
        FROM ma_users_1win
    )

SELECT
    pv.user_id AS user_id,
    pv.first_page_view_date AS first_page_view_date,
    ur.user_type AS user_type,
    tu.test_name AS country,
    CASE
        WHEN tu.group = 'A' THEN 'bets_analytics'
        WHEN tu.group = 'B' THEN 'digitain'
        ELSE 'Not in test group'
    END AS group,
    CASE
        WHEN ab.bets > 0 THEN 1
        WHEN ab.bets = 0 THEN 0
        ELSE -1
    END AS view2bet,
    ab.bets AS bets
FROM
    page_view pv
LEFT JOIN
    all_bets ab ON pv.user_id = ab.user_id
LEFT JOIN
    test_users tu ON pv.user_id = tu.user_id
LEFT JOIN
    user_registrations ur ON pv.user_id = ur.user_id

SETTINGS max_execution_time = 360000000;