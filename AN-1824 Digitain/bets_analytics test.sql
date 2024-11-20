WITH
    page_view AS (
        SELECT
            toUInt64(user_id) AS user_id,
            session_id,
            min(server_upload_time) AS session_start,
            min(server_upload_time + INTERVAL 15 MINUTE) AS session_end
        FROM holistic.amplitude_1win
        WHERE
            server_upload_time >= '2024-05-01'
            AND session_id > 0
            AND user_id > 0
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
            AND user_id IN (
                            SELECT DISTINCT
                                user_id
                            FROM holistic.ab_tests_tags
                            WHERE test_name LIKE 'retention_digitain_%')
        GROUP BY
            user_id,
            session_id
    ),
    user_registrations AS (
        SELECT DISTINCT
            user_id,
            --country,
            CASE
                WHEN time_registration <= '2024-04-10' THEN 'Old Users'
                WHEN time_registration > '2024-04-10' THEN 'New Users'
                ELSE 'undefined'
            END AS user_type
        FROM ma_users_1win
    ),

    digitain_bets AS (
        SELECT
            toUInt64(user_id) AS user_id,
            time_open,
            uniq(order_number) AS bets,
            'digitain' as bets_group
        FROM
            digitain_bets_analytics_history AS dg
        LEFT JOIN
            holistic.currency_rates AS cr ON cr.date = toDate(dg.time_open) AND cr.currency = dg.currency
        WHERE
            time_open >= '2024-05-01'
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
            user_id,
            time_open,
            bets_group
    ),

    analytics_bets AS (
        SELECT
            toUInt64(user_id) AS user_id,
            time_open,
            uniq(bet_id) AS bets,
            'bet_analytics' as bets_group
        FROM
            bets_analytics AS dg
        WHERE
            time_open >= '2024-05-01'
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
            user_id,
            time_open,
            bets_group
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
    )

    SELECT
        session_id AS session_id,
        toDate(pv.session_start) AS session_start,
        user_type AS user_type,
        CASE
            WHEN group = 'A' THEN 'bets_analytics'
            WHEN group = 'B' THEN 'digitain'
            ELSE 'Not in test group'
        END AS group,
        tu.test_name AS country,
        CASE
            WHEN bets > 0 AND time_open BETWEEN session_start AND session_end THEN 1
            ELSE 0
        END AS view2bet_per_session,
        SUM(if(time_open > session_start AND time_open < session_end, bets, 0)) AS bets_per_session


    FROM page_view pv
    LEFT JOIN user_registrations ur ON pv.user_id = ur.user_id
    LEFT JOIN all_bets ab           ON pv.user_id = ab.user_id
    LEFT JOIN test_users tu         ON pv.user_id = tu.user_id

    GROUP BY
       session_id,
       session_start,
       session_end,
       time_open,
       user_type,
       group,
       bets,
       country
SETTINGS max_execution_time = 360000000;