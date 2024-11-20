SELECT
    arrayElement(user_properties.up_value, arrayFirstIndex(x -> x = 'country', user_properties.up_key)) AS country,
    arrayElement(user_properties.up_value, arrayFirstIndex(x -> x = 'platform', user_properties.up_key)) AS platform,
    uniq(session_id) AS sessions,
    SUM(event_type = 'casino_filter_field') AS event_search,
    SUM(CASE
        WHEN event_type = 'casino_game' AND arrayElement(event_properties.ep_value, arrayFirstIndex(x -> x = 'category_name', event_properties.ep_key)) = 'search' THEN 1
        ELSE 0
    END) AS casino_game,
    SUM(CASE
            WHEN time_open BETWEEN server_upload_time AND server_upload_time + INTERVAL 10 MINUTE AND  is_bet = 1 THEN 1
        ELSE 0 END) AS is_bet
FROM holistic.amplitude_1win a

LEFT JOIN (
        SELECT DISTINCT
            toUInt64(user_id) AS user_id,
            time_open,
            1 AS is_bet
        FROM
            holistic.bets_analytics AS dg
        WHERE
            time_open >= '2024-09-16'
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
        )  AS b
ON toUInt64(a.user_id) = toUInt64(b.user_id)
WHERE --time_open BETWEEN server_upload_time AND server_upload_time + INTERVAL 1 MINUTE AND
server_upload_time >= '2024-09-16'
    AND event_type IN ('casino_filter_field', 'casino_game')
    AND user_id NOT IN (SELECT DISTINCT toUInt64(user_id)
                               FROM holistic.ma_users_meta_1win
                               WHERE fake_account = true
                                  OR withdrawal_block = true
                                  OR `1win_tester` = true
                                  OR sb_users = true
                                  OR cash_test = true
                                  OR cash_agent = true
                                  OR user_demo_withdrawal = true
                                  OR dd_fm_partner_advertising_accounts = true)
GROUP BY
    country,
    platform

SETTINGS
    max_execution_time = 36000000000;