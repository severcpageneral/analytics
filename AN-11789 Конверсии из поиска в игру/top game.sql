WITH report_date AS (
    SELECT toDate('2024-09-20') AS report_date
)

SELECT
    arrayElement(event_properties.ep_value, arrayFirstIndex(x -> x = 'game_id', event_properties.ep_key)) AS game_id,
    arrayElement(user_properties.up_value, arrayFirstIndex(x -> x = 'country', user_properties.up_key)) AS country,
    arrayElement(user_properties.up_value, arrayFirstIndex(x -> x = 'platform', user_properties.up_key)) AS platform,

    COUNT(*) AS casino_game,
    uniq(user_id) AS users,
    uniq(session_id) AS sessions


FROM holistic.amplitude_1win a

WHERE server_upload_time >= (SELECT report_date FROM report_date)
    AND event_type IN ('casino_game')
    AND arrayElement(event_properties.ep_value, arrayFirstIndex(x -> x = 'category_name', event_properties.ep_key)) = 'search'
    AND user_id NOT IN (
        SELECT DISTINCT toUInt64(user_id)
        FROM holistic.ma_users_meta_1win
        WHERE fake_account = true
            OR withdrawal_block = true
            OR 1win_tester = true
            OR sb_users = true
            OR cash_test = true
            OR cash_agent = true
            OR user_demo_withdrawal = true
            OR dd_fm_partner_advertising_accounts = true
    )
GROUP BY
    game_id,
    country,
    platform

SETTINGS
    max_execution_time = 36000000000;