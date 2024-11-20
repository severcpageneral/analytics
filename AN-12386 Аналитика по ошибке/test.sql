WITH report_date AS (
    SELECT toDate('2024-10-10') AS report_date
),

reg AS(
SELECT DISTINCT
    session_id
FROM holistic.amplitude_1win a
WHERE server_upload_time >= (SELECT report_date FROM report_date)
    AND event_type = 'registration_success'
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
    AND user_id > 0
GROUP BY
    session_id
)

SELECT
    arrayElement(user_properties.up_value, arrayFirstIndex(x -> x = 'country', user_properties.up_key)) AS country,
    lower(arrayElement(user_properties.up_value, arrayFirstIndex(x -> x = 'platform', user_properties.up_key))) AS platform,
    uniq(user_id) as users,
    uniq(session_id) as sessions
FROM holistic.amplitude_1win a
WHERE session_id GLOBAL IN (SELECT session_id
                     FROM reg)
    AND event_type = 'registration_error'
    AND arrayElement(event_properties.ep_value, arrayFirstIndex(x -> x = 'registration_error_text', event_properties.ep_key)) = 'credentials.phoneOrEmailExists'
GROUP BY
    country,
    platform

SETTINGS
    max_execution_time = 36000000000;