SELECT
    arrayElement(event_properties.ep_value, arrayFirstIndex(x -> x = 'social_method', event_properties.ep_key)) AS social_method,
    arrayElement(user_properties.up_value, arrayFirstIndex(x -> x = 'country', user_properties.up_key)) AS country,
    arrayElement(user_properties.up_value, arrayFirstIndex(x -> x = 'platform', user_properties.up_key)) AS platform,
    COUNT(DISTINCT session_id) AS session_count,
    SUM(event_type = 'registration_submit') AS registration_submit,
    SUM(event_type = 'registration_success') AS registration_success,
    ROUND(SUM(event_type = 'registration_success')  / NULLIF(SUM(event_type = 'registration_submit'), 0), 2) AS submit2success,
    ROUND(SUM(event_type = 'registration_success')  / NULLIF(COUNT(DISTINCT session_id), 0), 2) AS session2success --Кол-во успешных success на сессию
FROM holistic.amplitude_1win
WHERE server_upload_time >= '2024-09-17'
  AND event_type IN ('registration_form_view', 'registration_tab_change', 'registration_submit', 'registration_success')
  AND session_id GLOBAL IN
    (SELECT DISTINCT session_id
     FROM holistic.amplitude_1win
     WHERE server_upload_time >= '2024-09-17'
       AND event_type IN ('registration_form_view', 'registration_tab_change')
       AND arrayElement(event_properties.ep_value, arrayFirstIndex(x -> x = 'registration_type', event_properties.ep_key)) = 'social'
       AND user_id NOT IN
         (SELECT DISTINCT toUInt64(user_id)
          FROM holistic.ma_users_meta_1win
          WHERE fake_account = true
            OR withdrawal_block = true
            OR `1win_tester` = true
            OR sb_users = true
            OR cash_test = true
            OR cash_agent = true
            OR user_demo_withdrawal = true
            OR dd_fm_partner_advertising_accounts = true))
GROUP BY
    country,
    social_method,
    platform
SETTINGS
    max_execution_time = 36000000000;
