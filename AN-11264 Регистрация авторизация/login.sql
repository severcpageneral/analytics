SELECT
    social_method,
    uniq(session_id) AS unique_sessions,
    SUM(event_login_submit) AS total_login_submit,
    SUM(event_login_success) AS total_login_success,
    ROUND(SUM(event_login_success) / NULLIF(SUM(event_login_submit), 0), 2) AS submit2success
FROM (
         SELECT session_id,
                arrayElement(user_properties.up_value, arrayFirstIndex(x -> x = 'country', user_properties.up_key)) AS country,
                arrayElement(user_properties.up_value, arrayFirstIndex(x -> x = 'platform', user_properties.up_key)) AS platform,
                arrayElement(event_properties.ep_value,
                             arrayFirstIndex(x -> x = 'social_method', event_properties.ep_key)) AS login_submit_socialmetod,
                SUM(event_type = 'login_social_method') AS event_login_submit,
                arrayElement(event_properties.ep_value,
                             arrayFirstIndex(x -> x = 'login_social_method', event_properties.ep_key)) AS login_success_socialmetod,
                SUM(event_type = 'login_success') AS event_login_success,
                IF(login_submit_socialmetod != '',
                   login_submit_socialmetod,
                   IF(login_success_socialmetod != '', login_success_socialmetod, '(not set)')) AS social_method
         FROM holistic.amplitude_1win
         WHERE server_upload_time >= '2024-08-16'
           AND event_type IN ('login_social_method', 'login_success')
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
             session_id,
             country,
             platform,
             login_success_socialmetod,
             login_submit_socialmetod
     )
GROUP BY
    social_method,
    country,
    platform
SETTINGS
    max_execution_time = 36000000000;
