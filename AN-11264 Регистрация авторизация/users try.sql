SELECT

    arrayElement(event_properties.ep_value, arrayFirstIndex(x -> x = 'social_method', event_properties.ep_key)) AS social_method,
    arrayElement(user_properties.up_value, arrayFirstIndex(x -> x = 'country', user_properties.up_key)) AS country,
    arrayElement(user_properties.up_value, arrayFirstIndex(x -> x = 'platform', user_properties.up_key)) AS platform,
    uniq(user_id) AS users,
    COUNT(DISTINCT session_id) AS session_count

FROM holistic.amplitude_1win
WHERE server_upload_time >= '2024-09-19'
  AND event_type != 'registration_success'
  AND session_id GLOBAL IN
    (SELECT DISTINCT session_id
     FROM holistic.amplitude_1win
     WHERE server_upload_time >= '2024-09-19'
       AND event_type = 'registration_submit'
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