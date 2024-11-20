SELECT
    country,
    uniq(amplitude_id) as users
from (
         SELECT DISTINCT amplitude_id,
                         arrayElement(user_properties.up_value,
                                      arrayFirstIndex(x -> x = 'country', user_properties.up_key)) AS country
         FROM holistic.amplitude_1win
         WHERE server_upload_time >= '2024-08-01'
           AND event_type IN ('registration_form_view', 'registration_tab_change', 'registration_password_field',
                              'registration_email_field', 'registration_phone_field')
           AND event_type NOT IN ('registration_success', 'registration_submit')
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
         )
GROUP BY
    country
SETTINGS
    max_execution_time = 36000000000;