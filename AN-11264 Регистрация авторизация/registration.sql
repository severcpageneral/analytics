WITH result AS (SELECT session_id,
                                --- Блок registration_form_view
                                SUM(CASE
                                    WHEN event_type = 'registration_form_view'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'registration_type',
                                                                         event_properties.ep_key)) = ''
                                        THEN 1
                                    ELSE 0
                                    END) AS registration_form_view_,

                                SUM(CASE
                                    WHEN event_type = 'registration_form_view'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'registration_type',
                                                                         event_properties.ep_key)) = 'new-modal'
                                        THEN 1
                                    ELSE 0
                                    END) AS registration_form_view_new_modal,

                                SUM(CASE
                                    WHEN event_type = 'registration_form_view'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'registration_type',
                                                                         event_properties.ep_key)) = 'full'
                                        THEN 1
                                    ELSE 0
                                    END) AS registration_form_view_full,

                                SUM(CASE
                                    WHEN event_type = 'registration_form_view'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'registration_type',
                                                                         event_properties.ep_key)) = 'social'
                                        THEN 1
                                    ELSE 0
                                    END) AS registration_form_view_social,

                                --- Блок registration_tab_change
                                SUM(CASE
                                    WHEN event_type = 'registration_tab_change'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'registration_type',
                                                                         event_properties.ep_key)) = 'social'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'social_method',
                                                                         event_properties.ep_key)) = 'google'
                                        THEN 1
                                    ELSE 0
                                    END) AS registration_tab_change_google,

                                SUM(CASE
                                    WHEN event_type = 'registration_tab_change'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'registration_type',
                                                                         event_properties.ep_key)) = 'social'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'social_method',
                                                                         event_properties.ep_key)) = 'yandex'
                                        THEN 1
                                    ELSE 0
                                    END) AS registration_tab_change_yandex,

                                SUM(CASE
                                    WHEN event_type = 'registration_tab_change'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'registration_type',
                                                                         event_properties.ep_key)) = 'social'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'social_method',
                                                                         event_properties.ep_key)) = 'telegram'
                                        THEN 1
                                    ELSE 0
                                    END) AS registration_tab_change_telegram,

                                SUM(CASE
                                    WHEN event_type = 'registration_tab_change'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'registration_type',
                                                                         event_properties.ep_key)) = 'social'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'social_method',
                                                                         event_properties.ep_key)) = 'mailru'
                                        THEN 1
                                    ELSE 0
                                    END) AS registration_tab_change_mailru,

                                SUM(CASE
                                    WHEN event_type = 'registration_tab_change'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'registration_type',
                                                                         event_properties.ep_key)) = 'social'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'social_method',
                                                                         event_properties.ep_key)) = 'ok'
                                        THEN 1
                                    ELSE 0
                                    END) AS registration_tab_change_ok,

                                SUM(CASE
                                    WHEN event_type = 'registration_tab_change'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'registration_type',
                                                                         event_properties.ep_key)) = 'social'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'social_method',
                                                                         event_properties.ep_key)) = 'vk'
                                        THEN 1
                                    ELSE 0
                                    END) AS registration_tab_change_vk,

                                SUM(CASE
                                    WHEN event_type = 'registration_tab_change'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'registration_type',
                                                                         event_properties.ep_key)) = 'social'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'social_method',
                                                                         event_properties.ep_key)) = ''
                                        THEN 1
                                    ELSE 0
                                    END) AS registration_tab_change_,

                                -- Блок registration_submit
                                SUM(CASE
                                    WHEN event_type = 'registration_submit'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'registration_type',
                                                                         event_properties.ep_key)) = 'social'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'social_method',
                                                                         event_properties.ep_key)) = 'yandex'
                                        THEN 1
                                    ELSE 0
                                    END) AS registration_submit_yandex,

                                SUM(CASE
                                    WHEN event_type = 'registration_submit'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'registration_type',
                                                                         event_properties.ep_key)) = 'social'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'social_method',
                                                                         event_properties.ep_key)) = 'vk'
                                        THEN 1
                                    ELSE 0
                                    END) AS registration_submit_vk,

                                SUM(CASE
                                    WHEN event_type = 'registration_submit'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'registration_type',
                                                                         event_properties.ep_key)) = 'social'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'social_method',
                                                                         event_properties.ep_key)) = 'mailru'
                                        THEN 1
                                    ELSE 0
                                    END) AS registration_submit_mailru,

                                SUM(CASE
                                    WHEN event_type = 'registration_submit'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'registration_type',
                                                                         event_properties.ep_key)) = 'social'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'social_method',
                                                                         event_properties.ep_key)) = 'steam'
                                        THEN 1
                                    ELSE 0
                                    END) AS registration_submit_steam,

                                SUM(CASE
                                    WHEN event_type = 'registration_submit'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'registration_type',
                                                                         event_properties.ep_key)) = 'social'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'social_method',
                                                                         event_properties.ep_key)) = 'google'
                                        THEN 1
                                    ELSE 0
                                    END) AS registration_submit_google,

                                SUM(CASE
                                    WHEN event_type = 'registration_submit'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'registration_type',
                                                                         event_properties.ep_key)) = 'social'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'social_method',
                                                                         event_properties.ep_key)) = 'telegram'
                                        THEN 1
                                    ELSE 0
                                    END) AS registration_submit_telegram,

                                SUM(CASE
                                    WHEN event_type = 'registration_submit'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'registration_type',
                                                                         event_properties.ep_key)) = 'social'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'social_method',
                                                                         event_properties.ep_key)) = 'ok'
                                        THEN 1
                                    ELSE 0
                                    END) AS registration_submit_ok,

                                SUM(CASE
                                    WHEN event_type = 'registration_submit'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'registration_type',
                                                                         event_properties.ep_key)) = 'social'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'social_method',
                                                                         event_properties.ep_key)) = ''
                                        THEN 1
                                    ELSE 0
                                    END) AS registration_submit_,

                                -- Блок registration_success
                               SUM(CASE
                                    WHEN event_type = 'registration_success'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'registration_type',
                                                                         event_properties.ep_key)) = 'social'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'social_method',
                                                                         event_properties.ep_key)) = 'google'
                                        THEN 1
                                    ELSE 0
                                    END) AS registration_success_google,

                                SUM(CASE
                                    WHEN event_type = 'registration_success'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'registration_type',
                                                                         event_properties.ep_key)) = 'social'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'social_method',
                                                                         event_properties.ep_key)) = 'mailru'
                                        THEN 1
                                    ELSE 0
                                    END) AS registration_success_mailru,

                                SUM(CASE
                                    WHEN event_type = 'registration_success'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'registration_type',
                                                                         event_properties.ep_key)) = 'social'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'social_method',
                                                                         event_properties.ep_key)) = 'ok'
                                        THEN 1
                                    ELSE 0
                                    END) AS registration_success_ok,

                                SUM(CASE
                                    WHEN event_type = 'registration_success'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'registration_type',
                                                                         event_properties.ep_key)) = 'social'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'social_method',
                                                                         event_properties.ep_key)) = 'steam'
                                        THEN 1
                                    ELSE 0
                                    END) AS registration_success_steam,

                                SUM(CASE
                                    WHEN event_type = 'registration_success'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'registration_type',
                                                                         event_properties.ep_key)) = 'social'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'social_method',
                                                                         event_properties.ep_key)) = 'telegram'
                                        THEN 1
                                    ELSE 0
                                    END) AS registration_success_telegram,

                                SUM(CASE
                                    WHEN event_type = 'registration_success'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'registration_type',
                                                                         event_properties.ep_key)) = 'social'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'social_method',
                                                                         event_properties.ep_key)) = 'vk'
                                        THEN 1
                                    ELSE 0
                                    END) AS registration_success_vk,

                                SUM(CASE
                                    WHEN event_type = 'registration_success'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'registration_type',
                                                                         event_properties.ep_key)) = 'social'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'social_method',
                                                                         event_properties.ep_key)) = 'yandex'
                                        THEN 1
                                    ELSE 0
                                    END) AS registration_success_yandex,

                                SUM(CASE
                                    WHEN event_type = 'registration_success'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'registration_type',
                                                                         event_properties.ep_key)) = 'social'
                                        AND arrayElement(event_properties.ep_value,
                                                         arrayFirstIndex(x -> x = 'social_method',
                                                                         event_properties.ep_key)) = ''
                                        THEN 1
                                    ELSE 0
                                    END) AS registration_success_

                FROM holistic.amplitude_1win
                WHERE server_upload_time >= '2024-09-05'
                  AND event_type IN ('registration_form_view', 'registration_tab_change', 'registration_submit',
                                     'registration_success', 'registration_form_exit')
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
                    session_id
)
SELECT
    COUNT(session_id) AS sessions,
    registration_form_view_ AS registration_form_view_,
    registration_form_view_new_modal AS registration_form_view_new_modal,
    registration_form_view_full AS registration_form_view_full,
    registration_form_view_social AS registration_form_view_social,
    registration_tab_change_google AS registration_tab_change_google,
    registration_tab_change_yandex AS registration_tab_change_yandex,
    registration_tab_change_telegram AS registration_tab_change_telegram,
    registration_tab_change_mailru AS registration_tab_change_mailru,
    registration_tab_change_ok AS registration_tab_change_ok,
    registration_tab_change_vk AS registration_tab_change_vk,
    registration_tab_change_ AS registration_tab_change_,
    registration_submit_yandex AS registration_submit_yandex,
    registration_submit_vk AS registration_submit_vk,
    registration_submit_mailru AS registration_submit_mailru,
    registration_submit_steam AS registration_submit_steam,
    registration_submit_google AS registration_submit_google,
    registration_submit_telegram AS registration_submit_telegram,
    registration_submit_ok AS registration_submit_ok,
    registration_submit_ AS registration_submit_,
    registration_success_google AS registration_success_google,
    registration_success_mailru AS registration_success_mailru,
    registration_success_ok AS registration_success_ok,
    registration_success_steam AS registration_success_steam,
    registration_success_telegram AS registration_success_telegram,
    registration_success_vk AS registration_success_vk,
    registration_success_yandex AS registration_success_yandex,
    registration_success_ AS registration_success_
FROM result
GROUP BY
    registration_form_view_ AS registration_form_view_,
    registration_form_view_new_modal AS registration_form_view_new_modal,
    registration_form_view_full AS registration_form_view_full,
    registration_form_view_social AS registration_form_view_social,
    registration_tab_change_google AS registration_tab_change_google,
    registration_tab_change_yandex AS registration_tab_change_yandex,
    registration_tab_change_telegram AS registration_tab_change_telegram,
    registration_tab_change_mailru AS registration_tab_change_mailru,
    registration_tab_change_ok AS registration_tab_change_ok,
    registration_tab_change_vk AS registration_tab_change_vk,
    registration_tab_change_ AS registration_tab_change_,
    registration_submit_yandex AS registration_submit_yandex,
    registration_submit_vk AS registration_submit_vk,
    registration_submit_mailru AS registration_submit_mailru,
    registration_submit_steam AS registration_submit_steam,
    registration_submit_google AS registration_submit_google,
    registration_submit_telegram AS registration_submit_telegram,
    registration_submit_ok AS registration_submit_ok,
    registration_submit_ AS registration_submit_,
    registration_success_google AS registration_success_google,
    registration_success_mailru AS registration_success_mailru,
    registration_success_ok AS registration_success_ok,
    registration_success_steam AS registration_success_steam,
    registration_success_telegram AS registration_success_telegram,
    registration_success_vk AS registration_success_vk,
    registration_success_yandex AS registration_success_yandex,
    registration_success_ AS registration_success_
SETTINGS
    max_execution_time = 36000000000;