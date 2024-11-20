WITH reg AS (
    SELECT
        user_id,
        arrayElement(
            user_properties.up_value,
            arrayFirstIndex(x -> x = 'country', user_properties.up_key)
        ) as country,
        min(server_upload_time) as time_registration
    FROM holistic.amplitude_1win
    WHERE event_type = 'registration_form_view'
    AND toDate(server_upload_time) BETWEEN today() - 30 AND today() - 1
    AND arrayElement(
            user_properties.up_value,
            arrayFirstIndex(x -> x = 'country', user_properties.up_key)
        )  in ('India', 'Turkey')
    AND toUInt64(user_id) NOT IN
        (SELECT DISTINCT user_id
            FROM holistic.ma_users_meta_1win
            WHERE fake_account = true
            OR withdrawal_block = true
            OR `1win_tester` = true
            OR sb_users = true
            OR cash_test = true
            OR user_demo_withdrawal = true
            OR dd_fm_partner_advertising_accounts = true)
    GROUP BY
        user_id,
        country
)

SELECT
    reg.user_id as user_id,
    reg.country as country,
    reg.time_registration as time_registration,
    p.first_dep as first_dep,

FROM reg

LEFT JOIN (SELECT
                user_id,
                min(time_confirm) as first_dep
            FROM
            enriched.payments
            WHERE p.status = 1
            AND p.event = 'DEPOSIT'
            AND p.is_real_operation = True
            AND toDate(p.time_confirm) BETWEEN today() - 30 AND today() - 1
            GROUP BY user_id
            ) p ON reg.user_id = p.user_id

--WHERE toDate(p.first_dep) IS NOT NULL

SETTINGS
max_execution_time = 36000000000;