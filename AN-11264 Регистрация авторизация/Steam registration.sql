WITH report_date AS (
    SELECT toDate('2024-09-01') AS report_date
),

deposit AS (
    SELECT
        user_id,
        COUNT(*) as deposit_count,
        ROUND(SUM(amount_converted), 2) AS deposit_amount
    FROM
        enriched.payments od
    WHERE
        toDate(time_confirm) >= (SELECT report_date FROM report_date)
        AND status = 1
        AND event = 'DEPOSIT'
        AND is_real_operation = TRUE
        AND number_of_f4_dep != 1
        AND hasAny([1, 2, 3, 25, 4, 26, 27, 29], users_marks) = FALSE
    GROUP BY
        user_id
),

bet AS (
    SELECT
        user_id,
        SUM(CASE
            WHEN event = 'BET' AND is_refund != 1 THEN 1
            ELSE 0
        END) AS bets
    FROM enriched.casino
    WHERE
        datetime >= (SELECT report_date FROM report_date)
        AND country = 'Филиппины'
        AND event = 'BET'
        AND status = 1
        AND is_refund != 1
        AND hasAny(
            [
                1,  -- withdrawal_block
                2,  -- 1win_tester
                3,  -- fake_account
                25, -- sb_users
                4,  -- User_demo_withdrawal
                26, -- cash_test
                27, -- payment scammers
                29  -- dd_fm_partner_advertising_accounts
            ],
            users_marks
        ) = False
    GROUP BY
        user_id
),

result AS (SELECT ur.session_id,
                  ur.user_id,
                  ur.event_type,
                  ur.registration_date,
                  d.deposit_count,
                  d.deposit_amount,
                  b.bets
           FROM (
                    SELECT session_id,
                           user_id,
                           event_type,
                           toDate(server_upload_time) AS registration_date
                    FROM holistic.amplitude_1win
                    WHERE server_upload_time >= (SELECT report_date FROM report_date)
                      AND event_type = 'registration_success'
                      AND arrayElement(event_properties.ep_value,
                                       arrayFirstIndex(x -> x = 'social_method', event_properties.ep_key)) = 'steam'
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
                    ) AS ur
                    LEFT JOIN deposit d ON toUInt64(ur.user_id) = toUInt64(d.user_id)
                    LEFT JOIN bet b ON toUInt64(ur.user_id) = toUInt64(b.user_id)
)

SELECT
    ur.registration_date,
    uniq(ur.user_id) as users,
    sum(d.deposit_count) as deposit_count,
    sum(d.deposit_amount) as deposit_amount,
    sum(b.bets) as bets
FROM
    result
GROUP BY
    ur.registration_date

SETTINGS max_execution_time = 36000000000;
