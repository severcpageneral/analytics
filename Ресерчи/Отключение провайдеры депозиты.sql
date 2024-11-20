WITH users_providers AS (
    SELECT DISTINCT
        user_id
    FROM
        enriched.casino
    WHERE
        datetime BETWEEN '2024-08-01' AND '2024-08-23'
        AND provider IN ('Play''n GO', 'AGT', 'Clawbuster', 'Turbo Games', 'Onlyplay', 'Gamebeat')
        AND event = 'BET'
        AND country = 'Индия'
        AND status = 1
        AND hasAny(
            [1,  -- withdrawal_block
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
)
SELECT
    user_id,
    date,
    SUM(CASE
        WHEN rang = 1 THEN 1
        ELSE 0
    END) AS is_first_dep_count,

    SUM(CASE
        WHEN rang = 1 THEN amount_converted
        ELSE 0
    END) AS is_first_dep_amount,

    SUM(CASE
        WHEN rang = 2 THEN 1
        ELSE 0
    END) AS is_second_dep_count,

    SUM(CASE
        WHEN rang = 2 THEN amount_converted
        ELSE 0
    END) AS is_second_dep_amount

FROM (
         SELECT toUInt64(user_id)    AS user_id, -- Приведение к UInt64
                toDate(time_confirm) AS date,
                ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY time_confirm) AS rang,
                amount_converted
         FROM enriched.payments od
         WHERE user_id GLOBAL IN (SELECT user_id FROM users_providers)
           AND toDate(time_confirm) BETWEEN '2024-08-01' AND '2024-08-23'
           AND status = 1
           AND event = 'DEPOSIT'
           AND is_real_operation = TRUE
           AND number_of_f4_dep != 1
           AND hasAny([1, 2, 3, 25, 4, 26, 27, 29], users_marks) = FALSE
           AND country = 'Индия'
           AND toUInt64(user_id) NOT IN (SELECT DISTINCT toUInt64(user_id)
                                         FROM holistic.ma_users_meta_1win
                                         WHERE withdrawal_block = TRUE
                                            OR fake_account = TRUE
                                            OR `1win_tester` = TRUE
                                            OR sb_users = TRUE
                                            OR cash_test = TRUE
                                            OR payment_scammers = TRUE
                                            OR dd_fm_partner_advertising_accounts = TRUE
                                            OR user_demo_withdrawal = TRUE
                                            OR cash_agent = TRUE
                                            OR partner_game_account = TRUE)
         )

GROUP BY
    user_id,
    date



SETTINGS
    max_execution_time = 36000000000;