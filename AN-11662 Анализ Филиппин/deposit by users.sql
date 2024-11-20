WITH report_date AS (
    SELECT toDate('2024-01-01') AS report_date
),
user_registration AS (
SELECT
    user_id,
    name,
    toDate(time_registration) AS time_registration
FROM holistic.ma_users_1win t
left join (select id_key, name
          from holistic.partner_keys_1win_partner) pk on toUInt64(pk.id_key) = toUInt64(t.hash_id)
WHERE
    time_registration >= (SELECT report_date FROM report_date)
    AND country = 'ph'
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
),

bet AS (
SELECT
    user_id,
    game_category,
    game_id,
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
    user_id,
    game_category,
    game_id
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


results AS(
SELECT
    ur.user_id AS user_id,
    ur.name AS name,
    toDate(toMonday(ur.time_registration)) AS week_registration,
    b.bets AS bets,
    d.deposit_count AS deposit_count,
    ROUND(d.deposit_amount, 2) AS deposit_amount

FROM user_registration ur
LEFT JOIN deposit d ON ur.user_id = d.user_id
LEFT JOIN bet b ON ur.user_id = b.user_id
)


SELECT
    *
FROM results

SETTINGS
    max_execution_time = 36000000000;

