WITH report_date AS (
    SELECT toDate('2024-10-10') AS report_date
),

dep AS (SELECT user_id,
               '' AS provider_type,
               time_confirm AS action_date,
               CASE
                   WHEN deposit_num = 1 THEN 'first_deposit'
                   WHEN deposit_num = 2 THEN 'second_deposit'
                   WHEN deposit_num = 3 THEN 'third_deposit'
                   WHEN deposit_num = 4 THEN 'fourth_deposit'
                ELSE 'other_deposit'
                END AS type
        FROM (
                 SELECT user_id,
                        time_confirm,
                        COUNT(event) OVER (PARTITION BY user_id ORDER BY time_confirm) AS deposit_num
                 FROM enriched.payments od
                 WHERE toDate(time_confirm) >= (SELECT report_date FROM report_date) - INTERVAL 10 DAY

                   AND status = 1
                   AND event = 'DEPOSIT'
                   AND is_real_operation = TRUE
                   AND number_of_f4_dep != 1
                   AND hasAny([25, 20, 21], od.users_marks) = 0
                 )),

bet AS (
SELECT
    user_id,
    CASE
        WHEN provider IN ('Spribe', 'Spinomenal', '3 Oaks Gaming', 'Hacksaw', '1play') THEN 'Support'
        WHEN provider IN ('Pragmatic', 'BGaming', 'Evolution', 'Smartsoft', 'Endorphina') THEN 'Not Support'
        ELSE 'Other'
    END AS provider_type,
    datetime AS action_date,
    CASE
        WHEN event = 'BET' AND is_refund != 1 THEN 'bet'
        ELSE null
    END AS type

FROM enriched.casino
WHERE
    datetime >= (SELECT report_date FROM report_date)
    --AND provider IN ('Spribe','Spinomenal','3 Oaks Gaming', 'Hacksaw', '1play','Pragmatic', 'BGaming','Evolution', 'Smartsoft', 'Endorphina')
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
    ),

combined AS (
    SELECT
        user_id,
        provider_type,
        action_date,
        type
    FROM (
        SELECT
            b.user_id,
            b.provider_type,
            b.action_date,
            b.type
        FROM bet AS b

        UNION ALL

        SELECT
            d.user_id,
            d.provider_type,
            d.action_date,
            d.type
        FROM dep d
    )
),

base_data AS (
    SELECT
        *,
        dateDiff('minute',
                 lagInFrame(action_date, 1, action_date) OVER (PARTITION BY user_id ORDER BY action_date),
                 action_date) AS time_between_bet_and_deposit,
        maxIf(provider_type, type = 'bet') OVER (PARTITION BY user_id ORDER BY action_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) as provider_bet,

        -- Общее количество ставок для каждого user_id
        count(CASE WHEN type = 'bet' THEN 1 END) OVER (PARTITION BY user_id) AS total_bets,

        -- Общее количество депозитов для каждого user_id
        count(CASE WHEN type LIKE '%deposit%' THEN 1 END) OVER (PARTITION BY user_id) AS total_deposits

    FROM combined
)

SELECT
    provider_bet,
    type,
    uniq(user_id) as users,
    avg(time_between_bet_and_deposit) as avg_time,
    max(total_bets) as bets,
    max(total_deposits) as deposit
FROM base_data
WHERE
    user_id != 130
    AND type != 'bet'
    AND time_between_bet_and_deposit <= 60
GROUP BY
    provider_bet,
    type

SETTINGS
    max_execution_time = 36000000000;