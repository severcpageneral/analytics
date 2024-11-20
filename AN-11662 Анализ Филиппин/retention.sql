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
    toDate(datetime) AS bet_date,
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
    bet_date
),

result AS(

SELECT
    ur.user_id,
    ur.name,
    b.game_category,
    toDate(toMonday(ur.time_registration)) AS week_registration,
    ur.time_registration,
    b.bet_date,
    CASE
        WHEN b.bet_date >= ur.time_registration THEN dateDiff('day', ur.time_registration, b.bet_date)
        ELSE 0
    END AS cohort_day,
    b.bets

FROM user_registration ur
LEFT JOIN bet b ON ur.user_id = b.user_id
)

SELECT
    week_registration,
    uniq(user_id) AS total_users,
    sum(bets) AS total_bets,

    uniqIf(user_id, cohort_day <= 1) / total_users AS day_1_users,
    uniqIf(user_id, cohort_day = 2) / total_users AS day_2_users,
    uniqIf(user_id, cohort_day = 3) / total_users AS day_3_users,
    uniqIf(user_id, cohort_day = 4) / total_users AS day_4_users,
    uniqIf(user_id, cohort_day = 5) / total_users AS day_5_users,
    uniqIf(user_id, cohort_day = 7) / total_users AS day_7_users,
    uniqIf(user_id, cohort_day = 14) / total_users AS day_14_users,

    sumIf(bets, cohort_day = 0) / total_bets AS day_0_bets,
    sumIf(bets, cohort_day = 1) / total_bets AS day_1_bets,
    sumIf(bets, cohort_day = 2) / total_bets AS day_2_bets,
    sumIf(bets, cohort_day = 3) / total_bets AS day_3_bets,
    sumIf(bets, cohort_day = 4) / total_bets AS day_4_bets,
    sumIf(bets, cohort_day = 5) / total_bets AS day_5_bets,
    sumIf(bets, cohort_day = 7) / total_bets AS day_7_bets,
    sumIf(bets, cohort_day = 14) / total_bets AS day_14_bets

FROM result
WHERE cohort_day IS NOT NULL
GROUP BY
    week_registration
