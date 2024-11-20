WITH report_date AS (
    SELECT toDate('2024-09-16') AS report_date
),

user_registration AS (
    SELECT
    user_id,
    time_registration
FROM holistic.ma_users_1win t
WHERE
    time_registration >= (SELECT report_date FROM report_date)
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
        SELECT DISTINCT
        toUInt64(user_id) AS user_id,
        time_open AS time_bet,
        COUNT(*) AS is_bet
    FROM holistic.bets_analytics AS dg
    WHERE time_open >= (SELECT report_date FROM report_date)
        AND status <> 0
        AND toUInt64(user_id) NOT IN (
            SELECT DISTINCT toUInt64(user_id)
            FROM holistic.ma_users_meta_1win
            WHERE
                withdrawal_block OR
                fake_account OR
                1win_tester OR
                user_demo_withdrawal OR
                sb_users OR
                cash_test OR
                payment_scammers OR
                dd_fm_partner_advertising_accounts
        )
    GROUP BY
        user_id,
        time_bet
),

deposit AS (
    SELECT
    user_id,
    MIN(time_confirm) AS first_deposit_date
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

sessions AS (
SELECT
    user_id,
    session_id,
    arrayElement(user_properties.up_value, arrayFirstIndex(x -> x = 'country', user_properties.up_key)) AS country,
    arrayElement(user_properties.up_value, arrayFirstIndex(x -> x = 'platform', user_properties.up_key)) AS platform,

    SUM(event_type = 'casino_filter_field') AS event_search,
    SUM(
        CASE
            WHEN event_type = 'casino_game'
                AND arrayElement(event_properties.ep_value, arrayFirstIndex(x -> x = 'category_name', event_properties.ep_key)) = 'search'
            THEN 1
            ELSE 0
        END
    ) AS casino_game,

    MIN(CASE
            WHEN event_type = 'casino_game' THEN server_upload_time
        ELSE null END) AS time_game
FROM holistic.amplitude_1win a

WHERE server_upload_time >= (SELECT report_date FROM report_date)
    AND event_type IN ('casino_filter_field', 'casino_game')
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
GROUP BY
    user_id,
    session_id,
    country,
    platform
),

result AS (
SELECT
    ur.user_id,
    ur.time_registration,
    dp.first_deposit_date,
    bt.time_bet,
    bt.is_bet,
    ss.session_id,
    ss.country,
    ss.platform,
    ss.event_search,
    ss.casino_game,
    ss.time_game
FROM user_registration ur
LEFT JOIN deposit dp ON ur.user_id = dp.user_id
LEFT JOIN bet bt ON ur.user_id = bt.user_id
LEFT JOIN sessions ss ON ur.user_id = toUInt64(ss.user_id)
)

SELECT
    ss.country,
    ss.platform,
    uniq(ur.user_id) AS users,
    uniq(ss.session_id) AS sessions,
    SUM(ss.event_search) AS event_search,
    SUM(ss.casino_game) AS casino_game,

    -- Bet
    SUM(
        CASE
            WHEN bt.time_bet BETWEEN ss.time_game AND ss.time_game + INTERVAL 1 MINUTE THEN bt.is_bet ELSE 0
        END) AS bet_1,
    SUM(
        CASE
            WHEN bt.time_bet BETWEEN ss.time_game AND ss.time_game + INTERVAL 2 MINUTE THEN bt.is_bet ELSE 0
        END) AS bet_2,
    SUM(
        CASE
            WHEN bt.time_bet BETWEEN ss.time_game AND ss.time_game + INTERVAL 3 MINUTE THEN bt.is_bet ELSE 0
        END) AS bet_3,
    SUM(
        CASE
            WHEN bt.time_bet BETWEEN ss.time_game AND ss.time_game + INTERVAL 4 MINUTE THEN bt.is_bet ELSE 0
        END) AS bet_4,
    SUM(
        CASE
            WHEN bt.time_bet BETWEEN ss.time_game AND ss.time_game + INTERVAL 5 MINUTE THEN bt.is_bet ELSE 0
        END) AS bet_5,
    SUM(
        CASE
            WHEN bt.time_bet BETWEEN ss.time_game AND ss.time_game + INTERVAL 6 MINUTE THEN bt.is_bet ELSE 0
        END) AS bet_6,
    SUM(
        CASE
            WHEN bt.time_bet BETWEEN ss.time_game AND ss.time_game + INTERVAL 7 MINUTE THEN bt.is_bet ELSE 0
        END) AS bet_7,
    SUM(
        CASE
            WHEN bt.time_bet BETWEEN ss.time_game AND ss.time_game + INTERVAL 8 MINUTE THEN bt.is_bet ELSE 0
        END) AS bet_8,
    SUM(
        CASE
            WHEN bt.time_bet BETWEEN ss.time_game AND ss.time_game + INTERVAL 9 MINUTE THEN bt.is_bet ELSE 0
        END) AS bet_9,
    SUM(
        CASE
            WHEN bt.time_bet BETWEEN ss.time_game AND ss.time_game + INTERVAL 10 MINUTE THEN bt.is_bet ELSE 0
        END) AS bet_10,

    -- Deposit
    SUM(CASE WHEN dp.first_deposit_date BETWEEN ss.time_game AND ss.time_game + INTERVAL 10 MINUTE THEN 1 ELSE 0 END) AS deposit_count,
    ROUND(AVG(CASE
                   WHEN dp.first_deposit_date BETWEEN ss.time_game AND ss.time_game + INTERVAL 10 MINUTE THEN dateDiff('minute', ss.time_game, dp.first_deposit_date)
                ELSE null END)) AS avg_deposit_minutes

FROM result r
/*
WHERE
    dp.first_deposit_date BETWEEN ss.time_game AND ss.time_game + INTERVAL 10 MINUTE
*/

GROUP BY
    ss.country,
    ss.platform

SETTINGS
    max_execution_time = 36000000000;