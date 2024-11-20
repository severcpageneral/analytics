SELECT
    provider AS provider,
    uniq(user_id) AS users,

    countIf(1, event = 'BET') - countIf(1, is_refund = 1 AND event = 'BET') AS bets_count,
    round(coalesce(sumIf(toFloat64(amount_converted), event = 'BET'), 0) - coalesce(sumIf(toFloat64(amount_converted), is_refund = 1 AND event = 'BET'), 0), 2) AS bets_sum,

    round(bets_count / users, 2)   AS bets_count_users,
    round(bets_sum   / users, 2)   AS bets_sum_users,

    countIf(1, event = 'WIN') - countIf(1, is_refund = 1 AND event = 'BET') AS wins_count,
    round(coalesce(sumIf(toFloat64(amount_converted), event = 'WIN'), 0) - coalesce(sumIf(toFloat64(amount_converted), is_refund = 1 AND event = 'BET'), 0), 2) AS wins_sum,

    round(wins_count / users, 2)    AS wins_count_users,
    round(wins_sum   / users, 2)   AS wins_sum_users,

    bets_sum - wins_sum AS GGR,
    round(GGR / users, 2) AS GGR_users
FROM
    enriched.casino

WHERE
    datetime BETWEEN '2024-08-01' AND CURRENT_DATE()
    AND country = 'Индия'
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
GROUP BY
    provider

SETTINGS
    max_execution_time = 36000000000;