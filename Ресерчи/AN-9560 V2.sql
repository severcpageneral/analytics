SELECT
    provider,
    game,
    COUNT(DISTINCT toUInt64(main.user_id)) as user_id,

    round(coalesce(sumIf(toFloat64(amount_converted), event = 'BET'), 0) - coalesce(sumIf(toFloat64(amount_converted), is_refund = 1 AND event = 'BET'), 0), 2) AS casino_bets_sum,
    countIf(1, event = 'BET') - countIf(1, is_refund = 1 AND event = 'BET') AS casino_bets_count,

    round(coalesce(sumIf(toFloat64(amount_converted), event = 'WIN'), 0) - coalesce(sumIf(toFloat64(amount_converted), is_refund = 1 AND event = 'BET'), 0), 2) AS casino_wins_sum,
    countIf(1, event = 'WIN') - countIf(1, is_refund = 1 AND event = 'BET') AS casino_wins_count,



FROM (
    SELECT provider,
           game,
           user_id,
           amount_converted,
           is_refund,
           event,
           datetime,
          -- DATEDIFF('day', MIN(datetime) OVER (PARTITION BY user_id), datetime) AS days_difference
    FROM enriched.casino
    WHERE datetime > '2024-08-01'
      AND event IN ('BET', 'WIN')
      AND country = 'Индия'
      AND status = 1
      AND product IN ('casino', 'case')
      AND hasAny(
            [1,  -- withdrawal_block
             2,  -- 1win_tester
             3,  -- fake_account
             25, -- sb_users
             4,  -- User_demo_withdrawal
             26, -- cash_test
             27, -- payment scammers
             29  -- dd_fm_partner_advertising_accounts
            ], users_marks) = False
) main
LEFT JOIN (
    SELECT DISTINCT user_id, 'vip' as vip_status
    FROM holistic.vip_users_relations
    WHERE vip_users_relations.manager_id > -1
) AS vip_table ON toUInt64(vip_table.user_id) = toUInt64(main.user_id)

GROUP BY
    provider,
    game
SETTINGS max_execution_time = 36000000000;
