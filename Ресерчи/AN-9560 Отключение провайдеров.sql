SELECT
    c.aggregator,
    c.provider,
  --  c.game,
    COUNT(DISTINCT c.user_id) AS user_count,
    SUM(amount)
FROM
    enriched.casino c
WHERE
    c.status = 1
   -- AND c.event = 'WIN'
    AND toDate(c.datetime) >= '2024-07-01'
    AND country = 'Бразилия'
    AND toUInt64(c.user_id) NOT IN (
        SELECT DISTINCT user_id
        FROM holistic.ma_users_meta_1win
        WHERE
            withdrawal_block = TRUE
            OR fake_account = TRUE
            OR `1win_tester` = TRUE
            OR sb_users = TRUE
            OR cash_test = TRUE
            OR payment_scammers = TRUE
            OR dd_fm_partner_advertising_accounts = TRUE
            OR user_demo_withdrawal = TRUE
            OR cash_agent = TRUE
            OR partner_game_account = TRUE
    )
    AND c.is_refund = FALSE
GROUP BY
    c.aggregator,
    c.provider
   -- c.game
ORDER BY
    user_count DESC
SETTINGS max_execution_time = 36000000000;
