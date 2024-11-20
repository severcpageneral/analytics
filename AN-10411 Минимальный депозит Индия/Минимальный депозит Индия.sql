SELECT
    user_id,
    toDate(time_confirm) AS date,
    SUM(amount_converted) AS deposit_amount,
    COUNT(*) AS deposit_count
FROM
    enriched.payments od
WHERE
    toDate(time_confirm) >= '2024-08-10'
    AND status = 1
    AND event = 'DEPOSIT'
    AND is_real_operation = TRUE
    AND number_of_f4_dep != 1
    AND hasAny([1, 2, 3, 25, 4, 26, 27, 29], users_marks) = FALSE
    --AND amount < 300
    AND country = 'Индия'
    AND toUInt64(user_id) NOT IN (
        SELECT DISTINCT
            toUInt64(user_id)
        FROM
            holistic.ma_users_meta_1win
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
GROUP BY
    user_id,
    date
SETTINGS
    max_execution_time = 36000000000;
