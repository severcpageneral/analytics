WITH report_date AS (
    SELECT toDate('2024-10-10') AS report_date
)
SELECT
    a.user_id,
    (
        SELECT SUM(amount_converted)
        FROM enriched.casino
        WHERE
            --datetime >= report_date
            event = 'BET'
            AND status = 1
            AND is_refund != 1
            AND user_id = a.user_id  -- Используем переименованное поле
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
            ) = false
    ) AS amount_converted,
    a.session_id,
    toDate(a.server_upload_time) AS modal_date
FROM
    holistic.amplitude_1win a
WHERE
    a.server_upload_time >= report_date
    AND a.event_type IN ('casino_game')
    AND a.user_id NOT IN (
        SELECT DISTINCT toUInt64(user_id)
        FROM holistic.ma_users_meta_1win
        WHERE
            (fake_account = true
            OR withdrawal_block = true
            OR 1win_tester = true
            OR sb_users = true
            OR cash_test = true
            OR cash_agent = true
            OR user_demo_withdrawal = true
            OR dd_fm_partner_advertising_accounts = true)
            AND tenant_id = 1
            AND user_id > 0
    )
GROUP BY
    a.user_id,
    a.session_id,
    modal_date
