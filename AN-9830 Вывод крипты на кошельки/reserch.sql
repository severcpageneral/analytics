SELECT
        user_id,
        wallet,
        time_confirm,
        country,
        actual_amount,
        currency,
        payment_system
    FROM
        enriched.payments
    WHERE
        toDate(time_create) >= '2024-07-25'
        AND is_real_operation = TRUE
        --AND number_of_f4_dep != 0
        AND status = 1
        AND hasAny([1, 2, 3, 25, 4, 26, 27, 29], users_marks) = FALSE

        AND toUInt64(user_id) GLOBAL NOT IN (
            SELECT DISTINCT toUInt64(user_id)
            FROM holistic.ma_users_meta_1win
            WHERE
                fake_account = TRUE
                OR withdrawal_block = TRUE
                OR `1win_tester` = TRUE
                OR sb_users = TRUE
                OR cash_test = TRUE
                OR cash_agent = TRUE
                OR user_demo_withdrawal = TRUE
                OR dd_fm_partner_advertising_accounts = TRUE
        )


        AND merchant_name IN ('metacash', 'b2binpay-v2')
        AND event = 'WITHDRAWAL'
        AND actual_amount_converted >= 200


