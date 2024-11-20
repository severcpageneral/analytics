        SELECT
            toUInt64(user_id) AS user_id,
            toDate(time_open) AS bets_date,
            'bet_analytics' as bets_group,
            uniq(bet_id) AS bets
        FROM
            bets_analytics
        WHERE
            time_open >= '2024-04-10'
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
                    dd_fm_partner_advertising_accounts)

        GROUP BY
            user_id,
            bets_date
SETTINGS max_execution_time = 360000000;