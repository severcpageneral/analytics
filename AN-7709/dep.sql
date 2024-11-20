SELECT
                user_id,
                count(event) AS deposit_count,
                ROUND(SUM(amount_converted), 2) AS deposit_amount
FROM enriched.payments od
WHERE toDate(time_confirm) >= '2024-07-15'
  AND status = 1
  AND is_real_operation = TRUE
  AND hasAny([25, --sb_users
             20, --game_partner_account
             21 --game_cash_account
             ], od.users_marks) = 0
GROUP BY
    user_id