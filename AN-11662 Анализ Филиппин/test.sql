WITH report_date AS (
    SELECT toDate('2024-09-01') AS report_date
)


SELECT
    user_id,
    game_id,

    MIN(datetime) over (partitions by user_id) AS min_bet,
    MAX(datetime) over (partitions by user_id) max_bet,

    SUM(CASE
        WHEN event = 'BET' AND is_refund != 1 THEN 1
        ELSE 0
    END) AS bets,

    SUM(CASE
        WHEN event = 'BET' AND is_refund != 1 THEN amount_converted
        ELSE 0
    END) AS bets_amount,

    MIN(CASE
        WHEN event = 'BET' AND is_refund != 1 THEN amount_converted
        ELSE 0
    END) AS bets_min,

    SUM(CASE
        WHEN event = 'WIN' AND is_refund != 1 THEN 1
        ELSE 0
    END) AS wins,

    SUM(CASE
        WHEN event = 'WIN' AND is_refund != 1 THEN amount_converted
        ELSE 0
    END) AS wins_amount

FROM enriched.casino
WHERE
    datetime >= (SELECT report_date FROM report_date)
    AND country = 'Филиппины'
    AND event IN ('BET', 'WIN')
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
    game_id
ORDER BY
    user_id