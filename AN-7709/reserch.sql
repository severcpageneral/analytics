
WITH reg AS (
    SELECT 
        user_id,
        toDate(time_registration) as event_date,
        c.name as country_n
    FROM holistic.ma_users_1win m
    LEFT JOIN (SELECT code, name FROM holistic.country_names) c ON c.code = m.country 
    WHERE
        time_registration BETWEEN today() - 28
        AND today() - 7
    AND toUInt64(m.user_id) NOT IN 
        (SELECT DISTINCT user_id
            FROM holistic.ma_users_meta_1win
            WHERE fake_account = true
            OR withdrawal_block = true
            OR `1win_tester` = true
            OR sb_users = true
            OR cash_test = true
            OR user_demo_withdrawal = true
            OR dd_fm_partner_advertising_accounts = true)
    AND c.name = 'Индия'
)

SELECT
    reg.user_id AS user_id,
    SUM(CASE WHEN sq.count_dep = 1 THEN 1 ELSE 0 END) AS is_cohort,
    SUM(CASE WHEN sq.count_dep = 2 THEN 1 ELSE 0 END) AS is_first,
    SUM(CASE WHEN sq.count_dep = 3 THEN 1 ELSE 0 END) AS is_second
FROM reg 
LEFT JOIN (
            SELECT *
            FROM (
                    SELECT
                        ROW_NUMBER() over (partition by user_id order by time_confirm) as count_dep,
                        user_id
                    FROM
                        enriched.payments p
                    WHERE
                        p.status = 1
                        AND p.event = 'DEPOSIT'
                        AND p.is_real_operation = True
                )
            WHERE count_dep < 4) AS sq
ON reg.user_id = sq.user_id
GROUP BY
    reg.user_id