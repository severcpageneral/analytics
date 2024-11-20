WITH reg AS (
    SELECT
        user_id,
        time_registration,
        c.name as country_n
    FROM holistic.ma_users_1win m
    LEFT JOIN (SELECT code, name FROM holistic.country_names) c ON c.code = m.country
    WHERE time_registration BETWEEN today() - 30 AND today() - 1
    AND country in ('tr', 'in')
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
)

SELECT
    reg.country_n as country_n,
    reg.user_id as user_id,
    reg.time_registration as time_registration,
    CASE
        WHEN  toDate(p.first_dep) == toDate('1970-01-01 03:00:00') THEN -1
        ELSE dateDiff('day', toDate(reg.time_registration), toDate(p.first_dep))
    END AS time2dep,
    CASE
        WHEN toDate(p.first_dep) < toDate(reg.time_registration) THEN 0
        ELSE 1
    END AS reg2dep

FROM reg
LEFT JOIN (SELECT
                user_id,
                min(time_confirm) as first_dep
            FROM
            enriched.payments p
            WHERE p.status = 1
            AND p.event = 'DEPOSIT'
            AND p.is_real_operation = True
            AND toDate(p.time_confirm) BETWEEN today() - 30 AND today() - 1
            GROUP BY user_id
            ) p ON reg.user_id = p.user_id

