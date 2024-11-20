WITH

user_reg AS(
SELECT
    toDate(created_at) AS date,
    uniqExact(id) as user_count
FROM stg.users
WHERE toDate(created_at) >= '2024-05-01'
AND role = 'user' AND status = 1
GROUP BY date)


,dep AS (
SELECT
    date,
    sum(deposit_amount) as amount,
    count(paymentId) as deps,

    SUM(CASE
        WHEN deposit_number = 1 THEN deposit_amount
        ELSE 0
    END) AS first_Deposit_amount,

    SUM(CASE
        WHEN deposit_number = 1 THEN 1
        ELSE 0
    END) AS first_Deposit_count,

    SUM(CASE
        WHEN deposit_number = 2 THEN deposit_amount
        ELSE 0
    END) AS second_Deposit_amount,

    SUM(CASE
        WHEN deposit_number = 2 THEN 1
        ELSE 0
    END) AS second_Deposit_count,

    SUM(CASE
        WHEN deposit_number >= 3 THEN deposit_amount
        ELSE 0
    END) AS thirdplus_Deposit_amount,

    SUM(CASE
        WHEN deposit_number >= 3 THEN 1
        ELSE 0
    END) AS thirdplus_Deposit_count,

    COUNT(DISTINCT CASE
                    WHEN deposit_number >= 3 THEN userId
                    ELSE 0
                END) AS thirdplus_Deposit_uniq



FROM(
    SELECT
        userId,
        toDate(date) AS date,
        amount AS deposit_amount,
        paymentId,
        row_number() over (PARTITION BY userId ORDER BY date) AS deposit_number
    FROM stats.payments_history ph
    WHERE 1=1
    AND toUInt64(userId) in (select distinct
                                toUInt64(id) as user_id
                            from stg.users
                            where role = 'user'
                            and status = 1
                            )
    --AND toDate(date) >= '2024-05-01'
    )
WHERE date >= '2024-05-01'


GROUP BY
    date),

payout AS (
SELECT
    date,
    sum(payout_amount) as amount,
    count(payoutId) as wth,


    SUM(CASE
        WHEN payout_number = 1 THEN payout_amount
        ELSE 0
    END) AS first_payout_amount,

    SUM(CASE
        WHEN payout_number = 1 THEN 1
        ELSE 0
    END) AS first_payout_count,

    SUM(CASE
        WHEN payout_number = 2 THEN payout_amount
        ELSE 0
    END) AS second_payout_amount,

    SUM(CASE
        WHEN payout_number = 2 THEN 1
        ELSE 0
    END) AS second_payout_count,

    SUM(CASE
        WHEN payout_number = 3 THEN payout_amount
        ELSE 0
    END) AS third_payout_amount,

    SUM(CASE
        WHEN payout_number = 3 THEN 1
        ELSE 0
    END) AS third_payout_count,

    SUM(CASE
        WHEN payout_number >= 4 THEN payout_amount
        ELSE 0
    END) AS fourthplus_payout_amount,

    SUM(CASE
        WHEN payout_number >= 4 THEN 1
        ELSE 0
    END) AS fourthplus_payout_count

FROM(
    SELECT
        userId,
        toDate(date) AS date,
        amount AS payout_amount,
        payoutId,
        row_number() over (PARTITION BY userId ORDER BY date) AS payout_number
    FROM stats.payouts_history ph
    WHERE 1=1
    AND toUInt64(userId) in (select distinct
                                toUInt64(id) as user_id
                            from stg.users
                            where role = 'user'
                            and status = 1
                            )
    --AND toDate(date) >= '2024-05-01'
    )

WHERE date >= '2024-05-01'
GROUP BY
    date)

,active as (select dt,
                    uniqExact(userId) as active
                from(
                select toDate(date) as dt,
                    userId
                FROM stats.transactions
                WHERE toDate(date) >= '2024-05-01'
                union distinct
                SELECT toDate(created_at) as dt,
                    user_id
                FROM stg.betting_sessions
                WHERE toDate(created_at) >= '2024-05-01'
                union distinct
                select
                    toDate(date) as dt,
                    userId
                from stats.payments_history ph
                WHERE toDate(date) >= '2024-05-01')
            WHERE toUInt64(userId) in (select distinct toUInt64(id) as user_id from stg.users where role = 'user' and status = 1)
            group by dt)



SELECT
    ur.date AS date,
    ur.user_count AS regs,
    a.active as active_users,
    dep.deps as deps_cnt,
    dep.amount as deps_amount,
    po.wth as wth_cnt,
    po.amount as wth_amount,
    round((dp.first_Deposit_count / ur.user_count)*100, 2) AS reg2dep,

    po.first_payout_amount AS first_payout_amount,
    po.first_payout_count AS first_payout_count,
    po.second_payout_amount AS second_payout_amount,
    po.second_payout_count AS second_payout_count,
    po.third_payout_amount AS third_payout_amount,
    po.third_payout_count AS third_payout_count,
    po.fourthplus_payout_amount AS fourthplus_payout_amount,
    po.fourthplus_payout_count AS fourthplus_payout_count,

    dp.first_Deposit_amount AS first_Deposit_amount,
    dp.first_Deposit_count AS first_Deposit_count,
    dp.second_Deposit_amount AS second_Deposit_amount,
    dp.second_Deposit_count AS second_Deposit_count,
    dp.thirdplus_Deposit_amount AS thirdplus_Deposit_amount,
    dp.thirdplus_Deposit_count AS thirdplus_Deposit_count,
    dp.thirdplus_Deposit_uniq AS thirdplus_Deposit_uniq
FROM user_reg ur
LEFT JOIN payout AS po ON ur.date = po.date
LEFT JOIN dep    AS dp ON ur.date = dp.date
LEFT JOIN active AS a ON ur.date = a.dt

WHERE date < today()