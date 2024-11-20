WITH
user_reg AS (
    SELECT
        toDate(created_at) AS date,
        uniqExact(id) AS user_count
    FROM stg.users
    WHERE toDate(created_at) >= '2024-05-01'
    AND role = 'user' AND status = 1
    GROUP BY date
),

active AS (
    SELECT
        dt,
        uniqExact(userId) AS active
    FROM (
        SELECT
            toDate(date) AS dt,
            userId
        FROM stats.transactions
        WHERE toDate(date) >= '2024-05-01'

        UNION DISTINCT

        SELECT
            toDate(created_at) AS dt,
            user_id
        FROM stg.betting_sessions
        WHERE toDate(created_at) >= '2024-05-01'

        UNION DISTINCT

        SELECT
            toDate(date) AS dt,
            userId
        FROM stats.payments_history
        WHERE toDate(date) >= '2024-05-01'
    )
    WHERE
        toUInt64(userId) IN (
            SELECT DISTINCT toUInt64(id) AS user_id
            FROM stg.users
            WHERE role = 'user' AND status = 1
        )
    GROUP BY dt
),

dep AS (
    SELECT
        toDate(date) AS dep_date,
        deposit_number,
        merchant,
        SUM(deposit_amount) AS amount_total,
        MIN(deposit_amount) AS amount_min,
        MAX(deposit_amount) AS amount_max,
        COUNT(paymentId) AS deps_count
    FROM (
        SELECT
            userId,
            toDate(date) AS date,
            amount AS deposit_amount,
            paymentId,
            merchant,
            row_number() OVER (PARTITION BY userId ORDER BY date) AS deposit_number
        FROM stats.payments_history
        WHERE toUInt64(userId) IN (
            SELECT DISTINCT toUInt64(id) AS user_id
            FROM stg.users
            WHERE role = 'user' AND status = 1
        )
    )
    WHERE date >= '2024-06-01'
    GROUP BY date, deposit_number, merchant
)

SELECT
    ur.date,
    ur.user_count,
    dp.dep_date,
    dp.deposit_number,
    dp.merchant,
    dp.amount_total,
    dp.amount_min,
    dp.amount_max,
    dp.deps_count,
    a.dt,
    a.active
FROM user_reg ur
LEFT JOIN dep dp ON ur.date = dp.dep_date
LEFT JOIN active a ON ur.date = a.dt
WHERE ur.date < today();

SETTINGS max_execution_time = 360000000;
