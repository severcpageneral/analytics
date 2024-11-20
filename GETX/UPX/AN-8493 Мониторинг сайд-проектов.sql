WITH user_reg AS(
SELECT
   id AS user_id,
    CASE
        WHEN created_at >= '2024-05-21' THEN 'NEW'
        ELSE 'OLD'
    END AS user_type,
   CAST(created_at AS date) AS user_registration
FROM stg.users
WHERE created_at > '2024-01-01'
),

dep AS (
SELECT
    userId,

    CAST(MIN(first_deposit_date) AS DATE) AS first_deposit_date,

    SUM(CASE
        WHEN deposit_number = 1 THEN deposit_amount
        ELSE 0
    END) AS first_Deposit,

    SUM(CASE
        WHEN deposit_number = 2 THEN deposit_amount
        ELSE 0
    END) AS second_Deposit,

    SUM(CASE
        WHEN deposit_number = 3 THEN deposit_amount
        ELSE 0
    END) AS third_Deposit,

    SUM(CASE
        WHEN deposit_number >= 4 THEN deposit_amount
        ELSE 0
    END) AS fourth_Deposit,

    MAX(All_Deposit_count) AS All_Deposit_count,
    MAX(All_Deposit_amount) AS All_Deposit_amount
FROM(
    SELECT
        userId,
        currency AS deposit_currency,
        amount AS deposit_amount,
        MIN(date) over (PARTITION BY userId ORDER BY date) AS first_deposit_date,
        RANK() over (PARTITION BY userId ORDER BY date) AS deposit_number,
        COUNT(*) OVER (PARTITION BY userId) AS All_Deposit_count,
        SUM(amount) OVER (PARTITION BY userId) AS All_Deposit_amount
    FROM stats.payments_history ph
    --WHERE date > '2024-05-01'
    )
GROUP BY
    userId
),

payout AS (
SELECT
    userId,
    SUM(CASE
        WHEN payout_number = 1 THEN payout_amount
        ELSE 0
    END) AS first_Payout,

    SUM(CASE
        WHEN payout_number = 2 THEN payout_amount
        ELSE 0
    END) AS second_first_Payout,

    SUM(CASE
        WHEN payout_number = 3 THEN payout_amount
        ELSE 0
    END) AS third_first_Payout,

    SUM(CASE
        WHEN payout_number >= 4 THEN payout_amount
        ELSE 0
    END) AS fourth_first_Payout,

    SUM(All_Payout_count) AS All_Payout_count,
    SUM(All_Payout_amount) AS All_Payout_amount
FROM(
    SELECT
        userId,
        amount AS payout_amount,
        RANK() OVER (PARTITION BY userId ORDER BY date) AS payout_number,
        COUNT(*) OVER (PARTITION BY userId) AS All_Payout_count,
        SUM(amount) OVER (PARTITION BY userId) AS All_Payout_amount
    FROM stats.payouts_history ph
    WHERE date > '2024-05-01'
) AS subquery
GROUP BY userId)


SELECT *
FROM user_reg AS ur

LEFT JOIN dep dp ON ur.user_id = dp.userId
LEFT JOIN payout po ON ur.user_id = po.userId