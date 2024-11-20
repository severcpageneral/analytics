    SELECT
        userId,
        CAST(date AS DATE) AS date,
        RANK() over (PARTITION BY userId ORDER BY date) AS deposit_number,
        COUNT(*) OVER (PARTITION BY userId) AS All_Deposit_count,
        SUM(amount) OVER (PARTITION BY userId) AS All_Deposit_amount
    FROM stats.payments_history ph
    WHERE date > '2024-05-01'