WITH report_date AS (
    SELECT toDate('2024-08-01') AS start_date,
           toDate('2024-08-31') AS end_date
)
    SELECT DISTINCT
         SUM(CASE WHEN number_of_f4_dep = 1 THEN 1 ELSE NULL END) AS deposit_1
    FROM enriched.payments
    WHERE toDate(time_confirm) BETWEEN (SELECT start_date FROM report_date) AND (SELECT end_date FROM report_date)
        AND status = 1
        AND country = 'Филиппины'
        AND event = 'DEPOSIT'
        AND is_real_operation= TRUE
        AND number_of_f4_dep IN (1, 2, 3, 4)
        AND NOT hasAny(users_marks, [1, 2, 3, 25, 4, 26, 27, 29])
        AND tenant_id = 1