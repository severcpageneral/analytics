SELECT
    user_id,
    MIN(time_confirm) AS first_deposit_date
FROM
    enriched.payments od
WHERE
    toDate(time_confirm) >= '2024-09-16'
    AND status = 1
    AND event = 'DEPOSIT'
    AND is_real_operation = TRUE
    AND number_of_f4_dep != 1
    AND hasAny([1, 2, 3, 25, 4, 26, 27, 29], users_marks) = FALSE
GROUP BY
    user_id