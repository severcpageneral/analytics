SELECT *
   /* toDate(ph.date) AS payment_date,
    ph.tag,
    ph.merchant,


    CASE
        WHEN date_vip >= '2024-06-01' THEN 'Vip'
        ELSE 'No Vip'
    END AS is_vip,

    uniqExact(reg.user_id) AS user_count,
    uniqExact(ph.paymentId) AS payments,
    round(sum(ph.amount), 2) AS amount,
    round(avg(ph.amount), 2) AS avg_amount,
    round(min(ph.amount), 2) AS min_amount,
    round(max(ph.amount), 2) AS max_amount
*/
FROM stats.payments_history ph

LEFT JOIN (
    SELECT
        id AS user_id,
        created_at
    FROM stg.users
    WHERE created_at > '2023-09-01'
) AS reg
ON ph.userId = reg.user_id

LEFT JOIN(
    SELECT
        toUInt64(user_id) AS user_id,
        date_vip
    FROM dm.vip_segments_daily
    WHERE date >= '2024-04-01'
) AS vp
ON ph.userId = vp.user_id

WHERE ph.date >= '2024-06-01'
/*
GROUP BY
    payment_date,
    ph.tag,
    ph.merchant,
    is_vip
*/
SETTINGS max_execution_time = 36000000000, max_memory_usage = 4294967296, max_bytes_before_external_group_by = 4294967296;
