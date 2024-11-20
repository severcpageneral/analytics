WITH test AS (
    SELECT
        amp.user_id AS user_id,
        vip_table.vip_status,
        MIN(amp.server_upload_time) AS event_time,
        event_properties.ep_value[indexOf(event_properties.ep_key, '[Experiment] Variant')] AS group
    FROM holistic.amplitude_1win AS amp
    LEFT JOIN (SELECT DISTINCT toUInt64(user_id) as user_id, 'vip' as vip_status
                            FROM holistic.vip_users_relations
                            WHERE vip_users_relations.manager_id > -1) as vip_table ON toUInt64(vip_table.user_id) = toUInt64(amp.user_id)
    WHERE event_type = '[Experiment] Exposure'
        AND event_properties.ep_value[indexOf(event_properties.ep_key, '[Experiment] Flag Key')] = 'an-7709-freespin-in-cashier-new2'
        AND user_id > 0
        --AND {black_list}
        --AND toDate(server_upload_time) >= '2024-07-25'


GROUP BY
    user_id,
    group,
    vip_status
),
dep AS (
    SELECT
    user_id,
    ROUND(SUM(deposit_num = 1), 0) AS is_first_deposit,
    ROUND(SUM(IF(deposit_num = 1, deposit_amount, 0)), 2) AS is_first_amount,
    ROUND(SUM(deposit_num = 2), 0) AS is_second_deposit,
    ROUND(SUM(IF(deposit_num = 2, deposit_amount, 0)), 2) AS is_second_amount,
    MAX(deposit_num) AS total_deposit_num,
    ROUND(SUM(deposit_amount), 2) AS total_deposit_amount
FROM (
    SELECT
        user_id,
        COUNT(event) OVER (PARTITION BY user_id ORDER BY time_confirm) AS deposit_num,
        ROUND(amount_converted, 2) AS deposit_amount
    FROM enriched.payments od
    WHERE toDate(time_confirm) >= '2024-07-25'
        AND status = 1
        AND event = 'DEPOSIT'
        AND is_real_operation = TRUE
        AND number_of_f4_dep != 1
        AND amount >= 300
        AND hasAny([25, 20, 21], od.users_marks) = 0
) AS aggregated_data
GROUP BY user_id
)

SELECT
    tst.user_id AS user_id,
    tst.vip_status AS vip_status,
    tst.event_time AS event_time,
    tst.group AS group,
    CASE
        WHEN toDate(ma.time_registration) < toDate(tst.event_time) THEN 'Old User'
        WHEN toDate(ma.time_registration) >= toDate(tst.event_time) THEN 'New User'
    END AS user_type,
    dp.is_first_deposit AS is_first_deposit,
    dp.is_first_amount AS is_first_amount,
    dp.is_second_deposit AS is_second_deposit,
    dp.is_second_amount AS is_second_amount,
    dp.total_deposit_num AS total_deposit_num,
    dp.total_deposit_amount AS total_deposit_amount
FROM test tst
LEFT JOIN (
    SELECT DISTINCT
        user_id,
        time_registration
    FROM holistic.ma_users_1win
) ma ON toInt64(tst.user_id) = toInt64(ma.user_id)
LEFT JOIN dep dp ON toInt64(tst.user_id) = toInt64(dp.user_id)
SETTINGS max_execution_time = 36000000000;
