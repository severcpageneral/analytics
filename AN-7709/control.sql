SELECT
    amp.user_id AS user_id,
    amp.event_time AS event_time,
    amp.group AS group,
    dp.deposit_count  AS deposit_count,
    dp.deposit_amount AS deposit_amount
    FROM (
             SELECT amp.user_id                                                                         AS user_id,
                    amp.server_upload_time                                                              AS event_time,
                    event_properties.ep_value[indexOf(event_properties.ep_key, '[Experiment] Variant')] AS group
             FROM holistic.amplitude_1win AS amp
             WHERE event_type = '[Experiment] Exposure'
               AND event_properties.ep_value[indexOf(event_properties.ep_key, '[Experiment] Flag Key')] = 'an-7709-freespin-banner-in-cashier-new'
               AND user_id > 0
               --AND {black_list}
               AND toDate(server_upload_time) >= '2024-07-15'
             ) AS amp

LEFT JOIN (SELECT
                user_id,
                time_registration
           FROM holistic.ma_users_1win) ma

ON toInt64(ma.user_id)=toInt64(amp.user_id)

LEFT JOIN (
            SELECT
                user_id,
                time_confirm As deposit_time,
                count(event) OVER (PARTITION BY user_id ORDER BY time_confirm) AS deposit_num,
                ROUND(amount_converted, 2) AS deposit_amount
            FROM enriched.payments od
            WHERE toDate(time_confirm) >= '2024-07-15'
                AND status = 1
                AND is_real_operation = TRUE
                AND hasAny([25, --sb_users
                            20, --game_partner_account
                            21 --game_cash_account
                            ], od.users_marks) = 0
                AND user_id IN (SELECT DISTINCT
                                    user_id
                                FROM holistic.amplitude_1win
                                WHERE
                                    event_properties.ep_value[indexOf(event_properties.ep_key, '[Experiment] Flag Key')] = 'an-7709-freespin-banner-in-cashier-new')

            ) AS dp
ON toInt64(ma.user_id)=toInt64(dp.user_id)

