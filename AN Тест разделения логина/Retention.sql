SELECT amp.users_amp,
       amp.group,
       ur.reg_date,
       ar.active_date,
       dateDiff('day', ur.reg_date, ar.active_date) AS day -- Более точный способ подсчета разницы дней
FROM (
         SELECT DISTINCT user_id                                                                             AS users_amp,
                         event_properties.ep_value[indexOf(event_properties.ep_key, '[Experiment] Variant')] as group
         FROM holistic.amplitude_1win
         WHERE event_type = '[Experiment] Exposure'
           --AND {black_list}
           AND event_properties.ep_value[indexOf(event_properties.ep_key, '[Experiment] Flag Key')] =
               'ab-test-formy-logina-na-filippin'
           AND toDate(client_event_time) >= '2024-10-23'
         ) AS amp

         LEFT JOIN (
    SELECT user_id                   as user_ur,
           toDate(time_registration) AS reg_date
    FROM holistic.ma_users_1win
    WHERE toDate(time_registration) >= '2024-10-23'
    ) AS ur ON toUInt64(amp.users_amp) = toUInt64(ur.user_ur)

         LEFT JOIN (
    SELECT DISTINCT user_id                   as user_ar,
                    toDate(client_event_time) AS active_date
    FROM holistic.amplitude_1win
    WHERE toDate(client_event_time) >= '2024-10-23'
    AND client_event_time <= now() -- ограничиваем временной диапазон
    ) AS ar ON toUInt64(amp.users_amp) = toUInt64(ar.user_ar)
    SETTINGS max_execution_time = 3600;