SELECT
    distinct amplitude_id, goal_event_time, country, browser, OS, device_type
FROM holistic.amplitude_1win a
GLOBAL INNER JOIN (
    SELECT
        DISTINCT amplitude_id AS amplitude_id,
        MIN(server_upload_time) AS goal_event_time,
        any(user_properties.up_value[indexOf(user_properties.up_key, 'country')]) as country,
        any(user_properties.up_value[indexOf(user_properties.up_key, 'os_name')]) as browser,
        any(user_properties.up_value[indexOf(user_properties.up_key, 'os')]) as OS,
        any(user_properties.up_value[indexOf(user_properties.up_key, 'device_type')]) as device_type
    FROM holistic.amplitude_1win
    WHERE event_type = 'registration_form_view'
        AND toDate(server_upload_time) BETWEEN '{start_date}'
        AND (user_properties.up_value[indexOf(user_properties.up_key, 'country')]) = 'Nigeria'
        AND (event_properties.ep_value[indexOf(event_properties.ep_key, 'domain')]) = '1win.ng'
    GROUP BY amplitude_id
) r ON a.amplitude_id = r.amplitude_id
WHERE a.server_upload_time <= r.goal_event_time --проверяем именно по событиям до или равно просмотру формы
    AND NOT (user_id > 1) -- мы так проверяем, что пользователь еще ни разу успешно не зарегался, но не чистим тех, кто не имеет user_id, но не первый раз заходит на форму
ORDER BY amplitude_id desc