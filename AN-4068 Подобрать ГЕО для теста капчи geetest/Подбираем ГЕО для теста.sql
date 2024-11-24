WITH '2024-11-10' AS start_date,
     '2024-11-23' AS end_date

SELECT
    user_properties.up_value[indexOf(user_properties.up_key, 'country')] AS country,
    uniq(amplitude_id),
    uniq(device_id),
    uniq(session_id),
    uniqExactIf(amplitude_id, event_type = 'login_success') / uniqExactIf(amplitude_id, event_type = 'login_form_view') as login2success
FROM holistic.amplitude_1win amp
WHERE toDate(server_upload_time) BETWEEN toDate(start_date) AND toDate(end_date)
--AND user_properties.up_value[indexOf(user_properties.up_key, 'country')]  != 'India'
GROUP BY
    country
SETTINGS
    max_execution_time = 36000000000;
