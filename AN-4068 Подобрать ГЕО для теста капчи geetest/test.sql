
WITH '2024-11-10' AS start_date,
     '2024-11-24' AS end_date
SELECT
    user_properties.up_value[indexOf(user_properties.up_key, 'country')] AS country,
    round(avg(daily_sessions), 2) as avg_daily_sessions
FROM (
    SELECT
        user_properties.up_value[indexOf(user_properties.up_key, 'country')] AS country,
        toDate(amp.server_upload_time) AS date,
        COUNT(DISTINCT session_id) as daily_sessions
    FROM holistic.amplitude_1win AS amp
    WHERE toDate(amp.server_upload_time) BETWEEN toDate(start_date) AND toDate(end_date)
      AND event_type IN ('login_form_view', 'login_submit')
    --  AND country IN ('{countries_sql}')
      AND {black_list}
    GROUP BY country, date
)
GROUP BY country
ORDER BY country

SETTINGS
    max_execution_time = 36000000000;
