WITH report_date AS (
    SELECT toDate('2024-10-10') AS report_date
)
SELECT
    arrayElement(user_properties.up_value, arrayFirstIndex(x -> x = 'country', user_properties.up_key)) AS country,
    arrayElement(user_properties.up_value, arrayFirstIndex(x -> x = 'platform', user_properties.up_key)) AS platform,
    uniq(session_id) AS sessions,
    SUM(event_type = 'casino_filter_field') AS event_search,
    SUM(CASE
        WHEN event_type = 'casino_game' AND arrayElement(event_properties.ep_value, arrayFirstIndex(x -> x = 'category_name', event_properties.ep_key)) = 'search' THEN 1
        ELSE 0
    END) AS casino_game,
    SUM(CASE
            WHEN time_open BETWEEN server_upload_time AND server_upload_time + INTERVAL 10 MINUTE AND  is_bet = 1 THEN 1
        ELSE 0 END) AS is_bet
FROM holistic.amplitude_1win a