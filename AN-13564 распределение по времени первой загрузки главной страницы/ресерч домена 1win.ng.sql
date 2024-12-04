SELECT /*
    device_id AS user_id,
    session_id,
    toDate(client_event_time_utc) AS event_date
        */
    distinct
    event_type


FROM holistic.events_1win AS gr
WHERE arrayElement(event_properties.ep_value, arrayFirstIndex(x -> x = 'domain', event_properties.ep_key)) == '1win.ng'
    AND toDate(client_event_time_utc) > toDate('2024-19-01')
SETTINGS max_execution_time = 36000000000;