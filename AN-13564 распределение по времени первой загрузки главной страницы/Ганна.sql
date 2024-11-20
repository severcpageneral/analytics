SELECT 
    device_id AS user_id,
    arrayElement(event_properties.ep_value, arrayFirstIndex(x -> x = 'domain', event_properties.ep_key)) AS domain,
    ctr.name as geo,
    toDate(client_event_time_utc) AS event_date,
    session_id,
    CAST(arrayElement(event_properties.ep_value, arrayFirstIndex(x -> x = 'is_mobile', event_properties.ep_key)) = 'true' AS UInt8) AS is_mobile,
    if(arrayFirstIndex(x -> x = 'banners_load_time', event_properties.ep_key) > 0,
        toInt32(arrayElement(event_properties.ep_value, arrayFirstIndex(x -> x = 'banners_load_time', event_properties.ep_key))) / 1000,
        NULL) AS banners_load_time,
    if(arrayFirstIndex(x -> x = 'config_ready_time', event_properties.ep_key) > 0,
        toInt32(arrayElement(event_properties.ep_value, arrayFirstIndex(x -> x = 'config_ready_time', event_properties.ep_key))) / 1000,
        NULL) AS config_ready_time,
    if(arrayFirstIndex(x -> x = 'lang_load_time', event_properties.ep_key) > 0,
        toInt32(arrayElement(event_properties.ep_value, arrayFirstIndex(x -> x = 'lang_load_time', event_properties.ep_key))) / 1000,
        NULL) AS lang_load_time,
    if(arrayFirstIndex(x -> x = 'page_load_time', event_properties.ep_key) > 0,
        toInt32(arrayElement(event_properties.ep_value, arrayFirstIndex(x -> x = 'page_load_time', event_properties.ep_key))) / 1000,
        NULL) AS page_load_time,
    if(arrayFirstIndex(x -> x = 'token_ready_time', event_properties.ep_key) > 0,
        toInt32(arrayElement(event_properties.ep_value, arrayFirstIndex(x -> x = 'token_ready_time', event_properties.ep_key))) / 1000,
        NULL) AS token_ready_time
FROM holistic.events_1win AS gr
LEFT JOIN (
    SELECT DISTINCT code, name 
    FROM holistic.country_names
) AS ctr ON lower(ctr.code) = lower(arrayElement(gr.event_properties.ep_value, arrayFirstIndex(x -> x = 'geo', gr.event_properties.ep_key)))
WHERE event_type IN ('time_first_load', 'user_time_metrics')
    AND toDate(client_event_time_utc) > toDate('2024-10-01')
    AND arrayElement(event_properties.ep_value, arrayFirstIndex(x -> x = 'page_path', event_properties.ep_key)) = '/'
ORDER BY event_date ASC