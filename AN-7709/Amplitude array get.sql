SELECT DISTINCT
    arrayElement(user_properties.up_value, arrayFirstIndex(x -> x = 'country', user_properties.up_key)) as country
FROM holistic.amplitude_1win m
WHERE client_event_time > '2024-08-10'

