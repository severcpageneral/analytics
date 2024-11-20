/*
WITH user_in_test AS (SELECT amplitude_id,
                             client_event_time,
                             event_properties.ep_value[indexOf(event_properties.ep_key, '[Experiment] Variant')] as group
                      FROM holistic.amplitude_1win
                      WHERE event_type = '[Experiment] Exposure'
                        -- AND toDate(client_event_time) > toDate('2024-10-29')
                        AND event_properties.ep_value[indexOf(event_properties.ep_key, '[Experiment] Flag Key')] =
                            'ab-test-formy-logina-na-mallazia_new'
    --AND {black_list}
),

     metrics AS (SELECT
                        amplitude_id,
                        client_event_time,
                        uniqExactIf(amplitude_id, event_type = 'login_form_view') as form_view,
                        uniqExactIf(amplitude_id, event_type = 'login_success')   as login_success
                 FROM holistic.amplitude_1win
                 WHERE event_type IN ('login_form_view', 'login_success')
                   AND toDate(client_event_time) > toDate('2024-10-29')
                 GROUP BY
                    amplitude_id,
                    client_event_time)
SELECT
    amplitude_id,
    client_event_time,
    group,

    amplitude_id,
    client_event_time,
    form_view,
    login_success
FROM user_in_test ut
LEFT JOIN metrics mt ON ut.amplitude_id = mt.amplitude_id

SETTINGS
    max_execution_time = 36000000000;

 */



