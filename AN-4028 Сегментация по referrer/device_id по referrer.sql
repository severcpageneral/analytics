SELECT
    device_id,
    domain,
    argMin(referrer, client_event_time_utc) as first_referrer,
    min(client_event_time_utc) AS first_client_time,
    argMax(referrer, client_event_time_utc) as last_referrer,
    max(client_event_time_utc) AS last_client_time,
    groupArrayIf(client_event_time_utc, is_new_referrer = 1) AS event_time_array,
    groupArrayIf(referrer, is_new_referrer = 1) AS referrer_array
FROM
(
    SELECT
        *,
        neighbor(referrer, -1, '') != referrer AS is_new_referrer
    FROM
    (
        SELECT
            device_id,
            client_event_time_utc,
            event_properties.ep_value[indexOf(event_properties.ep_key, 'domain')] AS domain,
            event_properties.ep_value[indexOf(event_properties.ep_key, 'referrer')] AS referrer
        FROM holistic.pageview_1win
        WHERE toDate(client_event_time_local) BETWEEN toDate('2024-11-30') AND toDate('2024-12-05')
            AND event_type = 'page_view'
        ORDER BY device_id, client_event_time_utc
    )
)
GROUP BY device_id, domain
SETTINGS max_execution_time = 36000000000;