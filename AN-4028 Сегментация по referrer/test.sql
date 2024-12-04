SELECT DISTINCT event_type
         FROM holistic.events_1win
         WHERE toDate(client_event_time_utc) BETWEEN toDate('2024-11-30') AND toDate('2024-12-05')
           AND event_type LIKE '%view%'
SETTINGS
    max_execution_time = 36000000000;