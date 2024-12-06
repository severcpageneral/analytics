/*
Этот SQL-скрипт анализирует историю рефереров пользователей за 3-4 декабря 2024 года. Он отслеживает первое и последнее значение реферера для каждого device_id,
фиксирует все изменения рефереров во времени и связывает эти данные с информацией о партнерах пользователя. Скрипт работает с событиями просмотра страниц (pageview)
и обогащает результаты данными о партнерах из таблицы ma_users_1win. На выходе получается детальная аналитика по каждому устройству с полной историей изменения рефереров
и связанными партнерскими данными.

flowchart TD
    A[pageview_1win table] --> B[raw_pageviews]
    B --> |Extract device_id, user_id,\ndomain, referrer| C[referrer_changes]
    C --> |Calculate changes\nin referrer values| D[device_stats]
    D --> |Aggregate by device:\nfirst/last referrer,\ntime arrays| E[Final Result]
    F[ma_users_1win] --> |Get partner info| E

    subgraph "Data Processing Steps"
        B
        C
        D
    end

    subgraph "Input Sources"
        A
        F
    end

    subgraph "Result"
        E
    end
*/
WITH
raw_pageviews AS (
    SELECT
        device_id,
        user_id,
        connector_receiving_time_utc,
        event_properties.ep_value[indexOf(event_properties.ep_key, 'domain')] AS domain,
        event_properties.ep_value[indexOf(event_properties.ep_key, 'referrer')] AS referrer
    FROM holistic.pageview_1win
    WHERE toDate(connector_receiving_time_utc) BETWEEN toDate('2024-12-03') AND toDate('2024-12-04')
        AND event_type = 'page_view'
       -- AND {black_list}
),

referrer_changes AS (
    SELECT
        *,
        if(neighbor(referrer, -1) != referrer, 1, 0) AS is_new_referrer
    FROM raw_pageviews
    ORDER BY device_id, connector_receiving_time_utc
),

device_stats AS (
    SELECT
        device_id,
        domain,
        argMin(user_id, connector_receiving_time_utc) as user_id,
        argMin(referrer, connector_receiving_time_utc) as first_referrer,
        min(connector_receiving_time_utc) AS first_client_time,
        argMax(referrer, connector_receiving_time_utc) as last_referrer,
        max(connector_receiving_time_utc) AS last_client_time,
        groupArrayIf(connector_receiving_time_utc, is_new_referrer = 1) AS referrer_time_array,
        groupArrayIf(referrer, is_new_referrer = 1) AS referrer_array
    FROM referrer_changes
    GROUP BY
        device_id,
        domain
)

SELECT
    d.device_id,
    d.domain,
    d.user_id,
    p.partner_key,
    p.country,
    d.first_referrer,
    d.first_client_time,
    d.last_referrer,
    d.last_client_time,
    d.referrer_array,
    d.referrer_time_array
FROM device_stats d
LEFT JOIN (
    SELECT user_id,
           name AS country,
           partner_key

    FROM ma_users_1win ma
    LEFT JOIN (
                SELECT DISTINCT code, name
                FROM holistic.country_names
                ) AS ctr ON lower(ctr.code) = ma.country
    WHERE tenant_id = 1
) AS p ON toUInt64(d.user_id) = p.user_id

SETTINGS max_execution_time = 36000000000;