WITH
game_demo AS (
    SELECT
    user_id,
    session_id,
    game_id,
    demo_date,
    minute_in_demo
FROM (
         SELECT DISTINCT user_id,
                         session_id,
                         SUBSTRING_INDEX(
                                 arrayElement(
                                         event_properties.ep_value,
                                         arrayFirstIndex(x -> x = 'page_url', event_properties.ep_key)
                                 ), '?', 1)                                                          AS game_id,
                         toDate(client_event_time_local)                                             AS demo_date,
                         CASE
                             WHEN arrayElement(event_properties.ep_value,
                                               arrayFirstIndex(x -> x = 'page_url', event_properties.ep_key)) LIKE
                                  ('%demo%') THEN 'demo'
                             ELSE ''
                             END                                                                     AS page_type,
                             dateDiff('minute',
                                        client_event_time_local,
                                        lagInFrame(client_event_time_local, 1) OVER (PARTITION BY session_id ORDER BY client_event_time_local DESC)
                            ) AS minute_in_demo
         FROM pageview_1win pw
         WHERE toDate(client_event_time_local) >= '2024-08-20'
           AND user_id NOT IN (SELECT DISTINCT toUInt64(user_id)
                               FROM holistic.ma_users_meta_1win
                               WHERE fake_account = true
                                  OR withdrawal_block = true
                                  OR 1win_tester = true
                OR sb_users = true
                OR cash_test = true
                OR cash_agent = true
                OR user_demo_withdrawal = true
                OR dd_fm_partner_advertising_accounts = true)
             /* AND arrayElement(
            event_properties.ep_value,
            arrayFirstIndex(x -> x = 'page_url', event_properties.ep_key)
        ) LIKE ('%demo%')*/
           AND arrayElement(
                       event_properties.ep_value,
                       arrayFirstIndex(x -> x = 'page_url', event_properties.ep_key)
               ) NOT LIKE ('%localhost%')
           AND session_id != 0
        -- WINDOW window_name as (PARTITION BY session_id ORDER BY client_event_time_local)
         )
WHERE page_type = 'demo'
    AND minute_in_demo >= 0
),

geo_device AS (
    SELECT
        user_id,
        argMax(
            if(
                arrayElement(
                    user_properties.up_value,
                    arrayFirstIndex(x -> x = 'country', user_properties.up_key)
                ) != '',
                arrayElement(
                    event_properties.ep_value,
                    arrayFirstIndex(x -> x = 'country', user_properties.up_value)
                ),
                null
            ),
            client_event_time
        ) AS country,
        argMax(
            if(
                arrayElement(
                    user_properties.up_value,
                    arrayFirstIndex(x -> x = 'city', user_properties.up_key)
                ) != '',
                arrayElement(
                    user_properties.up_value,
                    arrayFirstIndex(x -> x = 'city', user_properties.up_key)
                ),
                null
            ),
            client_event_time
        ) AS city,
        argMax(
            if(
                arrayElement(
                    user_properties.up_value,
                    arrayFirstIndex(x -> x = 'device_name', user_properties.up_key)
                ) != '',
                arrayElement(
                    user_properties.up_value,
                    arrayFirstIndex(x -> x = 'device_name', user_properties.up_value)
                ),
                null
            ),
            client_event_time
        ) AS device_name,
        argMax(
            if(
                arrayElement(
                    user_properties.up_value,
                    arrayFirstIndex(x -> x = 'os_name', user_properties.up_key)
                ) != '',
                arrayElement(
                    user_properties.up_value,
                    arrayFirstIndex(x -> x = 'os_name', user_properties.up_value)
                ),
                null
            ),
            client_event_time
        ) AS os,
        argMax(
            if(
                arrayElement(
                    user_properties.up_value,
                    arrayFirstIndex(x -> x = 'device_type', user_properties.up_key)
                ) != '',
                arrayElement(
                    user_properties.up_value,
                    arrayFirstIndex(x -> x = 'device_type', user_properties.up_value)
                ),
                null
            ),
            client_event_time
        ) AS device_type
    FROM
        amplitude_1win
    WHERE
        toDate(client_event_time) >= '2024-08-20'
        AND user_id NOT IN (
            SELECT DISTINCT toUInt64(user_id)
            FROM holistic.ma_users_meta_1win
            WHERE fake_account = true
                OR withdrawal_block = true
                OR `1win_tester` = true
                OR sb_users = true
                OR cash_test = true
                OR cash_agent = true
                OR user_demo_withdrawal = true
                OR dd_fm_partner_advertising_accounts = true
        )
    GROUP BY
        user_id
),

pay AS (
    SELECT
        user_id,
        1 AS deposit_count,
        toDate(time_confirm) AS date_confirm
    FROM
        enriched.payments
    WHERE
        toDate(time_confirm) >= '2024-08-20'
        AND status = 1
        AND event = 'DEPOSIT'
        AND is_real_operation = TRUE
        AND number_of_f4_dep != 1
        AND hasAny([1, 2, 3, 25, 4, 26, 27, 29], users_marks) = FALSE
        AND toUInt64(user_id) NOT IN (
            SELECT DISTINCT
                toUInt64(user_id)
            FROM
                holistic.ma_users_meta_1win
            WHERE
                withdrawal_block = TRUE
                OR fake_account = TRUE
                OR `1win_tester` = TRUE
                OR sb_users = TRUE
                OR cash_test = TRUE
                OR payment_scammers = TRUE
                OR dd_fm_partner_advertising_accounts = TRUE
                OR user_demo_withdrawal = TRUE
                OR cash_agent = TRUE
                OR partner_game_account = TRUE
        )
)

SELECT
    gd.game_id AS game_id,
    ge.country,
    ge.city,
    ge.device_name,
    ge.os,
    ge.device_type,
    round(avg(minute_in_demo),1) AS minute_in_demo,
    SUM(py.deposit_count) AS deposit_count,
    uniq(gd.user_id) AS users,
    uniq(gd.session_id) AS sessions
FROM
    game_demo gd
LEFT JOIN
    geo_device ge ON gd.user_id = ge.user_id
LEFT JOIN
    pay py ON toUInt64(gd.user_id) = toUInt64(py.user_id)
WHERE
    py.date_confirm >= gd.demo_date
    AND ge.country IS NOT NULL
    AND ge.city IS NOT NULL
    AND ge.device_name IS NOT NULL
    AND ge.os IS NOT NULL
    AND ge.device_type IS NOT NULL
GROUP BY
    gd.game_id,
    ge.country,
    ge.city,
    ge.device_name,
    ge.os,
    ge.device_type

SETTINGS
    max_execution_time = 36000000000;
