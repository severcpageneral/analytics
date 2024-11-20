WITH user_in_test AS (SELECT session_id,
                             event_properties.ep_value[indexOf(event_properties.ep_key, '[Experiment] Variant')] as group
                      FROM holistic.amplitude_1win
                      WHERE event_type = '[Experiment] Exposure'
                        AND event_properties.ep_value[indexOf(event_properties.ep_key, '[Experiment] Flag Key')] =
                            'ab-test-formy-logina-na-mallazia_new'
    -- AND {black_list}
),

     deposit AS (SELECT toStartOfHour(time_confirm) as date_aggregated,
                        group,
                        uniqExact(source_id)        as deposit_count,
                        sum(amount_converted)       as deposit_sum
                 FROM enriched.payments p
                          INNER JOIN users u ON p.user_id = u.user_id
                 WHERE toDate(time_confirm) BETWEEN start_date AND end_date
                   AND time_create >= add_time
                   AND p.status = 1
                   AND p.is_real_operation = True
                   AND hasAny([25, --sb_users
                                  20, --game_partner_account
                                  21 --game_cash_account
                                  ], p.users_marks) = FALSE
                   AND event = 'DEPOSIT'
                   AND is_real_operation = True
                 GROUP BY 1, 2)
SELECT ut.session_id,
       ut.group,

       mt.event_time,
       mt.form_view,
       mt.login_success
FROM user_in_test ut
         LEFT JOIN metrics mt ON ut.session_id = mt.session_id
    SETTINGS
    max_execution_time = 36000000000;