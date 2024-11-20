WITH
widget_stat AS (
    SELECT
        toUInt64(user_id) AS user_id,
        toDate(server_upload_time) AS event_date,
        arrayElement(
            event_properties.ep_value,
            arrayFirstIndex(x -> x = 'game_id', event_properties.ep_key)
        ) AS game_id,
        arrayElement(
            user_properties.up_value,
            arrayFirstIndex(x -> x = 'country', user_properties.up_key)
        ) AS country,
        sum(if(event_type = 'casino_game', 1, 0)) AS casino_click,
        sum(if(event_type = 'casino_game_week', 1, 0)) AS gw_click,
        sum(if(event_type = 'casino_game_week_view', 1, 0)) AS gw_view
    FROM holistic.amplitude_1win
    WHERE
        server_upload_time >= '2024-04-20' AND
        user_id > 0 AND

        event_type IN ('casino_game', 'casino_game_week', 'casino_game_week_view') AND
        user_id NOT IN (
            SELECT DISTINCT toUInt64(user_id)
            FROM holistic.ma_users_meta_1win
            WHERE
                fake_account = true OR
                withdrawal_block = true OR
                `1win_tester` = true OR
                sb_users = true OR
                cash_test = true OR
                cash_agent = true OR
                user_demo_withdrawal = true OR
                dd_fm_partner_advertising_accounts = true
        )
    GROUP BY
        user_id, event_date, game_id, country
),

digitain_bets AS (
    SELECT
        toUInt64(user_id) AS user_id,
        toDate(time_open) AS bets_date,
        uniq(order_number) AS bets,
        'digitain' AS source_group,
        ROUND(SUM(toDecimal32(usdt_ggr, 5)), 5) AS ggr
    FROM (
        SELECT DISTINCT
            user_id,
            order_number,
            time_open,
            (bet_amount * cr.course) AS usdt_bet_amount,
            SUM(coefficient) OVER (PARTITION BY bet_id ORDER BY time_open DESC) AS total_coefficient,
            (profit * cr.course) AS usdt_user_profit_amount,
            usdt_bet_amount * coefficient / total_coefficient AS usdt_bet_amount_per_selection,
            usdt_user_profit_amount * coefficient / total_coefficient AS usdt_user_profit_amount_per_selection,
            usdt_bet_amount_per_selection - usdt_user_profit_amount_per_selection AS usdt_ggr
        FROM digitain_bets_analytics_history
        LEFT JOIN currency_rates AS cr ON cr.date = toDate(time_open) AND cr.currency = digitain_bets_analytics_history.currency
        WHERE
            time_open >= '2024-04-20' AND
            status IN (1, 2) AND
            toUInt64(user_id) NOT IN (
                SELECT DISTINCT toUInt64(user_id)
                FROM holistic.ma_users_meta_1win
                WHERE
                    fake_account = true OR
                    withdrawal_block = true OR
                    `1win_tester` = true OR
                    sb_users = true OR
                    cash_test = true OR
                    cash_agent = true OR
                    user_demo_withdrawal = true OR
                    dd_fm_partner_advertising_accounts = true
            )
    )
    GROUP BY user_id, bets_date
),

SETTINGS max_execution_time = 360000000;
