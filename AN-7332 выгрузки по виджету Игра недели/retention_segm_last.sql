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
        arrayElement(
            user_properties.up_value,
            arrayFirstIndex(x -> x = 'device_type', user_properties.up_key)
        ) AS device_type,
        sum(if(event_type = 'casino_game', 1, 0)) AS casino_click,
        sum(if(event_type = 'casino_game_week', 1, 0)) AS gw_click,
        sum(if(event_type = 'casino_game_week_view', 1, 0)) AS gw_view
    FROM holistic.amplitude_1win
    WHERE
        server_upload_time >= '2024-03-01' AND
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
        user_id, event_date, game_id, country, device_type
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
            time_open >= '2024-03-01' AND
            status <> 0
            AND
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

analytics_bets AS (
    SELECT
        toUInt64(user_id) AS user_id,
        toDate(bets_date) AS bets_date,
        uniq(bet_id) AS bets,
        'bets_analytics' AS source_group,
        SUM(IF(status <> 3, toDecimal64(bet_amount - profit, 10), 0)) AS ggr
    FROM (
        SELECT
            DISTINCT user_id,
            bet_id,
            status,
            time_open AS bets_date,
            MAX(bet_amount_converted) OVER (PARTITION BY bet_id) AS bet_amount,
            MAX(profit_converted) OVER (PARTITION BY bet_id) AS profit
        FROM bets_analytics
        WHERE
            time_open >= '2024-03-01' AND
            user_id > 0 AND
            status <> 0 AND
            toUInt64(user_id) NOT IN (
                SELECT DISTINCT toUInt64(user_id)
                FROM holistic.ma_users_meta_1win
                WHERE
                    fake_account OR
                    withdrawal_block OR
                    `1win_tester` OR
                    user_demo_withdrawal OR
                    sb_users OR
                    cash_test OR
                    payment_scammers OR
                    dd_fm_partner_advertising_accounts
            )
    )
    GROUP BY user_id, bets_date
),

all_bets AS (
    SELECT *
    FROM digitain_bets
    UNION ALL
    SELECT *
    FROM analytics_bets
)

SELECT ws.event_date AS event_date,
       ws.device_type AS device_type,
       CASE
           WHEN ws.game_id IN ('pragmatic_vs20olympgate',
                               'pragmatic_vs20olympx',
                               'softswiss_pragmaticexternal:GatesOfOlympus1',
                               'softswiss_pragmaticexternal:GatesofOlympus1000')   THEN 'Gates_Of_Olimpus'
           WHEN ws.game_id IN ('pragmatic_vswaysbufking',
                               'softswiss_pragmaticexternal:BuffaloKingMegaways1') THEN 'Buffalo_King_Megaways'
           WHEN ws.game_id IN ('pragmatic_vswaysbufking',
                               'softswiss_pragmaticexternal:BuffaloKingMegaways1') THEN 'Demi_Gods_5'
           WHEN ws.game_id IN ('softswiss_softswiss:WildTiger')                    THEN 'Wild_tiger'
           WHEN ws.game_id IN ('fundist_2237091')                                  THEN 'Fortune_Tiger'
           WHEN ws.game_id IN ('endorphina_endorphina2_HellHot100@ENDORPHINA')     THEN 'Hell_Hot_100'
           WHEN ws.game_id IN ('spinomenal_SlotMachine_SpinsQueen')                THEN 'Spins_Queen'
           WHEN ws.game_id IN ('')                                                 THEN 'Aztec_Cluster'
           WHEN ws.game_id IN ('softswiss_netgame:RoyalFruits5HoldnLink')          THEN 'Royal_Fruits_5_Hold&Link'
           WHEN ws.game_id IN ('infingames_ag_aliens')                             THEN 'Aliens'
           WHEN ws.game_id IN ('bgaming_MergeUp',
                               'softswiss_softswiss:MergeUp')                      THEN 'Merge_UP'
           WHEN ws.game_id IN ('mrslotty_gamzix-1052',
                                'softswiss_gamzix:UltraLuck')                      THEN 'Ultra_Luck'
           WHEN ws.game_id IN ('mrslotty_amigogaming-frozen-crown')                THEN 'Frozen_Crown'
           WHEN ws.game_id IN ('endorphina_endorphina2_JokerStoker@ENDORPHINA')    THEN 'Joker_Stoker'
           WHEN ws.game_id IN ('pragmatic_vs20olympx',
                               'softswiss_pragmaticexternal:GatesofOlympus1000')   THEN 'Gates_of_Olympus_1000'
           WHEN ws.game_id IN ('fundist_2791322')                                  THEN 'Dreamshock:jackpot_X'
           WHEN ws.game_id IN ('')                                                 THEN 'Hot_papper'
           WHEN ws.game_id IN ('infingames_rp_immortal_ways_diamonds_easter',
                               'infingames_rp_immortal_ways_diamonds_game',
                               'pariplay_RP_HTML5_ImmortalWaysDiamonds88',
                               'pariplay_RP_HTML5_ImmortalWaysDiamonds90',
                               'pariplay_RP_HTML5_ImmortalWaysDiamonds94')         THEN 'Immortal_Ways_Diamonds'
           WHEN ws.game_id IN ('mrslotty_netgame-luck-of-tiger-bonus-combo')       THEN 'Luck_of_Tiger:_Bonus_Combo'
           WHEN ws.game_id IN ('bgaming_SpaceXY',
                               'softswiss_softswiss:KonibetSpaceXY',
                               'softswiss_softswiss:SpaceXY')                      THEN 'Space_XY'
           WHEN ws.game_id IN ('softswiss_clawbuster:LUCKY_TIGER_CLAW')            THEN 'Lucky_Tiger_Claw'
           WHEN ws.game_id IN ('infingames_smartsoft_jetx_pl2',
                               'infingames_smartsoft_jet_x3_pl2',
                               'mrslotty_smartsoft-jetx',
                               'mrslotty_smartsoft-jetx3',
                               'softswiss_smartsoft:JetX',
                               'softswiss_smartsoft:JetX.1Win',
                               'softswiss_smartsoft:JetX3',
                               'softswiss_smartsoft:JetX_1Win')                     THEN 'Jet_X'
           WHEN ws.game_id IN ('betgames_7')                                        THEN 'Wheel_of_fortune'
           WHEN ws.game_id IN ('spinomenal_Tower_1ReelWolfFang-CW')                 THEN 'Reel_wolf_fang '
           WHEN ws.game_id IN ('mrslotty_gamzix-1030',
                               'mrslotty_gamzix-1034',
                               'mrslotty_gamzix-1036',
                               'softswiss_gamzix:Pilot',
                               'softswiss_gamzix:PilotCoin',
                               'softswiss_gamzix:PilotCup')                         THEN 'Pilot'
           ELSE 'None'
       END AS game,

       ws.country AS country,
       uniq(user_id) as users,
       uniqIf(user_id, bets > 0) as players,

       sum(ws.casino_click) AS casino_click,
       sum(ws.gw_click) AS gw_click,
       sum(ws.gw_view) AS gw_view,

       sum(IF(bets > 0, ws.casino_click, 0)) AS casino_click_ifbets,
       sum(IF(bets > 0, ws.gw_click, 0)) AS gw_click_ifbets,
       sum(IF(bets > 0, ws.gw_view, 0)) AS gw_view_ifbets,

       sum(ab.bets) AS sum_bets,
       sum(ab.ggr) AS ggr
FROM widget_stat ws
LEFT JOIN all_bets ab
    ON ws.user_id = ab.user_id AND ws.event_date = ab.bets_date
WHERE ws.country IN('Brazil', 'India')
GROUP BY ws.event_date,
         ws.device_type,
         game,
         ws.country


SETTINGS max_execution_time = 360000000;