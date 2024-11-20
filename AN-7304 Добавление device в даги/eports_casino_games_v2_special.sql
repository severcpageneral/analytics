WITH
toDate('{start_date}') as start_date,
toDate('{end_date}') as end_date,

game_cat as (
    SELECT
        game,
        wallet,
        provider,
        aggregator,
        groupArray(main_game_category) AS main_game_categories
    FROM (
        SELECT DISTINCT
            game,
            wallet,
            provider,
            aggregator,
            CASE
                WHEN arrayExists(x -> x IN ('Quick games', 'Mines', 'Lottery', 'Scratch Cards', 'Fishing games'), game_category) THEN 'Quick Games'
                WHEN arrayExists(x -> x IN ('Slots', 'Drops & Wins', 'Fortune'), game_category) THEN 'Slots'
                WHEN arrayExists(x -> x IN ('Live Casino', 'BlackJacks', 'Roulette', 'Table games', 'Game shows', 'Video Poker', 'Baccarat'), game_category) THEN 'Live Casino'
                WHEN arrayExists(x -> x = 'Virtual Sports', game_category) THEN 'Virtual Sports'
                ELSE NULL
            END AS main_game_category
        FROM enriched.casino
        WHERE datetime BETWEEN start_date AND end_date
    )
    GROUP BY
        game,
        wallet,
        provider,
        aggregator
)
SELECT
    subquery.provider AS provider,
    subquery.agregator AS agregator,
    subquery.wallet AS wallet,
    game,
    main_game_categories,
    vip_status,
    event as type,
    date,
    user_id AS user_id_select,
    amount_converted AS max_amount_converted_user,
    avg(amount_converted) OVER (PARTITION BY game, user_id, date, type) AS avg_amount_converted_user,
    country                                                       AS country,
    regulated_domain_name                                         AS domain,
    device                                                        AS device, -- Added device column
    CASE WHEN tg_bot_id = 1 THEN 1 ELSE 0 END AS tg_bot_id, -- Доработка кликера в ходе задачи ANALYTICS-7304
    ''                                                            AS amount_converted_bin,
    0                                                          AS count_people,
    0                                                          AS refund_sum,
    0                                                          AS refund_count,
    0                                                          AS casino_bets_sum,
    0                                                          AS casino_bets_count,
    0                                                          AS casino_wins_sum,
    0                                                          AS casino_wins_count,
    0                                                          AS max_bet,
    0                                                          AS max_win,
    ROW_NUMBER() OVER (ORDER BY max_amount_converted_user DESC) + 99 AS id_b -- (SELECT MAX(id_b) FROM reports.casino_games_v2)
FROM (
    SELECT
        toDate(datetime) as date,
        main.aggregator  AS agregator,
        country,
        regulated_domain_name,
        main.wallet AS wallet,
        IF(cg4.nameEn = '', main.wallet, nameEn) AS game,
        IF(cg4.provider_new = '', 'Провайдер не определен', cg4.provider_new) AS provider,
        multiIf(vip_table.vip_status = 'vip', 'vip', 'casual')  AS vip_status,
        toInt32(main.user_id) AS user_id,
        clicker_table.tg_bot_id AS tg_bot_id,
        event,
        amount_converted,
        device,
        ROW_NUMBER() OVER (PARTITION BY game, date, event, agregator, vip_status, regulated_domain_name, country  ORDER BY amount_converted DESC) AS row_num
    FROM
        enriched.casino main
    LEFT JOIN (SELECT distinct provider as merchant_name, gameOwnerName as provider_new, providerId as wallet, nameEn FROM holistic.casino_games_3_1win) AS cg4
    ON cg4.wallet = main.wallet AND cg4.merchant_name = main.aggregator
    LEFT JOIN (SELECT DISTINCT user_id, 'vip' as vip_status FROM holistic.vip_users_relations WHERE vip_users_relations.manager_id > -1) as vip_table
    ON toUInt64(vip_table.user_id) = toUInt64(main.user_id)
    LEFT JOIN (SELECT DISTINCT user_id, tg_bot_id FROM holistic.tg_accounts_binds_logs) AS clicker_table
      ON toUInt64(clicker_table.user_id) = toUInt64(main.user_id) -- Кликер

    WHERE
        date >= start_date
        AND date <= end_date
        AND product = 'casino'
        AND event IN ('BET', 'WIN')
        AND is_refund = false
        AND hasAny([1,2,3,25,4,26,27,29], users_marks) = false
        AND amount_converted >= 50
) subquery
LEFT JOIN game_cat ON game_cat.wallet = subquery.wallet AND game_cat.aggregator = subquery.agregator AND game_cat.provider = subquery.provider
WHERE
    row_num <= 25

UNION ALL

SELECT
    IF(cg4.provider_new = '', 'Провайдер не определен', cg4.provider_new) AS provider,
    main.aggregator                                               AS agregator,
    main.wallet                                                   AS wallet,
    IF(cg4.nameEn = '', main.wallet, nameEn)                      AS game,
    main_game_categories,
    multiIf(vip_table.vip_status = 'vip', 'vip', 'casual')        AS vip_status,
    'game_metrics'                                                AS type,
    toDate(date)                                                  AS date,
    0                                                          AS user_id_select,
    0                                                          AS max_amount_converted_user,
    0                                                          AS avg_amount_converted_user,
    main.country                                                  AS country,
    main.regulated_domain_name                                    AS domain,
    main.device                                                   AS device, -- Added device column
    CASE WHEN tg_bot_id = 1 THEN 1 ELSE 0 END                     AS tg_bot_id, -- Доработка кликера в ходе задачи ANALYTICS-7304
    ''                                                          AS amount_converted_bin,
    count(DISTINCT main.user_id)                                  AS count_people,
    coalesce((sumIf(toFloat64(amount_converted), is_refund = 1 AND event = 'BET')), 0) AS refund_sum,
    sumIf(count, is_refund = 1 AND event = 'BET')                                        AS refund_count,
    coalesce((sumIf(toFloat64(amount_converted), event = 'BET')), 0) - refund_sum      AS casino_bets_sum,
    sumIf(count, event = 'BET') - refund_count                                           AS casino_bets_count,
    coalesce((sumIf(toFloat64(amount_converted), event = 'WIN')), 0) - refund_sum      AS casino_wins_sum,
    sumIf(count, event = 'WIN') - refund_count                                           AS casino_wins_count,
    max_bet,
    max_win,
    0 as id_b
FROM (SELECT * FROM ads.casino_users_daily
        WHERE
         date >= start_date
         AND date <= end_date
        AND event IN ('BET','WIN')
        AND status = 1
        AND product = 'casino'
        AND hasAny(
            [1,  --withdrawal_block
            2,  --1win_tester
            3,  --fake_account
            25, --sb_users
            4, -- User_demo_withdrawal
            26, --cash_test
            27, --payment scammers
            29 --dd_fm_partner_advertising_accounts
            ],users_marks) = False
      ) main
LEFT JOIN (SELECT distinct provider as merchant_name, gameOwnerName as provider_new, providerId as wallet, nameEn FROM holistic.casino_games_3_1win) AS cg4
ON cg4.wallet = main.wallet AND cg4.merchant_name = main.aggregator

LEFT JOIN (SELECT DISTINCT user_id, 'vip' as vip_status FROM holistic.vip_users_relations WHERE vip_users_relations.manager_id > -1) as vip_table
        ON toUInt64(vip_table.user_id) = toUInt64(main.user_id)


LEFT JOIN (SELECT DISTINCT user_id, tg_bot_id FROM holistic.tg_accounts_binds_logs) AS clicker_table
      ON toUInt64(clicker_table.user_id) = toUInt64(main.user_id) -- Кликер

LEFT JOIN game_cat ON game_cat.wallet = main.wallet AND game_cat.aggregator = main.aggregator AND game_cat.provider = cg4.provider_new
LEFT JOIN (
            SELECT  wallet, aggregator, country, date,
                    regulated_domain_name, vip_status,
                    maxIf(amount_converted,event = 'BET') AS max_bet,
                    maxIf(amount_converted,event = 'WIN') AS max_win
            FROM
                (SELECT amount_converted ,event, aggregator, wallet, regulated_domain_name, country, toDate(datetime) as date,
                        multiIf(vip_table.vip_status = 'vip', 'vip', 'casual')  AS vip_status
                FROM enriched.casino main
                LEFT JOIN (SELECT DISTINCT user_id, 'vip' AS vip_status FROM holistic.vip_users_relations WHERE vip_users_relations.manager_id > -1) as vip_table
                    ON toUInt64(vip_table.user_id) = toUInt64(main.user_id)
                WHERE
                 date >= start_date
                 AND date <= end_date
                    AND event IN ('BET','WIN') AND status = 1 AND product = 'casino'
                    AND hasAny(
                    [1,  --withdrawal_block
                    2,   --1win_tester
                    3,   --fake_account
                    25,  --sb_users
                    4,   -- User_demo_withdrawal
                    26,  --cash_test
                    27,  --payment scammers
                    29  --dd_fm_partner_advertising_accounts
                    ],users_marks) = False
                )
            GROUP BY  wallet, aggregator, country, regulated_domain_name, vip_status, date
            )   mxb
        ON  mxb.wallet = main.wallet AND mxb.aggregator = main.aggregator AND mxb.country = main.country
            AND mxb.regulated_domain_name = main.regulated_domain_name AND mxb.vip_status = multiIf(vip_table.vip_status = 'vip', 'vip', 'casual')
            AND mxb.date = main.date
GROUP BY  game, agregator, provider, wallet, country, date, domain, device, tg_bot_id, vip_status, max_bet, max_win, main_game_categories;