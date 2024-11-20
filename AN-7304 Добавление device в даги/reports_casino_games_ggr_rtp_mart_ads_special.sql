INSERT INTO tmp.reports_casino_games_ggr_rtp_mart_ads
    SELECT
      toDate(date)                                                  AS date,
      country,
      if(cg4.provider_new = '', 'Провайдер не определен', cg4.provider_new) AS provider,
      main.aggregator                                               AS agregator,
      main.wallet                                                   AS wallet,
      if(cg4.nameEn = '', main.wallet, nameEn)                      AS game,
      main.user_id                                                  AS user_id,
      main.currency                                                 AS currency,
      regulated_domain_name                                         AS domain,     -- Домен
      device                                                        AS device,  -- Доработка в ходе задачи ANALYTICS-7304
      CASE WHEN tg_bot_id = 1 THEN 1 ELSE 0 END AS tg_bot_id, -- Доработка кликера в ходе задачи ANALYTICS-7304
      multiIf(vip_table.vip_status = 'vip', 'vip', 'casual')        AS vip_status, -- Вип Статус
    --Считаем рефанды, что бы вычесть их из ставок и выигрышей:
        coalesce((sumIf(toFloat64(amount_converted), is_refund = 1 AND event = 'BET')), 0) AS refund_sum,
        sumIf(count, is_refund = 1 AND event = 'BET')                                   AS refund_count,
        coalesce((sumIf(toFloat64(amount_converted), event = 'BET')), 0) - refund_sum AS casino_bets_sum,
        sumIf(count, event = 'BET') - refund_count                                      AS casino_bets_count,
        coalesce((sumIf(toFloat64(amount_converted), event = 'WIN')), 0) - refund_sum AS casino_wins_sum,
        sumIf(count, event = 'WIN') - refund_count                                      AS casino_wins_count
    FROM ads.casino_users_daily main
    LEFT JOIN (SELECT distinct provider as merchant_name, gameOwnerName as provider_new, providerId as wallet, nameEn FROM holistic.casino_games_3_1win) AS cg4
    ON cg4.wallet = main.wallet AND cg4.merchant_name = main.aggregator
    LEFT JOIN (SELECT DISTINCT user_id, 'vip' as vip_status FROM holistic.vip_users_relations WHERE vip_users_relations.manager_id > -1) AS vip_table
      ON toUInt64(vip_table.user_id) = toUInt64(main.user_id) -- ВИП СТАТУС

    LEFT JOIN (SELECT user_id, created_at AS created_clicker, tg_bot_id FROM holistic.tg_accounts_binds_logs) AS clicker_table
      ON toUInt64(clicker_table.user_id) = toUInt64(main.user_id) -- Кликер
    WHERE
      product = 'casino'
      AND toDate(date) >= '{start_date}' toDate('2024-09-15')
      AND toDate(date) <= toDate('{end_date}') + interval 1 day
      AND status = 1
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
    GROUP BY
      date,
      country,
      provider,
      aggregator,
      wallet,
      game,
      user_id,
      currency,
      vip_status,
      domain,
      device,
      tg_bot_id;