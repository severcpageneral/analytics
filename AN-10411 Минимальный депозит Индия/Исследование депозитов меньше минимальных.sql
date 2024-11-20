WITH deps AS (
SELECT
    toUInt64(user_id) AS user_id,  -- Приведение к UInt64
    toDate(time_confirm) AS date,
    SUM(amount_converted) AS back_deposit_amount,
    COUNT(*) AS back_deposit_count
FROM
    enriched.payments od
WHERE
    toDate(time_confirm) >= '2024-08-10'
    AND status = 1
    AND event = 'DEPOSIT'
    AND is_real_operation = TRUE
    AND number_of_f4_dep != 1
    AND hasAny([1, 2, 3, 25, 4, 26, 27, 29], users_marks) = FALSE
    --AND amount < 300
    AND country = 'Индия'
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
GROUP BY
    user_id,
    date
),

front AS (
    SELECT
        toUInt64(user_id) AS user_id,  -- Приведение к UInt64
        toDate(client_event_time) as date,

        SUM(CASE
                WHEN event_type = 'deposit_to_confirm' THEN toFloat64(arrayElement(event_properties.ep_value,
                                                                                   arrayFirstIndex(
                                                                                           x -> x = 'submit_amount',
                                                                                           event_properties.ep_key)))
                ELSE 0
            END) AS front_deposit_to_confirm_amount,

        SUM(CASE
                WHEN toFloat64(arrayElement(event_properties.ep_value,
                                            arrayFirstIndex(x -> x = 'submit_amount',
                                                            event_properties.ep_key))) > 0 THEN 1
                ELSE 0
            END) AS front_deposit_to_confirm_count,

        SUM(CASE
                WHEN event_type = 'deposit_submit' THEN toFloat64(arrayElement(event_properties.ep_value,
                                                                               arrayFirstIndex(
                                                                                       x -> x = 'submit_amount',
                                                                                       event_properties.ep_key)))
                ELSE 0
            END) AS front_deposit_submit_amount,

        SUM(CASE
                WHEN toFloat64(arrayElement(event_properties.ep_value,
                                            arrayFirstIndex(x -> x = 'submit_amount',
                                                            event_properties.ep_key))) > 0 THEN 1
                ELSE 0
            END) AS front_deposit_submit_count

    FROM holistic.amplitude_1win m
    WHERE event_type IN ('deposit_to_confirm', 'deposit_submit')
        AND client_event_time > '2024-08-10'
        --AND arrayElement(user_properties.up_value, arrayFirstIndex(x -> x = 'country', user_properties.up_key)) = 'India'

        AND toUInt64(user_id) NOT IN (SELECT DISTINCT toUInt64(user_id)
                                      FROM holistic.ma_users_meta_1win
                                      WHERE withdrawal_block
                                         OR fake_account
                                         OR 1win_tester OR
        user_demo_withdrawal OR
        sb_users OR
        cash_test OR
        payment_scammers OR
        dd_fm_partner_advertising_accounts)
    GROUP BY user_id,
             session_id,
             date
)

SELECT
    dp.user_id,
    dp.date,

    dp.back_deposit_amount,
    dp.back_deposit_count,

    fr.front_deposit_to_confirm_amount,
    front_deposit_to_confirm_count,
    fr.front_deposit_submit_amount,
    front_deposit_submit_count
FROM deps dp
LEFT JOIN front AS fr ON fr.user_id = dp.user_id AND fr.date = dp.date

SETTINGS
    max_execution_time = 36000000000;
