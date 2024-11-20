WITH report_date AS (
    SELECT toDate('2024-10-10') AS report_date
),

bets AS (
SELECT
    user_id,
    CASE
        WHEN provider IN ('Spribe', 'Spinomenal', '3 Oaks Gaming', 'Hacksaw', '1play') THEN 'Support'
        WHEN provider IN ('Pragmatic', 'BGaming', 'Evolution', 'Smartsoft', 'Endorphina') THEN 'Not Support'
        ELSE 'Other'
    END AS provider_type,
    toDate(datetime) AS bet_date,
    datetime AS bet_time,
    SUM(CASE
        WHEN event = 'BET' AND is_refund != 1 THEN 1
        ELSE 0
    END) AS bets,
    avg(CASE
        WHEN event = 'BET' AND is_refund != 1 THEN amount_converted
        ELSE 0
    END) AS bets_amount

FROM enriched.casino
WHERE
    datetime >= (SELECT report_date FROM report_date)
    --AND provider IN ('Spribe','Spinomenal','3 Oaks Gaming', 'Hacksaw', '1play','Pragmatic', 'BGaming','Evolution', 'Smartsoft', 'Endorphina')
    AND event = 'BET'
    AND status = 1
    AND is_refund != 1
    AND hasAny(
        [
            1,  -- withdrawal_block
            2,  -- 1win_tester
            3,  -- fake_account
            25, -- sb_users
            4,  -- User_demo_withdrawal
            26, -- cash_test
            27, -- payment scammers
            29  -- dd_fm_partner_advertising_accounts
        ],
        users_marks
    ) = False
GROUP BY
    user_id,
    provider_type,
    bet_date,
    bet_time
    ),

sessions AS (
SELECT
    user_id,
    session_id,
    toDate(server_upload_time) as modal_date,
    server_upload_time AS modal_time,
    arrayElement(user_properties.up_value, arrayFirstIndex(x -> x = 'country', user_properties.up_key)) AS country,
    lower(arrayElement(user_properties.up_value, arrayFirstIndex(x -> x = 'platform', user_properties.up_key))) AS platform,

    COUNT(event_type = 'casino_no_funds') AS casino_no_funds
FROM holistic.amplitude_1win a
WHERE server_upload_time >= (SELECT report_date FROM report_date)
    AND event_type = 'casino_no_funds'
    AND user_id NOT IN (
        SELECT DISTINCT toUInt64(user_id)
        FROM holistic.ma_users_meta_1win
        WHERE fake_account = true
            OR withdrawal_block = true
            OR 1win_tester = true
            OR sb_users = true
            OR cash_test = true
            OR cash_agent = true
            OR user_demo_withdrawal = true
            OR dd_fm_partner_advertising_accounts = true
    AND user_id > 0
    )
GROUP BY
    user_id,
    session_id,
    modal_date,
    modal_time,
    country,
    platform
),


 deposit AS(
     SELECT user_id,
            toDate(time_confirm) AS deposit_date,
            time_confirm AS deposit_time,
            COUNT(*) AS dep
    FROM enriched.payments od
    WHERE toDate(time_confirm) >= (SELECT report_date FROM report_date)
        AND status = 1
        AND event = 'DEPOSIT'
        AND is_real_operation = TRUE
        AND number_of_f4_dep != 1
        AND hasAny([25, 20, 21], od.users_marks) = 0
    GROUP BY
        user_id,
        deposit_date,
        time_confirm
 )

SELECT ss.user_id,
       ss.session_id,
       ss.country,
       ss.platform,
       ss.casino_no_funds,
       ss.casino_no_funds_to_deposit,
       ss.modal_time,
       b.bet_time,
       dp.deposit_time,
       b.bets,
       b.bets_amount,
       b.provider_type,
       dp.dep
       /*
       CASE
           WHEN dateDiff('minute', ss.modal_date, dp.deposit_date) <= 3 THEN '3 minute'
           WHEN dateDiff('day', ss.modal_date, dp.deposit_date) <= 1 THEN '1 day'
           WHEN dp.deposit_date <  THEN '1 day'
           ELSE 0
        END AS is_deposit
*/
FROM sessions ss
LEFT JOIN deposit dp ON toUInt64(ss.user_id) = dp.user_id AND ss.modal_date = dp.deposit_date
LEFT JOIN bets b ON toUInt64(ss.user_id) = b.user_id AND ss.modal_date = b.bet_date
WHERE ss.user_id > 1
ORDER BY
    ss.user_id,
    ss.modal_time,
    b.bet_date,
    dp.deposit_time

SETTINGS
    max_execution_time = 36000000000;