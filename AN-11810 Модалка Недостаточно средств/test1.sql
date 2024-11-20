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
    AND event_type IN ('casino_no_funds', 'casino_no_funds_to_deposit')
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
)


SELECT *
FROM sessions


SETTINGS
    max_execution_time = 36000000000;