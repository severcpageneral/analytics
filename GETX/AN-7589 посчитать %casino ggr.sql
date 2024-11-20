SELECT
    date,

    COUNT(DISTINCT ID) AS uniq_bets,
    COUNT(DIStINCT USER_ID) AS uniq_users,
    SUM(CASE
            WHEN CURRENCY = 'RUB' THEN BET_AMOUNT
            ELSE 0
        END) AS BET_AMOUNT_RUB,

    SUM(CASE
            WHEN CURRENCY = 'RUB' THEN WIN_AMOUNT
            ELSE 0
        END) AS WIN_AMOUNT_RUB,

    SUM(CASE
            WHEN CURRENCY = 'RUB' THEN BET_AMOUNT-WIN_AMOUNT
            ELSE 0
        END) AS GGR_RUB,

    COUNT(DISTINCT CASE WHEN u_id IS NOT NULL THEN ID END) AS unique_bets_filter,
    COUNT(DISTINCT CASE WHEN u_id IS NOT NULL THEN USER_ID END) AS unique_users_filter,
    SUM(CASE
            WHEN CURRENCY = 'RUB' AND u_id IS NOT NULL THEN BET_AMOUNT
            ELSE 0
        END) AS BET_AMOUNT_RUB_FILTER,

    SUM(CASE
            WHEN CURRENCY = 'RUB' AND u_id IS NOT NULL THEN WIN_AMOUNT
            ELSE 0
        END) AS WIN_AMOUNT_RUB_FILTER,

    SUM(CASE
            WHEN CURRENCY = 'RUB'  AND u_id IS NOT NULL THEN BET_AMOUNT-WIN_AMOUNT
            ELSE 0
        END) AS GGR_RUB_RUB_FILTER

FROM (SELECT USER_ID,
             CURRENCY,
             filtr.u_id as u_id,
             ID,
             TO_DATE(CREATED_AT) AS date,
             MAX(BET_AMOUNT) AS BET_AMOUNT,
             MAX(WIN_AMOUNT) AS WIN_AMOUNT

      FROM L1_RAW_GET_X.GETXDB_CASINO_SESSIONS AS ses
               LEFT JOIN (SELECT DISTINCT ID AS u_id
                          FROM L1_RAW_GET_X.GETXDB_USERS
                          WHERE status = 1
                            AND role = 'user'
                            --AND room = 'default'
                            --AND is_payout_disable = 0
                            --AND is_withdrawal_disabled = 0
                        ) AS filtr ON ses.USER_ID = filtr.u_id

      WHERE CREATED_AT BETWEEN '2023-12-01' AND '2024-05-12'
      GROUP BY USER_ID,
             CURRENCY,
             u_id,
             ID,
             date) AS res

GROUP BY date