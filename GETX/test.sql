SELECT
    DATE_TRUNC('day', CREATED_AT) as day,
    SUM(AMOUNT_REAL) as amount_real,
    COUNT(AMOUNT_REAL) as count_real,
    SUM(CASE
            WHEN IS_FAKE = 1 THEN AMOUNT_REAL
        ELSE 0) AS amount_is_fake,
    SUM(CASE
            WHEN IS_FAKE = 1 THEN 1
        ELSE 0) AS count_is_fake,

FROM L1_RAW_GET_X.GETXDB_PAYMENTS
WHERE
    CREATED_AT  > '2024-05-23'
    and user_currency ='RUB'
group by day