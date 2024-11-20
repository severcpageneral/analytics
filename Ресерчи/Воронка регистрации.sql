WITH
-- Подзапрос для определения времени создания пользователя в каждой сессии
user_creation AS (
    SELECT
        session_id,
        min(client_event_time) AS user_create_time
    FROM holistic.amplitude_1win
    WHERE
        event_type = 'registration_success'
        AND toDate(client_event_time)  BETWEEN '2024-09-01' AND '2024-09-02'
        AND user_id NOT IN (
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
    GROUP BY session_id
),
-- Подзапрос для расчета времени между событиями
session_times AS (
    SELECT
        se.session_id,
        se.event_type,
        se.client_event_time,
        if(prev_time = 0, NULL, dateDiff('second', prev_time, se.client_event_time)) AS time_diff
    FROM (
        SELECT
            session_id,
            event_type,
            client_event_time,
            lagInFrame(client_event_time) OVER (PARTITION BY session_id ORDER BY client_event_time) AS prev_time
        FROM holistic.amplitude_1win se
        -- Используем GLOBAL JOIN
        GLOBAL JOIN user_creation uc ON se.session_id = uc.session_id
        WHERE se.client_event_time <= uc.user_create_time
            AND toDate(client_event_time)  BETWEEN '2024-09-01' AND '2024-09-02'
            AND event_type IN ('[Amplitude] Start Session', 'registration_form_view', 'registration_phone_field', 'registration_email_field', 'registration_password_field', 'registration_submit', 'registration_success')
            AND user_id NOT IN (
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
    ) se
)

-- Основной запрос
SELECT
    *

FROM session_times

SETTINGS
    max_execution_time = 36000000000;
