SELECT
    country,
    uniq(session_id) AS unique_sessions,

    -- Нажата карточка игры
    SUM(GameClicked_count) AS total_GameClicked,
    AVG(GameClicked_count) AS avg_GameClicked_per_session,

    -- Список игр доскролен следующей страницы
    SUM(GameOffsetReached_count) AS total_GameOffsetReached,
    AVG(GameOffsetReached_count) AS avg_GameOffsetReached_per_session,

    -- Открыт экран поиска игр
    SUM(Init_count) AS total_Init,
    AVG(Init_count) AS avg_Init_per_session,

    -- Нажата кнопка авторизации
    SUM(LoginBtnClicked_count) AS total_LoginBtnClicked,
    AVG(LoginBtnClicked_count) AS avg_LoginBtnClicked_per_session,

    -- Изменен текст поиска игры
    SUM(SearchTextChanged_count) AS total_SearchTextChanged,
    AVG(SearchTextChanged_count) AS avg_SearchTextChanged_per_session,

    -- Нажата кнопка ‘Играть’
    SUM(StartPlay_count) AS total_StartPlay,
    AVG(StartPlay_count) AS avg_StartPlay_per_session


FROM
(
    SELECT
        arrayElement(user_properties.up_value, arrayFirstIndex(x -> x = 'country', user_properties.up_key)) AS country,
        session_id,
        SUM(event_type = 'GameClicked') AS GameClicked_count,
        SUM(event_type = 'GameOffsetReached') AS GameOffsetReached_count,
        SUM(event_type = 'Init') AS Init_count,
        SUM(event_type = 'LoginBtnClicked') AS LoginBtnClicked_count,
        SUM(event_type = 'SearchTextChanged') AS SearchTextChanged_count,
        SUM(event_type = 'StartPlay') AS StartPlay_count
    FROM holistic.ma_android_nativeapp_markup
    WHERE
        arrayElement(event_properties.ep_value, arrayFirstIndex(x -> x = 'screen', event_properties.ep_key)) = 'casino/search'
        AND arrayElement(user_properties.up_value, arrayFirstIndex(x -> x = 'country', user_properties.up_key)) IN ('IN', 'MX')
        AND toDate(client_event_time) >= '2024-08-01'
        AND event_type IN ('GameClicked',       -- Нажата карточка игры
                          'GameOffsetReached',  -- Список игр доскролен следующей страницы
                          'Init',               -- Открыт экран поиска игр
                          'LoginBtnClicked',    -- Нажата кнопка авторизации
                          'SearchTextChanged',  -- Изменен текст поиска игры
                          'StartPlay'           -- Нажата кнопка ‘Играть’
                         )
    GROUP BY
        country,
        session_id
)
GROUP BY country
SETTINGS max_execution_time = 36000000000;
