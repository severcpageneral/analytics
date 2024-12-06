SELECT user_properties.up_value[indexOf(user_properties.up_key, 'domain')]         AS up_domain,
       user_properties.up_value[indexOf(user_properties.up_key, 'country')]        AS up_country,
       event_properties.ep_value[indexOf(event_properties.ep_key, 'domain')]       AS ep_domain,
       event_properties.ep_value[indexOf(event_properties.ep_key, 'country_code')] AS ep_country,
       COUNT(DISTINCT amplitude_id)                                                AS unique_amplitude_id,
       COUNT(DISTINCT device_id)                                                   AS unique_device_id
FROM (SELECT *
      FROM holistic.amplitude_1win
      WHERE event_type = 'registration_form_view'
        AND toDate(server_upload_time) BETWEEN '2024-11-15' AND '2024-11-22'
        AND ((user_properties.up_value[indexOf(user_properties.up_key, 'country')]) = 'Nigeria'
        OR (event_properties.ep_value[indexOf(event_properties.ep_key, 'country_code')]) = 'Nigeria')
        AND (user_properties.up_value[indexOf(user_properties.up_key, 'domain')] = '1win.ng'
        OR event_properties.ep_value[indexOf(event_properties.ep_key, 'domain')] = '1win.ng')) AS filtered
GROUP BY up_domain,
         up_country,
         ep_domain,
         ep_country;
SETTINGS
    max_execution_time = 36000000000;