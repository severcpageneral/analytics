SELECT event_type,
       event_properties.ep_value[indexOf(event_properties.ep_key, 'registration_error_text')]

FROM holistic.amplitude_1win AS amp
WHERE --toDate(amp.server_upload_time) BETWEEN toDate(start_date) AND toDate(end_date)
    toDate(amp.server_upload_time) BETWEEN toDate('2024-11-21') AND toDate('2024-11-24')
  AND event_type IN ('registration_success', 'registration_form_view', 'registration_submit')
  OR (
    event_type IN ('registration_error')
        AND
    event_properties.ep_value[indexOf(event_properties.ep_key, 'registration_error_text')] IN
    ('Google reCAPTCHA widget was closed by the user',
     'Unable to load recaptcha script',
     'captcha.invalid')
    )

SETTINGS
    max_execution_time = 36000000000;