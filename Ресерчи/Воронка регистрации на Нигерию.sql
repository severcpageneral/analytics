SELECT event_type,
       COUNT(distinct a.amplitude_id) as amplitude_id
FROM holistic.amplitude_1win a
WHERE toDate(server_upload_time) >= '2024-10-14'
AND (user_properties.up_value[indexOf(user_properties.up_key, 'country')]) = 'Nigeria'
AND event_type IN ('registration_form_view',
'registration_form_exit',
'registration_submit',
'registration_phone_field',
'registration_password_field',
'registration_promocode',
'registration_promocode_field',
'registration_to_agreement',
'registration_success',
'registration_error',
'phone_confirmation_form_view',
'phone_confirmation_phone_change_field',
'phone_confirmation_change_phone',
'phone_confirmation_phone_sms_field',
'phone_confirmation_error',
'phone_confirmation_success',
'phone_confirmation_resend_code',
'phone_confirmation_send_new_code',
'phone_confirmation_contact_ss',
'mail_entry_submit',
'mail_entry_form_view',
'mail_entry_mail_field',
'mail_entry_success',
'mail_entry_error',
'mail_entry_form_exit')
GROUP BY event_type

SETTINGS
    max_execution_time = 36000000000;