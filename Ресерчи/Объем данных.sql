SELECT
    *
   /* table,
    sum(bytes_on_disk) AS total_size_bytes,
    sum(rows) AS total_rows,
    total_size_bytes / total_rows*/
FROM
    system.parts
WHERE
    database = 'holistic'
    AND table = 'amplitude_1win_data'
    AND active = 1
--GROUP BY
 --   table;