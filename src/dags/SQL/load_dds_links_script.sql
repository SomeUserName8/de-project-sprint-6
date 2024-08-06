INSERT INTO
    STV2024071525__DWH.l_user_group_activity (
        hk_l_user_group_activity,
        hk_user_id,
        hk_group_id,
        load_dt,
        load_src
    )
SELECT
    DISTINCT hash(hu.hk_user_id, hg.hk_group_id),
    hu.hk_user_id,
    hg.hk_group_id,
    now() AS load_dt,
    's3' AS load_src
FROM
    STV2024071525__STAGING.group_log AS GL
    LEFT JOIN STV2024071525__DWH.h_users AS hu ON GL.user_id = hu.user_id
    LEFT JOIN STV2024071525__DWH.h_groups AS hg ON GL.group_id = hg.group_id
WHERE
    hash(hu.hk_user_id, hg.hk_group_id) NOT IN (
        SELECT
            hk_l_user_group_activity
        FROM
            STV2024071525__DWH.l_user_group_activity
    );