TRUNCATE TABLE STV2024071525__DWH.s_auth_history;

INSERT INTO
    STV2024071525__DWH.s_auth_history (
        hk_l_user_group_activity,
        user_id_from,
        event,
        event_dt,
        load_dt,
        load_src
    )
SELECT
    DISTINCT la.hk_l_user_group_activity,
    GL.user_id_from,
    GL.event,
    GL.event_ts as event_dt,
    now() as load_dt,
    's3' as load_src
FROM
    STV2024071525__DWH.l_user_group_activity AS la
    LEFT JOIN STV2024071525__DWH.h_users AS hu ON la.hk_user_id = hu.hk_user_id
    LEFT JOIN STV2024071525__DWH.h_groups AS hg ON la.hk_group_id = hg.hk_group_id
    LEFT JOIN STV2024071525__STAGING.group_log AS GL ON GL.user_id = hu.user_id
    AND GL.group_id = hg.group_id;