WITH user_group_log AS (
    SELECT
        luga.hk_group_id,
        COUNT(DISTINCT luga.hk_user_id) AS cnt_added_users
    FROM
        STV2024071525__DWH.s_auth_history AS suh
        LEFT JOIN STV2024071525__DWH.l_user_group_activity AS luga ON suh.hk_l_user_group_activity = luga.hk_l_user_group_activity
        LEFT JOIN STV2024071525__DWH.h_groups AS hg ON luga.hk_group_id = hg.hk_group_id
    WHERE
        suh.event = 'add'
    GROUP BY
        luga.hk_group_id,
        hg.registration_dt
    ORDER BY
        hg.registration_dt ASC
    LIMIT
        10
), user_group_messages AS (
    SELECT
        lgd.hk_group_id,
        COUNT(DISTINCT lum.hk_user_id) AS cnt_users_in_group_with_messages
    FROM
        STV2024071525__DWH.l_user_message AS lum
        LEFT JOIN STV2024071525__DWH.l_groups_dialogs AS lgd ON lum.hk_message_id = lgd.hk_message_id
    GROUP BY
        lgd.hk_group_id
)
SELECT
    ugm.hk_group_id,
    ugl.cnt_added_users,
    ugm.cnt_users_in_group_with_messages,
    (
        ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users
    ) AS group_conversion
FROM
    user_group_log AS ugl
    LEFT JOIN user_group_messages AS ugm ON ugl.hk_group_id = ugm.hk_group_id
ORDER BY
    ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users DESC;