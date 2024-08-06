INSERT INTO
    STV2024071525__DWH.h_users (
        hk_user_id,
        user_id,
        registration_dt,
        load_dt,
        load_src
    )
SELECT
    hash(U.id) AS hk_user_id,
    U.id AS user_id,
    U.registration_dt,
    now() AS load_dt,
    's3' AS load_src
FROM
    STV2024071525__STAGING.users AS U
    LEFT JOIN STV2024071525__STAGING.group_log AS GL ON GL.user_id = U.id
WHERE
    hash(U.id) NOT IN (
        SELECT
            hk_user_id
        FROM
            STV2024071525__DWH.h_users
    );

INSERT INTO
    STV2024071525__DWH.h_groups (
        hk_group_id,
        group_id,
        registration_dt,
        load_dt,
        load_src
    )
SELECT
    hash(G.id) AS hk_group_id,
    G.id AS group_id,
    G.registration_dt,
    now() AS load_dt,
    's3' AS load_src
FROM
    STV2024071525__STAGING.groups AS G
    LEFT JOIN STV2024071525__STAGING.group_log AS GL ON GL.group_id = G.id
WHERE
    hash(group_id) NOT IN (
        SELECT
            hk_group_id
        FROM
            STV2024071525__DWH.h_groups
    );