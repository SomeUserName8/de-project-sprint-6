--staging ddl
DROP TABLE IF EXISTS STV2024071525__STAGING.group_log;

CREATE TABLE STV2024071525__STAGING.group_log (
    group_id integer NOT NULL,
    user_id integer NOT NULL,
    user_id_from integer,
    event varchar(20),
    event_ts timestamp(0),
    CONSTRAINT group_log_pkey PRIMARY KEY (user_id, event_ts)
)
ORDER BY
    group_id,
    user_id SEGMENTED BY hash(group_id) ALL NODES PARTITION BY event_ts :: Date
GROUP BY
    CALENDAR_HIERARCHY_DAY(group_log.event_ts :: Date, 3, 2);

drop table if exists STV2024071525__STAGING.users;

create table STV2024071525__STAGING.users(
    id int primary key,
    chat_name varchar(200),
    registration_dt timestamp,
    country varchar(200),
    age int
)
ORDER BY
    id;

drop table if exists STV2024071525__STAGING.groups;

create table STV2024071525__STAGING.groups(
    id int primary key,
    admin_id int,
    group_name varchar(100),
    registration_dt timestamp,
    is_private boolean
)
order by
    id,
    admin_id PARTITION BY registration_dt :: date
GROUP BY
    calendar_hierarchy_day(registration_dt :: date, 3, 2);

--dds ddl
DROP TABLE IF EXISTS STV2024071525__DWH.h_users CASCADE;

CREATE TABLE STV2024071525__DWH.h_users (
    hk_user_id bigint PRIMARY KEY,
    user_id integer,
    registration_dt timestamp(0),
    load_dt datetime,
    load_src varchar(20)
)
ORDER BY
    load_dt SEGMENTED BY hk_user_id ALL NODES PARTITION BY load_dt :: date
GROUP BY
    calendar_hierarchy_day(load_dt :: date, 3, 2);

DROP TABLE IF EXISTS STV2024071525__DWH.h_groups CASCADE;

CREATE TABLE STV2024071525__DWH.h_groups (
    hk_group_id bigint PRIMARY KEY,
    group_id integer,
    registration_dt timestamp(6),
    load_dt datetime,
    load_src varchar(20)
)
ORDER BY
    load_dt SEGMENTED BY hk_group_id ALL NODES PARTITION BY load_dt :: date
GROUP BY
    calendar_hierarchy_day(load_dt :: date, 3, 2);

DROP TABLE IF EXISTS STV2024071525__DWH.l_user_group_activity;

CREATE TABLE STV2024071525__DWH.l_user_group_activity (
    hk_l_user_group_activity bigint PRIMARY KEY,
    hk_user_id integer NOT NULL CONSTRAINT l_user_group_activity_user_fkey REFERENCES STV2024071525__DWH.h_users (hk_user_id),
    hk_group_id integer NOT NULL CONSTRAINT l_user_group_activity_group_fkey REFERENCES STV2024071525__DWH.h_groups (hk_group_id),
    load_dt datetime,
    load_src varchar(20)
)
ORDER BY
    load_dt SEGMENTED BY hk_user_id ALL nodes PARTITION BY load_dt :: date
GROUP BY
    calendar_hierarchy_day(load_dt :: date, 3, 2);

DROP TABLE IF EXISTS STV2024071525__DWH.s_auth_history;

CREATE TABLE STV2024071525__DWH.s_auth_history (
    hk_l_user_group_activity bigint NOT NULL CONSTRAINT fk_s_auth_history_l_user_group_activity REFERENCES STV2024071525__DWH.l_user_group_activity (hk_l_user_group_activity),
    user_id_from integer,
    event varchar(20),
    event_dt timestamp(0),
    load_dt datetime,
    load_src varchar(20)
)
ORDER BY
    load_dt SEGMENTED BY hk_l_user_group_activity ALL NODES PARTITION BY load_dt :: date
GROUP BY
    calendar_hierarchy_day(load_dt :: date, 3, 2);