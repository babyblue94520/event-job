create table if not exists event_job
(
    `instance`      varchar(100) not null default '',
    `group`         varchar (100) not null default '',
    `name`          varchar(100) not null default '',
    `event`         varchar(100) not null default '',
    `timezone`      varchar(6) not null default '',
    `description`   varchar(200) not null default '',
    `cron`          varchar(200) not null default '',
    `after_group`   varchar(100) not null default '',
    `after_name`    varchar(100) not null default '',
    `prev_time`     bigint not null default 0,
    `next_time`     bigint not null default 0,
    `enabled`       tinyint not null default 1,
    `status`        int not null default 0,
    `start_time`    bigint not null default 0,
    `end_time`      bigint not null default 0,
    `data`          text,
    primary key (`instance`, `group`, `name`)
);
