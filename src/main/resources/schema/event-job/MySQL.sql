CREATE TABLE IF NOT EXISTS `event_job`
(
    `instance`    varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
    `group`       varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
    `name`        varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
    `event`       varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
    `timezone`    varchar(6) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci   NOT NULL DEFAULT '',
    `description` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
    `cron`        varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
    `after_group` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
    `after_name`  varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
    `prev_time`   bigint(13)                                                    NOT NULL DEFAULT 0,
    `next_time`   bigint(13)                                                    NOT NULL DEFAULT 0,
    `enabled`     tinyint(1)                                                    NOT NULL DEFAULT 1,
    `status`      int(1)                                                        NOT NULL DEFAULT 0,
    `start_time`  bigint(13)                                                    NOT NULL DEFAULT 0,
    `end_time`    bigint(13)                                                    NOT NULL DEFAULT 0,
    `data`        text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci         NULL,
    PRIMARY KEY (`instance`, `group`, `name`) USING BTREE
) ENGINE = InnoDB
  CHARACTER SET = utf8mb4
  COLLATE = utf8mb4_unicode_ci
  ROW_FORMAT = Dynamic;
