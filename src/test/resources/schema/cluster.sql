CREATE TABLE IF NOT EXISTS `log`
(
    `instance` varchar(100) NOT NULL DEFAULT '',
    `group`    varchar(100) NOT NULL DEFAULT '',
    `name`     varchar(100) NOT NULL DEFAULT '',
    `service`  varchar(64)  NOT NULL,
    `count`    bigint(19)   NOT NULL,
    PRIMARY KEY (`instance`, `group`, `name`, `service`)
);
