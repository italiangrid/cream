/************ Drop: Database ***************/
DROP DATABASE IF EXISTS delegationesdb;

/************ Create: Database ***************/
CREATE DATABASE delegationesdb;

/************ Use delegationesdb ***************/
use delegationesdb;

/******************** Add Tables ************************/
CREATE TABLE db_info (
    version            VARCHAR(5) NOT NULL,
    delegationSuffix   VARCHAR(14) NOT NULL,
    creationTime       TIMESTAMP NOT NULL
) ENGINE=InnoDB;

/******************** Add Table: delegation ************************/
CREATE TABLE delegation (
   id               VARCHAR(255) NOT NULL,
   dn               VARCHAR(255) NOT NULL,
   fqan             VARCHAR(255) NULL,
   vo               VARCHAR(50) NOT NULL,
   localUser        VARCHAR(100) NOT NULL,
   localUserGroup   VARCHAR(100) NOT NULL,
   vomsAttribute    TEXT NOT NULL,
   certificate      TEXT NOT NULL,
   info             TEXT NULL,
   startTime        DATETIME NOT NULL,
   expirationTime   DATETIME NOT NULL,
   lastUpdateTime   DATETIME NULL,
   PRIMARY KEY (id, dn)
) ENGINE=InnoDB;

/******************** Add Table: delegation_request ************************/
CREATE TABLE delegation_request (
   id                   VARCHAR(255) NOT NULL,
   dn                   VARCHAR(255) NOT NULL,
   localUser            VARCHAR(100) NOT NULL,
   certificateRequest   TEXT NOT NULL,
   publicKey            TEXT NOT NULL,
   privateKey           TEXT NOT NULL,
   vomsAttribute        TEXT NOT NULL,
   timestamp            DATETIME NOT NULL,
   PRIMARY KEY (id, dn)
) ENGINE=InnoDB;


/******************** Add Table: command_queue ************************/
CREATE TABLE command_queue (
    id               BIGINT NOT NULL AUTO_INCREMENT,
    commandGroupId   VARCHAR(256) NULL,
    name             VARCHAR(256) NOT NULL,
    category         VARCHAR(256) NULL,
    userId           TEXT NOT NULL,
    description      TEXT NULL,
    failureReason    TEXT NULL,
    statusType       INTEGER NOT NULL,
    creationtime     TIMESTAMP NULL DEFAULT now(),
    isScheduled      BOOL NOT NULL,
    priorityLevel    TINYINT UNSIGNED NOT NULL DEFAULT 0,
    executeMode      CHAR(1) NOT NULL,
    PRIMARY KEY (id)
)  ENGINE=InnoDB;

/******************** Add Table: command_queue_parameter ************************/
CREATE TABLE command_queue_parameter (
    id          BIGINT NOT NULL AUTO_INCREMENT,
    commandId   BIGINT NOT NULL,
    name        VARCHAR(256) NOT NULL,
    value       TEXT NOT NULL,
    PRIMARY KEY (id)
)  ENGINE=InnoDB;

/************ Foreign Key: fk_command_queue_parameter_commandId_command_queue_id ***************/
ALTER TABLE command_queue_parameter ADD CONSTRAINT fk_command_queue_parameter_commandId_command_queue_id
    FOREIGN KEY (commandId) REFERENCES command_queue (id) ON UPDATE CASCADE ON DELETE CASCADE;

/************ Insert values in db_info ***************/
insert into db_info (version, creationTime, delegationSuffix) values ('1.1', now(), floor(pow(10,13) + rand()*POW(10,13)));

commit;
