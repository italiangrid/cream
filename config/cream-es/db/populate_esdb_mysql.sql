/************ Drop: Database ***************/
DROP DATABASE IF EXISTS esdb;

/************ Create: Database ***************/
CREATE DATABASE esdb;

/************ Use: Database ***************/
USE esdb;

/************ Update: Tables ***************/
/* Build Table Structure */

/******************** Add Table: db_info ************************/
CREATE TABLE db_info
(
    version VARCHAR(5) NOT NULL,
    stored_proc_version VARCHAR(5) NOT NULL,
    startUpTime TIMESTAMP  NULL,
    creationTime TIMESTAMP  NOT NULL
) ENGINE=InnoDB;

/******************** Add Table: activity ************************/
CREATE TABLE activity
(
    activityId VARCHAR(14) NOT NULL,
    userId TEXT NOT NULL,
    name VARCHAR(256) NULL,
    description VARCHAR(256) NULL,
    type VARCHAR(50) NULL,
    input VARCHAR(256) NULL,
    output VARCHAR(256) NULL,
    error VARCHAR(256) NULL,
    expiration_time_date TIMESTAMP NULL,
    expiration_time_optional BOOL NULL,
    wipe_time_duration VARCHAR(100) NULL,
    wipe_time_optional BOOL NULL,
    client_data_push BOOL NULL,
    queue_name VARCHAR(50) NOT NULL,
    PRIMARY KEY (activityId)
) ENGINE=InnoDB;

/* Add Indexes for: activity */
CREATE INDEX activity_userId_Idx ON activity (userId(256));

/******************** Add Table: activity_status ************************/
CREATE TABLE activity_status
(
    id BIGINT NOT NULL AUTO_INCREMENT,
    activityId VARCHAR(14) NOT NULL,
    status VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    description TEXT NULL,
    is_transient BOOL NOT NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB;

/* Add Indexes for: activity_status */
CREATE INDEX activity_status_activityId_Idx ON activity_status (activityId(14));

/******************** Add Table: activity_status_attribute ************************/
CREATE TABLE activity_status_attribute
(
    id BIGINT NOT NULL AUTO_INCREMENT,
    activity_status_id BIGINT NOT NULL,
    attribute VARCHAR(50) NOT NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB;

/******************** Add Table: activity_command ************************/
CREATE TABLE activity_command
(
    id BIGINT NOT NULL AUTO_INCREMENT,
    activityId VARCHAR(14) NOT NULL,
    name VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    is_success BOOL NOT NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB;

/* Add Indexes for: activity_command */
CREATE INDEX activity_command_activityId_Idx ON activity_command (activityId(14));

/******************** Add Table: activity_property ************************/
CREATE TABLE activity_property
(
    activityId VARCHAR(14) NOT NULL,
    name VARCHAR(50) NOT NULL,
    value TEXT NOT NULL
) ENGINE=InnoDB;

/******************** Add Table: annotation ************************/
CREATE TABLE annotation
(
    activityId VARCHAR(14) NOT NULL,
    value TEXT NOT NULL
) ENGINE=InnoDB;

/******************** Add Table: environment ************************/
CREATE TABLE environment
(
    activityId VARCHAR(14) NOT NULL,
    name VARCHAR(50) NOT NULL,
    value VARCHAR(50) NOT NULL
) ENGINE=InnoDB;

/******************** Add Table: remote_logging ************************/
CREATE TABLE remote_logging
(
    activityId VARCHAR(14) NOT NULL,
    service_type VARCHAR(256) NOT NULL,
    url VARCHAR(256) NOT NULL,
    optional BOOL NOT NULL
) ENGINE=InnoDB;

/******************** Add Table: activity_executable_type ************************/
CREATE TABLE activity_executable_type
(
    activityId VARCHAR(14) NOT NULL,
    executable_type_id BIGINT NOT NULL,
    type VARCHAR(5) NOT NULL -- PRE, EXEC, POST
) ENGINE=InnoDB;

/******************** Add Table: executable_type ************************/
CREATE TABLE  executable_type
(
    id BIGINT NOT NULL AUTO_INCREMENT,
    path VARCHAR(256) NOT NULL,
    failIfExitCodeNotEqualTo VARCHAR(10) NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB;

/******************** Add Table: executable_type_argument ************************/
CREATE TABLE executable_type_argument
(
    executable_type_id BIGINT NOT NULL, 
    argument VARCHAR(256) NOT NULL
) ENGINE=InnoDB;

/******************** Add Table: notification ************************/
CREATE TABLE notification
(
    id BIGINT NOT NULL AUTO_INCREMENT,
    activityId VARCHAR(14) NOT NULL,
    protocol VARCHAR(50) NOT NULL,
    optional BOOL NOT NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB;

/******************** Add Table: recipient ************************/
CREATE TABLE recipient
(
    notification_id BIGINT NOT NULL,
    value VARCHAR(256) NOT NULL
) ENGINE=InnoDB;

/******************** Add Table: notification_onstate ************************/
CREATE TABLE notification_onstate
(
    notification_id BIGINT NOT NULL,
    onstate VARCHAR(256) NOT NULL
) ENGINE=InnoDB;

/******************** Add Table: input_file ************************/
CREATE TABLE input_file
(
    id BIGINT NOT NULL AUTO_INCREMENT,
    activityId VARCHAR(14) NOT NULL,
    name VARCHAR(256) NOT NULL,
    is_executable BOOL NULL,
    uri TEXT NULL,
    delegation_id VARCHAR(256) NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB;

/* Add Indexes for: input_file */
CREATE INDEX input_file_activityId_Idx ON input_file (activityId(14));

/******************** Add Table: output_file ************************/
CREATE TABLE output_file
(
    id BIGINT NOT NULL AUTO_INCREMENT,
    activityId VARCHAR(14) NOT NULL,
    name VARCHAR(256) NOT NULL,
    uri TEXT NULL,
    delegation_id VARCHAR(256) NULL,
    mandatory BOOL NULL,
    creation_flag VARCHAR(256) NULL,
    use_if_failure BOOL NULL,
    use_if_cancel BOOL NULL,
    use_if_success BOOL NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB;

/* Add Indexes for: input_file */
CREATE INDEX output_file_activityId_Idx ON output_file (activityId(14));

/******************** Add Table: option_type_input_file ************************/
CREATE TABLE option_type_input_file
(
    input_file_id BIGINT NOT NULL,
    name VARCHAR(50) NOT NULL,
    value VARCHAR(50) NOT NULL
) ENGINE=InnoDB;

/******************** Add Table: option_type_output_file ************************/
CREATE TABLE option_type_output_file
(
    output_file_id BIGINT NOT NULL,
    name VARCHAR(50) NOT NULL,
    value VARCHAR(50) NOT NULL
) ENGINE=InnoDB;

/******************** Add Table: command_queue ************************/
CREATE TABLE command_queue
(
    id                  BIGINT NOT NULL AUTO_INCREMENT,
    commandGroupId      VARCHAR(256) NULL,
    name                VARCHAR(256) NOT NULL,
    category            VARCHAR(256) NULL,
    userId              TEXT NOT NULL,
    description         TEXT NULL,
    failureReason       TEXT NULL,
    statusType          INTEGER NOT NULL,
    creationtime        TIMESTAMP NULL DEFAULT now(),
    isScheduled         BOOL NOT NULL,
    priorityLevel       TINYINT UNSIGNED NOT NULL DEFAULT 0,
    executionMode       CHAR(1) NOT NULL,
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


/************ Add Foreign Keys to Database ***************/

/************ Foreign Key: fk_activity_status_activityId_activity_activityId ***************/
ALTER TABLE activity_status ADD CONSTRAINT fk_activity_status_activityId_activity_activityId
    FOREIGN KEY (activityId) REFERENCES activity (activityId) ON UPDATE CASCADE ON DELETE CASCADE;

/************ Foreign Key: fk_activity_status_attribute_act_status_id_activity_status_id ***************/
ALTER TABLE activity_status_attribute ADD CONSTRAINT fk_activity_status_attribute_act_status_id_activity_status_id
    FOREIGN KEY (activity_status_id) REFERENCES activity_status (id) ON UPDATE CASCADE ON DELETE CASCADE;

/************ Foreign Key: fk_activity_command_activityId_activity_activityId ***************/
ALTER TABLE activity_command ADD CONSTRAINT fk_activity_command_activityId_activity_activityId
    FOREIGN KEY (activityId) REFERENCES activity (activityId) ON UPDATE CASCADE ON DELETE CASCADE;
    
/************ Foreign Key: fk_activity_property_activityId_activity_activityId ***************/  
ALTER TABLE activity_property ADD CONSTRAINT fk_activity_property_activityId_activity_activityId
    FOREIGN KEY (activityId) REFERENCES activity (activityId) ON UPDATE CASCADE ON DELETE CASCADE;
    
/************ Foreign Key: fk_annotation_activityId_activity_activityId ***************/
ALTER TABLE annotation ADD CONSTRAINT fk_annotation_activityId_activity_activityId
    FOREIGN KEY (activityId) REFERENCES activity (activityId) ON UPDATE CASCADE ON DELETE CASCADE;

/************ Foreign Key: fk_environment_activityId_activity_activityId ***************/
ALTER TABLE environment ADD CONSTRAINT fk_environment_activityId_activity_activityId
    FOREIGN KEY (activityId) REFERENCES activity (activityId) ON UPDATE CASCADE ON DELETE CASCADE;

/************ Foreign Key: fk_remote_logging_activityId_activity_activityId ***************/
ALTER TABLE remote_logging ADD CONSTRAINT fk_remote_logging_activityId_activity_activityId
    FOREIGN KEY (activityId) REFERENCES activity (activityId) ON UPDATE CASCADE ON DELETE CASCADE;

/************ Foreign Key: fk_activity_executable_type_activityId_activity_activityId ***************/
ALTER TABLE activity_executable_type ADD CONSTRAINT fk_activity_executable_activityId_activity_activityId
    FOREIGN KEY (activityId) REFERENCES activity (activityId) ON UPDATE CASCADE ON DELETE CASCADE;

/************ Foreign Key: fk_exec_type_argument_executable_type_id_executable_type_id ***************/
ALTER TABLE executable_type_argument ADD CONSTRAINT fk_exec_type_argument_executable_type_id_executable_type_id
    FOREIGN KEY (executable_type_id) REFERENCES executable_type (id) ON UPDATE CASCADE ON DELETE CASCADE;

/************ Foreign Key: fk_activity_executable_type_executable_type_id_executable_type_id ***************/
ALTER TABLE activity_executable_type ADD CONSTRAINT fkactivity_executable_type_executable_type_id_executable_type_id
    FOREIGN KEY (executable_type_id) REFERENCES executable_type (id) ON UPDATE CASCADE ON DELETE CASCADE;

/************ Foreign Key: fk_notification_activityId_activity_activityId ***************/
ALTER TABLE notification ADD CONSTRAINT fk_notification_activityId_activity_activityId
    FOREIGN KEY (activityId) REFERENCES activity (activityId) ON UPDATE CASCADE ON DELETE CASCADE;

/************ Foreign Key: fk_recipient_notification_id_notification_id ***************/
ALTER TABLE recipient ADD CONSTRAINT fk_recipient_notification_id_notification_id
    FOREIGN KEY (notification_id) REFERENCES notification (id) ON UPDATE CASCADE ON DELETE CASCADE;

/************ Foreign Key: fk_notification_onstate_notification_id_notification_id ***************/
ALTER TABLE notification_onstate ADD CONSTRAINT fk_notification_onstate_notification_id_notification_id
    FOREIGN KEY (notification_id) REFERENCES notification (id) ON UPDATE CASCADE ON DELETE CASCADE;

/************ Foreign Key: fk_input_file_activityId_activity_activityId ***************/
ALTER TABLE input_file ADD CONSTRAINT fk_input_file_activityId_activity_activityId
    FOREIGN KEY (activityId) REFERENCES activity (activityId) ON UPDATE CASCADE ON DELETE CASCADE;

/************ Foreign Key: fk_output_file_activityId_activity_activityId ***************/
ALTER TABLE output_file ADD CONSTRAINT fk_output_file_activityId_activity_activityId
    FOREIGN KEY (activityId) REFERENCES activity (activityId) ON UPDATE CASCADE ON DELETE CASCADE;

/************ Foreign Key: fk_option_type_input_file_input_file_id_input_file_id ***************/
ALTER TABLE option_type_input_file ADD CONSTRAINT fk_option_type_input_file_input_file_id_input_file_id
    FOREIGN KEY (input_file_id) REFERENCES input_file (id) ON UPDATE CASCADE ON DELETE CASCADE;

/************ Foreign Key: fk_option_type_output_file_output_file_id_output_file_id ***************/
ALTER TABLE option_type_output_file ADD CONSTRAINT fk_option_type_output_file_output_file_id_output_file_id
    FOREIGN KEY (output_file_id) REFERENCES output_file (id) ON UPDATE CASCADE ON DELETE CASCADE;

/************ Foreign Key: fk_command_queue_parameter_commandId_command_queue_id ***************/
ALTER TABLE command_queue_parameter ADD CONSTRAINT fk_command_queue_parameter_commandId_command_queue_id
    FOREIGN KEY (commandId) REFERENCES command_queue (id) ON UPDATE CASCADE ON DELETE CASCADE;
    
/************ Insert values in db_info ***************/    
insert into db_info (version, stored_proc_version, startUpTime, creationTime) values ('1.1', '0', NULL, now());

commit;   

