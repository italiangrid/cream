
/************ Add INSERT Stored Procedure to Database ***************/

/************ Stored Procedure: insertActivityTable ***************/
DROP PROCEDURE IF EXISTS insertActivityTable //
-- Name: insertActivityTable
-- Parameters: ...
CREATE PROCEDURE insertActivityTable(
                 activityId_param VARCHAR(14),
                 userId_param TEXT, 
                 name_param VARCHAR(256),
                 description_param VARCHAR(256),
                 type_param VARCHAR(50),
                 input_param VARCHAR(256),
                 output_param VARCHAR(256),
                 error_param VARCHAR(256),
                 expiration_time_date_param TIMESTAMP,
                 expiration_time_optional_param BOOL,
                 wipe_time_duration_param VARCHAR(100),
                 wipe_time_optional_param BOOL,
                 client_data_push_param BOOL,
                 queue_name_param VARCHAR(50)) COMMENT 'insert into activity table'
BEGIN
INSERT INTO activity (activityId, userId, name, description, type, input, output, error, expiration_time_date, expiration_time_optional, wipe_time_duration, wipe_time_optional, client_data_push, queue_name) 
VALUES (activityId_param, userId_param, name_param, description_param, type_param, input_param, output_param, error_param, expiration_time_date_param, expiration_time_optional_param, wipe_time_duration_param, wipe_time_optional_param, client_data_push_param, queue_name_param);     
END //

/************ Stored Procedure: insertActivityStatusTable ***************/
DROP PROCEDURE IF EXISTS insertActivityStatusTable //
-- Name: insertActivityStatusTable
-- Parameters: ...
CREATE PROCEDURE insertActivityStatusTable(
                 activityId_param VARCHAR(14),
                 status_param VARCHAR(50),
                 timestamp_param TIMESTAMP,
                 description_param TEXT,
                 is_transient_param BOOL,
                 OUT activityStatusId_param BIGINT) COMMENT 'insert into activity_status table'
BEGIN
INSERT INTO activity_status (activityId, status, timestamp, description, is_transient) VALUES (activityId_param, status_param, timestamp_param, description_param, is_transient_param);     
SELECT last_insert_id() INTO activityStatusId_param; 
END //

/************ Stored Procedure: insertActivityStatusAttributeTable ***************/
DROP PROCEDURE IF EXISTS insertActivityStatusAttributeTable //
-- Name: insertActivityStatusAttributeTable
-- Parameters: ...
CREATE PROCEDURE insertActivityStatusAttributeTable(
                 activity_status_id_param BIGINT,
                 attribute_param VARCHAR(50)) COMMENT 'insert into activity_status_attribute table'
BEGIN
INSERT INTO activity_status_attribute (activity_status_id, attribute) VALUES (activity_status_id_param, attribute_param);     
END //

/************ Stored Procedure: insertActivityCommandTable ***************/
DROP PROCEDURE IF EXISTS insertActivityCommandTable //
-- Name: insertActivityCommandTable
-- Parameters: ...
CREATE PROCEDURE insertActivityCommandTable(
                 activityId_param VARCHAR(14),
                 name_param VARCHAR(50),
                 timestamp_param TIMESTAMP,
                 is_success_param BOOL,
                 OUT activityCommandId_param BIGINT) COMMENT 'insert into activity_command table'
BEGIN
INSERT INTO activity_command (activityId, name, timestamp, is_success) VALUES (activityId_param, name_param, timestamp_param, is_success_param);     
SELECT last_insert_id() INTO activityCommandId_param;
END //

/************ Stored Procedure: insertActivityPropertyTable ***************/
DROP PROCEDURE IF EXISTS insertActivityPropertyTable //
-- Name: insertActivityPropertyTable
-- Parameters: ...
CREATE PROCEDURE insertActivityPropertyTable(
                   activityId_param VARCHAR(14),
                   name_param VARCHAR(50),
                   value_param TEXT) COMMENT 'insert into activity_property table'
BEGIN
INSERT INTO activity_property (activityId, name, value) VALUES (activityId_param, name_param, value_param);     
END //

/************ Stored Procedure: insertAnnotationTable ***************/
DROP PROCEDURE IF EXISTS insertAnnotationTable //
-- Name: insertAnnotationTable
-- Parameters: ...
CREATE PROCEDURE insertAnnotationTable(
                 activityId_param VARCHAR(14),
                 value_param TEXT) COMMENT 'insert into annotation table'
BEGIN
INSERT INTO annotation (activityId, value) VALUES (activityId_param, value_param);     
END //

/************ Stored Procedure: insertEnvironmentTable ***************/
DROP PROCEDURE IF EXISTS insertEnvironmentTable //
-- Name: insertEnvironmentTable
-- Parameters: ...
CREATE PROCEDURE insertEnvironmentTable(
                 activityId_param VARCHAR(14),
                 name_param VARCHAR(50),
                 value_param VARCHAR(50)) COMMENT 'insert into environment table'
BEGIN
INSERT INTO environment (activityId, name, value) VALUES (activityId_param, name_param, value_param);     
END //

/************ Stored Procedure: insertNotificationOnStateTable ***************/
DROP PROCEDURE IF EXISTS insertNotificationOnStateTable //
-- Name: insertNotificationOnStateTable
-- Parameters: ...
CREATE PROCEDURE insertNotificationOnStateTable(
                 notification_id_param BIGINT,
                 onstate_param VARCHAR(256)) COMMENT 'insert into notification_onstate table'
BEGIN
INSERT INTO notification_onstate (notification_id, onstate) VALUES (notification_id_param, onstate_param);     
END //

/************ Stored Procedure: insertRemoteLoggingTable ***************/
DROP PROCEDURE IF EXISTS insertRemoteLoggingTable //
-- Name: insertRemoteLoggingTable
-- Parameters: ...
CREATE PROCEDURE insertRemoteLoggingTable(
                 activityId_param VARCHAR(50),
                 service_type_param VARCHAR(256),
                 url_param VARCHAR(256),
                 optional_param BOOL) COMMENT 'insert into remote_logging table'
BEGIN
INSERT INTO remote_logging (activityId, service_type, url, optional) VALUES (activityId_param, service_type_param, url_param, optional_param);     
END //

/************ Stored Procedure: insertRecipientTable ***************/
DROP PROCEDURE IF EXISTS insertRecipientTable //
-- Name: insertRecipientTable
-- Parameters: ...
CREATE PROCEDURE insertRecipientTable(
                 notification_id_param BIGINT,
                 value_param VARCHAR(256)) COMMENT 'insert into recipient table'
BEGIN
INSERT INTO recipient (notification_id, value) VALUES (notification_id_param, value_param);     
END //

/************ Stored Procedure: insertNotificationTable ***************/
DROP PROCEDURE IF EXISTS insertNotificationTable //
-- Name: insertNotificationTable
-- Parameters: ...
CREATE PROCEDURE insertNotificationTable(
                activityId_param VARCHAR(50),
                protocol_param VARCHAR(50),
                optional_param BOOL,
                OUT notificationId_param BIGINT) COMMENT 'insert into notification table'
BEGIN
INSERT INTO notification (activityId, protocol,  optional) VALUES (activityId_param, protocol_param, optional_param); 
SELECT last_insert_id() INTO notificationId_param;    
END //

/************ Stored Procedure: insertExecutableTypeArgumentTable ***************/
DROP PROCEDURE IF EXISTS insertExecutableTypeArgumentTable //
-- Name: insertExecutableTypeArgumentTable
-- Parameters: ...
CREATE PROCEDURE insertExecutableTypeArgumentTable(
                executable_type_id_param BIGINT,
                argument_param VARCHAR(256)) COMMENT 'insert into executable_type_argument table'
BEGIN
INSERT INTO executable_type_argument (executable_type_id, argument) VALUES (executable_type_id_param, argument_param); 
END //

/************ Stored Procedure: insertActivityExecutableTypeTable ***************/
DROP PROCEDURE IF EXISTS insertActivityExecutableTypeTable //
-- Name: insertActivityExecutableTypeTable
-- Parameters: ...
CREATE PROCEDURE insertActivityExecutableTypeTable(
                activityId_param VARCHAR(50),
                executable_type_id_param BIGINT,
                type_param VARCHAR(5)) COMMENT 'insert into activity_executable_type table'
BEGIN
INSERT INTO activity_executable_type (activityId, executable_type_id, type) VALUES (activityId_param, executable_type_id_param, type_param); 
END //

/************ Stored Procedure: insertExecutableTypeTable ***************/
DROP PROCEDURE IF EXISTS insertExecutableTypeTable //
-- Name: insertExecutableTypeTable
-- Parameters: ...
CREATE PROCEDURE insertExecutableTypeTable(
                path_param VARCHAR(256),
                failIfExitCodeNotEqualTo_param BIGINT,
                OUT executable_type_id_param BIGINT) COMMENT 'insert into executable_type table'
BEGIN
INSERT INTO executable_type (path, failIfExitCodeNotEqualTo) VALUES (path_param, failIfExitCodeNotEqualTo_param); 
SELECT last_insert_id() INTO executable_type_id_param;    
END //

/************ Stored Procedure: insertInputFileTable ***************/
DROP PROCEDURE IF EXISTS insertInputFileTable //
-- Name: insertInputFileTable
-- Parameters: ...
CREATE PROCEDURE insertInputFileTable(
                activityId_param VARCHAR(14),
                name_param VARCHAR(256),
                is_executable_param BOOL,
                uri_param TEXT,
                delegation_id_param VARCHAR(256),
                OUT input_file_id_param BIGINT) COMMENT 'insert into input_file table'
BEGIN
INSERT INTO input_file (activityId, name, is_executable, uri, delegation_id) 
VALUES (activityId_param, name_param, is_executable_param, uri_param, delegation_id_param); 
SELECT last_insert_id() INTO input_file_id_param;    
END //

/************ Stored Procedure: insertOptionTypeInputFileTable ***************/
DROP PROCEDURE IF EXISTS insertOptionTypeInputFileTable //
-- Name: insertOptionTypeInputFileTable
-- Parameters: ...
CREATE PROCEDURE insertOptionTypeInputFileTable(
                input_file_id_param BIGINT,
                name_param VARCHAR(50),
                value_param VARCHAR(50)) COMMENT 'insert into option_type_input_file table'
BEGIN
INSERT INTO option_type_input_file (input_file_id, name, value) VALUES (input_file_id_param, name_param, value_param); 
END //

/************ Stored Procedure: insertOutputFileTable ***************/
DROP PROCEDURE IF EXISTS insertOutputFileTable //
-- Name: insertOutputFileTable
-- Parameters: ...
CREATE PROCEDURE insertOutputFileTable(
                activityId_param VARCHAR(14),
                name_param VARCHAR(256),
                uri_param TEXT,
                delegation_id_param VARCHAR(256),
                mandatory_param BOOL,
                creation_flag_param VARCHAR(256),
                use_if_failure_param BOOL,
                use_if_cancel_param BOOL,
                use_if_success_param BOOL,
                OUT output_file_id_param BIGINT) COMMENT 'insert into output_file table'
BEGIN
INSERT INTO output_file (activityId, name, uri, delegation_id, mandatory, creation_flag, use_if_failure, use_if_cancel, use_if_success) 
VALUES (activityId_param, name_param, uri_param, delegation_id_param, mandatory_param, creation_flag_param, use_if_failure_param, use_if_cancel_param, use_if_success_param); 
SELECT last_insert_id() INTO output_file_id_param;    
END //

/************ Stored Procedure: insertOptionTypeOutputFileTable ***************/
DROP PROCEDURE IF EXISTS insertOptionTypeOutputFileTable //
-- Name: insertOptionTypeOutputFileTable
-- Parameters: ...
CREATE PROCEDURE insertOptionTypeOutputFileTable(
                output_file_id_param BIGINT,
                name_param VARCHAR(50),
                value_param VARCHAR(50)) COMMENT 'insert into option_type_output_file table'
BEGIN
INSERT INTO option_type_output_file (output_file_id, name, value) VALUES (output_file_id_param, name_param, value_param); 
END //

/******************************************************************/
