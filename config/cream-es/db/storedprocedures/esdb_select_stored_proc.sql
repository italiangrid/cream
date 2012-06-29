
/************ Add SELECT Stored Procedure to Database ***************/

/************ Stored Procedure: selectActivityTable ***************/
DROP PROCEDURE IF EXISTS selectActivityTable //
-- Name: selectActivityTable
-- Parameters: ...
CREATE PROCEDURE selectActivityTable(
                 activityId_param VARCHAR(14),
                 userId_param TEXT) COMMENT 'select activity table'
BEGIN
SELECT name, description, type, input, output, error, expiration_time_date, expiration_time_optional, wipe_time_duration, wipe_time_optional, client_data_push, queue_name 
FROM activity
WHERE activityId = activityId_param 
AND (userId = userId_param OR userId_param IS NULL);
END //

/************ Stored Procedure: selectActivityPropertyTable ***************/
DROP PROCEDURE IF EXISTS selectActivityPropertyTable //
-- Name: selectActivityPropertyTable
-- Parameters: ...
CREATE PROCEDURE selectActivityPropertyTable(
                 activityId_param VARCHAR(14)) COMMENT 'select activity_property table'
BEGIN
SELECT name, value
FROM activity_property
WHERE activityId = activityId_param;
END //

/************ Stored Procedure: selectEnvironmentTable ***************/
DROP PROCEDURE IF EXISTS selectEnvironmentTable //
-- Name: selectEnvironmentTable
-- Parameters: ...
CREATE PROCEDURE selectEnvironmentTable(
                 activityId_param VARCHAR(14)) COMMENT 'select environment table'
BEGIN
SELECT name, value
FROM environment
WHERE activityId = activityId_param;
END //

/************ Stored Procedure: selectActivityStatusTable ***************/
DROP PROCEDURE IF EXISTS selectActivityStatusTable //
-- Name: selectActivityStatusTable
-- Parameters: ...
CREATE PROCEDURE selectActivityStatusTable(
                 activityId_param VARCHAR(14),
                 userId_param TEXT) COMMENT 'select activity_status table'
BEGIN
SELECT astatus.id, astatus.status, astatus.timestamp, astatus.description, astatus.is_transient 
FROM activity_status astatus, activity a
WHERE astatus.activityId = activityId_param
AND astatus.activityId = a.activityId
AND (a.userId = userId_param OR userId_param IS NULL)
order by astatus.timestamp DESC;
END //

/************ Stored Procedure: selectListActivities ***************/
DROP PROCEDURE IF EXISTS selectListActivities //
-- Name: selectListActivities
-- Parameters: ...
CREATE PROCEDURE selectListActivities(
                 fromDate_param TIMESTAMP,
                 toDate_param TIMESTAMP,
                 statusList_param VARCHAR(254),
                 statusAttributeNameList_param VARCHAR(254),
                 userId_param TEXT,
                 limit_param INT) COMMENT 'select ListActivities'
BEGIN
set @sql = 'SELECT DISTINCT a.activityId 
FROM activity a, activity_command acommand, activity_status AS astatus LEFT OUTER JOIN activity_status AS latest ON latest.activityId=astatus.activityId AND astatus.id < latest.id
WHERE latest.id IS null 
AND a.activityId=astatus.activityId 
AND acommand.activityId = a.activityId
AND acommand.name=\'CREATE_ACTIVITY\'';

IF (userId_param IS NOT NULL)
 THEN 
   set @sql =concat(@sql, " \nAND a.userId = '", userId_param, "'");
END IF;

IF (fromDate_param IS NOT NULL)
 THEN 
   set @sql =concat(@sql, " \nAND acommand.timestamp >= '", fromDate_param, "'");
END IF;

IF (toDate_param IS NOT NULL)
 THEN 
   set @sql =concat(@sql, " \nAND acommand.timestamp <= '", toDate_param, "'");
END IF;

IF (statusList_param IS NOT NULL)
 THEN 
   set @sql =concat(@sql, " \nAND astatus.status in (", statusList_param, ")");
END IF;
set @sql =concat(@sql, " \nLIMIT ",limit_param);
prepare STMT from @sql;
execute STMT; 
deallocate prepare STMT;
END //

/************ Stored Procedure: selectRetrieveOlderActivityId ***************/
DROP PROCEDURE IF EXISTS selectRetrieveOlderActivityId //
-- Name: selectRetrieveOlderActivityId
-- Parameters: ...
CREATE PROCEDURE selectRetrieveOlderActivityId(
                 statusList_param VARCHAR(254),
                 userId_param TEXT) COMMENT 'select OlderActivityId'
BEGIN
set @sql = 'SELECT a.activityId 
FROM activity a, activity_status AS astatus LEFT OUTER JOIN activity_status AS latest ON latest.activityId=astatus.activityId AND astatus.id < latest.id
WHERE latest.id IS null 
AND a.activityId=astatus.activityId';

IF (userId_param IS NOT NULL)
 THEN 
   set @sql =concat(@sql, " \nAND a.userId = '", userId_param, "'");
END IF;

IF (statusList_param IS NOT NULL)
 THEN 
   set @sql =concat(@sql, " \nAND astatus.status in (", statusList_param, ")");
END IF;

set @sql =concat(@sql, " \nORDER BY astatus.timestamp LIMIT 1");
prepare STMT from @sql;
execute STMT; 
deallocate prepare STMT;
END //

/************ Stored Procedure: selectListActivitiesForStatus ***************/
DROP PROCEDURE IF EXISTS selectListActivitiesForStatus //
-- Name: selectListActivitiesForStatus
-- Parameters: ...
CREATE PROCEDURE selectListActivitiesForStatus(
                 statusList_param VARCHAR(254),
                 userId_param TEXT,
                 date_value_param INTEGER UNSIGNED) COMMENT 'select ListActivitiesForStatus'
BEGIN
set @sql = 'SELECT DISTINCT a.activityId 
FROM activity a, activity_status AS astatus LEFT OUTER JOIN activity_status AS latest ON latest.activityId=astatus.activityId AND astatus.id < latest.id
WHERE latest.id IS null 
AND a.activityId=astatus.activityId';

IF (userId_param IS NOT NULL)
 THEN 
   set @sql =concat(@sql, " \nAND a.userId = '", userId_param, "'");
END IF;

IF (statusList_param IS NOT NULL)
 THEN 
   set @sql =concat(@sql, " \nAND astatus.status in (", statusList_param, ")");
END IF;

IF (date_value_param > 0)
 THEN 
   set @sql =concat(@sql, " \nAND astatus.timestamp < DATE_SUB(now(), INTERVAL ", date_value_param, " MINUTE)");
END IF;

prepare STMT from @sql;
execute STMT; 
deallocate prepare STMT;
END //

/************ Stored Procedure: selectActivityStatusAttributeTable ***************/
DROP PROCEDURE IF EXISTS selectActivityStatusAttributeTable //
-- Name: selectActivityStatusAttributeTable
-- Parameters: ...
CREATE PROCEDURE selectActivityStatusAttributeTable(
                 activity_status_id_param BIGINT) COMMENT 'select activity_status_attribute table'
BEGIN
SELECT asa.attribute
FROM activity_status_attribute asa
WHERE asa.activity_status_id=activity_status_id_param
order by asa.id DESC;
END //

/************ Stored Procedure: selectAnnotationTable ***************/
DROP PROCEDURE IF EXISTS selectAnnotationTable //
-- Name: selectAnnotationTable
-- Parameters: ...
CREATE PROCEDURE selectAnnotationTable(
                 activityId_param VARCHAR(14)) COMMENT 'select annotation table'
BEGIN
SELECT value
FROM annotation 
WHERE activityId=activityId_param;
END //

/************ Stored Procedure: selectActivityCommandTable ***************/
DROP PROCEDURE IF EXISTS selectActivityCommandTable //
-- Name: selectActivityCommandTable
-- Parameters: ...
CREATE PROCEDURE selectActivityCommandTable(
                 activityId_param VARCHAR(14),
                 userId_param TEXT) COMMENT 'select activity_command table'
BEGIN
SELECT ac.name, ac.timestamp, ac.is_success 
FROM activity_command ac, activity a
WHERE ac.activityId = activityId_param
AND ac.activityId = a.activityId
AND (a.userId = userId_param OR userId_param IS NULL)
order by ac.timestamp DESC;
END //

/************ Stored Procedure: selectNotificationOnStateTable ***************/
DROP PROCEDURE IF EXISTS selectNotificationOnStateTable //
-- Name: selectNotificationOnStateTable
-- Parameters: ...
CREATE PROCEDURE selectNotificationOnStateTable(
                 notification_id_param BIGINT) COMMENT 'select notification_onstate table'
BEGIN
SELECT onstate
FROM notification_onstate 
WHERE notification_id=notification_id_param;
END //

/************ Stored Procedure: selectRecipientTable ***************/
DROP PROCEDURE IF EXISTS selectRecipientTable //
-- Name: selectRecipientTable
-- Parameters: ...
CREATE PROCEDURE selectRecipientTable(
                 notification_id_param VARCHAR(14)) COMMENT 'select recipient table'
BEGIN
SELECT value
FROM recipient 
WHERE notification_id=notification_id_param;
END //

/************ Stored Procedure: selectActivityExecutableTypeTable ***************/
DROP PROCEDURE IF EXISTS selectActivityExecutableTypeTable //
-- Name: selectActivityExecutableTypeTable
-- Parameters: ...
CREATE PROCEDURE selectActivityExecutableTypeTable(
                 activityId_param VARCHAR(14),
                 type_param VARCHAR(5)) COMMENT 'select activity_executable_type table'
BEGIN
SELECT executable_type_id
FROM activity_executable_type 
WHERE activityId=activityId_param
AND type=type_param;
END //

/************ Stored Procedure: selectExecutableTypeTable ***************/
DROP PROCEDURE IF EXISTS selectExecutableTypeTable //
-- Name: selectExecutableTypeTable
-- Parameters: ...
CREATE PROCEDURE selectExecutableTypeTable(
                 executable_type_id_param BIGINT) COMMENT 'select executable_type table'
BEGIN
SELECT path, failIfExitCodeNotEqualTo
FROM executable_type 
WHERE id=executable_type_id_param;
END //

/************ Stored Procedure: selectExecutableTypeArgumentTable ***************/
DROP PROCEDURE IF EXISTS selectExecutableTypeArgumentTable //
-- Name: selectExecutableTypeArgumentTable
-- Parameters: ...
CREATE PROCEDURE selectExecutableTypeArgumentTable(
                 executable_type_id_param BIGINT) COMMENT 'select executable_type_argument table'
BEGIN
SELECT argument
FROM executable_type_argument 
WHERE executable_type_id=executable_type_id_param;
END //

/************ Stored Procedure: selectNotificationTable ***************/
DROP PROCEDURE IF EXISTS selectNotificationTable //
-- Name: selectNotificationTable
-- Parameters: ...
CREATE PROCEDURE selectNotificationTable(
                 activityId_param VARCHAR(14)) COMMENT 'select notification table'
BEGIN
SELECT id, protocol, optional
FROM notification 
WHERE activityId=activityId_param;
END //

/************ Stored Procedure: selectRemoteLoggingTable ***************/
DROP PROCEDURE IF EXISTS selectRemoteLoggingTable //
-- Name: selectRemoteLoggingTable
-- Parameters: ...
CREATE PROCEDURE selectRemoteLoggingTable(
                 activityId_param VARCHAR(14)) COMMENT 'select remote_logging table'
BEGIN
SELECT service_type, url, optional
FROM remote_logging 
WHERE activityId=activityId_param;
END //

/************ Stored Procedure: selectInputFileTable ***************/
DROP PROCEDURE IF EXISTS selectInputFileTable //
-- Name: selectInputFileTable
-- Parameters: ...
CREATE PROCEDURE selectInputFileTable(
                 activityId_param VARCHAR(14)) COMMENT 'select input_file table'
BEGIN
SELECT id, name, is_executable, uri, delegation_id
FROM  input_file
WHERE activityId=activityId_param;
END //

/************ Stored Procedure: selectOptionTypeInputFileTable ***************/
DROP PROCEDURE IF EXISTS selectOptionTypeInputFileTable //
-- Name: selectOptionTypeInputFileTable
-- Parameters: ...
CREATE PROCEDURE selectOptionTypeInputFileTable(
                 input_file_id_param BIGINT) COMMENT 'select option_type_input_file table'
BEGIN
SELECT name, value
FROM  option_type_input_file
WHERE input_file_id=input_file_id_param;
END //

/************ Stored Procedure: selectOutputFileTable ***************/
DROP PROCEDURE IF EXISTS selectOutputFileTable //
-- Name: selectOutputFileTable
-- Parameters: ...
CREATE PROCEDURE selectOutputFileTable(
                 activityId_param VARCHAR(14)) COMMENT 'select output_file table'
BEGIN
SELECT id, name, uri, delegation_id, mandatory, creation_flag, use_if_failure, use_if_cancel, use_if_success
FROM  output_file
WHERE activityId=activityId_param;
END //

/************ Stored Procedure: selectOptionTypeOutputFileTable ***************/
DROP PROCEDURE IF EXISTS selectOptionTypeOutputFileTable //
-- Name: selectOptionTypeOutputFileTable
-- Parameters: ...
CREATE PROCEDURE selectOptionTypeOutputFileTable(
                 output_file_id_param BIGINT) COMMENT 'select option_type_output_file table'
BEGIN
SELECT name, value
FROM  option_type_output_file
WHERE output_file_id=output_file_id_param;
END //

/************ Stored Procedure: selectActivityUserId ***************/
DROP PROCEDURE IF EXISTS selectActivityUserId //
-- Name: selectActivityUserId
-- Parameters: ...
CREATE PROCEDURE selectActivityUserId(
                 activityId_param VARCHAR(14),
                 OUT userId_param TEXT) COMMENT 'select activityUserId'
BEGIN
SELECT userId INTO userId_param
FROM  activity
WHERE activityId=activityId_param;
END //

/*****************************************************************/
