
/************ Add UPDATE Stored Procedure to Database ***************/

/************ Stored Procedure: updateActivityCommandTable ***************/
DROP PROCEDURE IF EXISTS updateActivityCommandTable //
-- Name: updateActivityCommandTable
-- Parameters: ...
CREATE PROCEDURE updateActivityCommandTable(
                 id_command_param BIGINT,
                 is_success_param BOOL) COMMENT 'update activity_command table'
BEGIN
UPDATE activity_command
SET is_success = is_success_param
WHERE id = id_command_param; 
END //

/************ Stored Procedure: updateActivityStatusTable ***************/
DROP PROCEDURE IF EXISTS updateActivityStatusTable //
-- Name: updateActivityStatusTable
-- Parameters: ...
CREATE PROCEDURE updateActivityStatusTable(
                 id_status_param BIGINT,
                 is_transient_param BOOL) COMMENT 'update activity_status table'
BEGIN
UPDATE activity_status
SET is_transient = is_transient_param
WHERE id = id_status_param; 
END //

/*****************************************************************/

