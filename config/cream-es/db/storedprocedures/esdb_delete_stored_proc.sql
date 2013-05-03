
/************ Add DELETE Stored Procedure to Database ***************/

/************ Stored Procedure: deleteActivityTable ***************/
DROP PROCEDURE IF EXISTS deleteActivityTable //
-- Name: deleteActivityTable
-- Parameters: ...
CREATE PROCEDURE deleteActivityTable(
                 activityId_param VARCHAR(14),
                 userId_param TEXT) COMMENT 'delete activity table'
BEGIN
DELETE FROM activity 
WHERE activityId = activityId_param 
AND (userId = userId_param OR userId_param IS NULL);
END //

/************ Stored Procedure: deleteActivityPropertyTable ***************/
DROP PROCEDURE IF EXISTS deleteActivityPropertyTable //
-- Name: deleteActivityPropertyTable
-- Parameters: ...
CREATE PROCEDURE deleteActivityPropertyTable(
                 activityId_param VARCHAR(14)) COMMENT 'delete activity_property table'
BEGIN
DELETE FROM activity_property 
WHERE activityId = activityId_param; 
END //

/************ Stored Procedure: deleteActivityStatusAttributeTable ***************/
DROP PROCEDURE IF EXISTS deleteActivityStatusAttributeTable //
-- Name: deleteActivityStatusAttributeTable
-- Parameters: ...
CREATE PROCEDURE deleteActivityStatusAttributeTable(
                 activity_status_id_param BIGINT) COMMENT 'delete activity_status_attribute table'
BEGIN
DELETE FROM activity_status_attribute 
WHERE activity_status_id = activity_status_id_param; 
END //

/*****************************************************************/

