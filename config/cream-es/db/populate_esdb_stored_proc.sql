/************ Use: Database ***************/
USE esdb;

/************ Add Stored Procedure to Database ***************/

DELIMITER //

source /etc/glite-ce-cream-es/storedprocedures/esdb_insert_stored_proc.sql
source /etc/glite-ce-cream-es/storedprocedures/esdb_select_stored_proc.sql
source /etc/glite-ce-cream-es/storedprocedures/esdb_update_stored_proc.sql
source /etc/glite-ce-cream-es/storedprocedures/esdb_delete_stored_proc.sql

DELIMITER ;

/************ Insert values in db_info ***************/    
update db_info set stored_proc_version = '1.1';

commit;

