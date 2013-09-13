/*
 * Copyright (c) Members of the EGEE Collaboration. 2004. 
 * See http://www.eu-egee.org/partners/ for details on the copyright
 * holders.  
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 *
 *     http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 */

/*
 * 
 * Authors: Eric Frizziero <eric.frizziero@pd.infn.it> 
 *
 */

package org.glite.ce.cream.jobmanagement.db.table;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.log4j.Logger;
import org.glite.ce.creamapi.jobmanagement.command.JobCommand;
import org.glite.ce.creamapi.jobmanagement.db.table.JobCommandTableInterface;

public class JobCommandTable implements JobCommandTableInterface {
    private final static Logger logger = Logger.getLogger(JobCommandTable.class.getName());

    public final static String NAME_TABLE = "job_command";
    public final static String ID_FIELD = "id";
    public final static String DESCRIPTION_FIELD = "description";
    public final static String STATUS_TYPE_FIELD = "statusType";
    public final static String FAILURE_REASON_FIELD = "failureReason";
    public final static String JOB_ID_FIELD = "jobId";
    public final static String CMD_EXECUTOR_NAME_FIELD = "cmdExecutorName";
    public final static String USER_ID_FIELD = "userId";
    public final static String START_SCHEDULING_TIME_FIELD = "startSchedulingTime";
    public final static String START_PROCESSING_TIME_FIELD = "startProcessingTime";
    public final static String EXECUTION_COMPLETED_TIME_FIELD = "executionCompletedTime";
    public final static String CREATION_TIME_FIELD = "creationTime";
    public final static String TYPE_FIELD = "type";
    public final static String USERID_ADMINISTRATOR = "ADMINISTRATOR";

    private final static String JOB_COMMAND_SEQUENCE_NAME = "job_command_id_seq";
    private final static String GET_SEQUENCE_QUERY = "select nextVal('" + JOB_COMMAND_SEQUENCE_NAME + " ')";
    private final static String insertQuery = getInsertQuery();
    private final static String deleteQuery = getDeleteQuery();
    private final static String selectHistoryQuery = getSelectHistoryQuery();
    private final static String updateQuery = getUpdateQuery();

    // private final static String updateAllUnterminatedJobCommandQuery =
    // getUpdateAllUnterminatedJobCommandQuery();

    private static String getDeleteQuery() {
        StringBuffer queryInsertJobStatusTable = new StringBuffer();
        queryInsertJobStatusTable.append("delete from ");
        queryInsertJobStatusTable.append(JobCommandTable.NAME_TABLE);
        queryInsertJobStatusTable.append(" where ");
        queryInsertJobStatusTable.append(JobCommandTable.JOB_ID_FIELD + "=?");
        return queryInsertJobStatusTable.toString();
    }

    private static String getInsertQuery() {
        StringBuffer queryInsertJobCommandQuery = new StringBuffer();
        queryInsertJobCommandQuery.append("insert into ");
        queryInsertJobCommandQuery.append(JobCommandTable.NAME_TABLE);
        queryInsertJobCommandQuery.append(" ( ");
        queryInsertJobCommandQuery.append(JobCommandTable.DESCRIPTION_FIELD + ", ");
        queryInsertJobCommandQuery.append(JobCommandTable.STATUS_TYPE_FIELD + ", ");
        queryInsertJobCommandQuery.append(JobCommandTable.FAILURE_REASON_FIELD + ", ");
        queryInsertJobCommandQuery.append(JobCommandTable.JOB_ID_FIELD + ", ");
        queryInsertJobCommandQuery.append(JobCommandTable.CMD_EXECUTOR_NAME_FIELD + ", ");
        queryInsertJobCommandQuery.append(JobCommandTable.USER_ID_FIELD + ", ");
        queryInsertJobCommandQuery.append(JobCommandTable.START_SCHEDULING_TIME_FIELD + ", ");
        queryInsertJobCommandQuery.append(JobCommandTable.START_PROCESSING_TIME_FIELD + ", ");
        queryInsertJobCommandQuery.append(JobCommandTable.EXECUTION_COMPLETED_TIME_FIELD + ", ");
        queryInsertJobCommandQuery.append(JobCommandTable.CREATION_TIME_FIELD + ", ");
        queryInsertJobCommandQuery.append(JobCommandTable.TYPE_FIELD);
        queryInsertJobCommandQuery.append(" ) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        return queryInsertJobCommandQuery.toString();
    }

    private static String getSelectHistoryQuery() {
        StringBuffer selectLastJobCommandQuery = new StringBuffer();
        selectLastJobCommandQuery.append("select ");
        selectLastJobCommandQuery.append(DESCRIPTION_FIELD + " AS " + DESCRIPTION_FIELD + ",");
        selectLastJobCommandQuery.append(STATUS_TYPE_FIELD + " AS " + STATUS_TYPE_FIELD + ",");
        selectLastJobCommandQuery.append(FAILURE_REASON_FIELD + " AS " + FAILURE_REASON_FIELD + ",");
        selectLastJobCommandQuery.append(JOB_ID_FIELD + " AS " + JOB_ID_FIELD + ",");
        selectLastJobCommandQuery.append(CMD_EXECUTOR_NAME_FIELD + " AS " + CMD_EXECUTOR_NAME_FIELD + ",");
        selectLastJobCommandQuery.append(USER_ID_FIELD + " AS " + USER_ID_FIELD + ",");
        selectLastJobCommandQuery.append(START_SCHEDULING_TIME_FIELD + " AS " + START_SCHEDULING_TIME_FIELD + ",");
        selectLastJobCommandQuery.append(START_PROCESSING_TIME_FIELD + " AS " + START_PROCESSING_TIME_FIELD + ",");
        selectLastJobCommandQuery.append(EXECUTION_COMPLETED_TIME_FIELD + " AS " + EXECUTION_COMPLETED_TIME_FIELD + ",");
        selectLastJobCommandQuery.append(CREATION_TIME_FIELD + " AS " + CREATION_TIME_FIELD + ",");
        selectLastJobCommandQuery.append(TYPE_FIELD + " AS " + TYPE_FIELD + ",");
        selectLastJobCommandQuery.append(ID_FIELD + " AS " + ID_FIELD);
        selectLastJobCommandQuery.append(" from " + JobCommandTable.NAME_TABLE);
        selectLastJobCommandQuery.append(" where ");
        selectLastJobCommandQuery.append(JobCommandTable.JOB_ID_FIELD + "=?");
        selectLastJobCommandQuery.append(" ORDER BY " + JobCommandTable.ID_FIELD);
        return selectLastJobCommandQuery.toString();
    }

    private static String getSelectQuery(List<String> jobId, String userId, Calendar startDate, Calendar endDate) {
        StringBuffer selectJobCommandQuery = new StringBuffer();
        selectJobCommandQuery.append("select ");
        selectJobCommandQuery.append(JOB_ID_FIELD + " AS " + JOB_ID_FIELD);
        selectJobCommandQuery.append(" from " + JobCommandTable.NAME_TABLE);
        selectJobCommandQuery.append(" where ");
        selectJobCommandQuery.append(JobCommandTable.TYPE_FIELD + "=0");
        
        if (startDate != null) {
            selectJobCommandQuery.append(" and " + JobCommandTable.CREATION_TIME_FIELD + " >= ?");
        }
        
        if (endDate != null) {
            selectJobCommandQuery.append(" and " + JobCommandTable.CREATION_TIME_FIELD + " <= ?");
        }

        if (userId != null) {
            selectJobCommandQuery.append(" and " + JobCommandTable.USER_ID_FIELD + "=?");
        }

        if ((jobId != null) && (jobId.size() > 0)) {
            StringBuffer jobIdList = new StringBuffer();
            for (int i = 0; i < jobId.size(); i++) {
                jobIdList.append(", '" + jobId.get(i) + "'");
            }
            jobIdList.deleteCharAt(0);
            selectJobCommandQuery.append(" and " + JobCommandTable.JOB_ID_FIELD + " IN (" + jobIdList.toString() + ")");
        }
        return selectJobCommandQuery.toString();
    }

    /**
     * Update for max id and jobId.
     * 
     * @return
     */
    private static String getUpdateQuery() {
        StringBuffer updateQuery = new StringBuffer();
        updateQuery.append("update ");
        updateQuery.append(JobCommandTable.NAME_TABLE);
        updateQuery.append(" set " + JobCommandTable.DESCRIPTION_FIELD + "=?, ");
        updateQuery.append(JobCommandTable.STATUS_TYPE_FIELD + "=?, ");
        updateQuery.append(JobCommandTable.FAILURE_REASON_FIELD + "=?, ");
        updateQuery.append(JobCommandTable.CMD_EXECUTOR_NAME_FIELD + "=?, ");
        updateQuery.append(JobCommandTable.START_SCHEDULING_TIME_FIELD + "=?, ");
        updateQuery.append(JobCommandTable.START_PROCESSING_TIME_FIELD + "=?, ");
        updateQuery.append(JobCommandTable.EXECUTION_COMPLETED_TIME_FIELD + "=?, ");
        updateQuery.append(JobCommandTable.CREATION_TIME_FIELD + "=?, ");
        updateQuery.append(JobCommandTable.TYPE_FIELD + "=?");
        updateQuery.append(" where ");
        updateQuery.append(JobCommandTable.ID_FIELD + "=?");
        updateQuery.append(" and " + JobCommandTable.JOB_ID_FIELD + "=?");
        return updateQuery.toString();
    }

    public JobCommandTable() throws SQLException {
        logger.debug("Call JobCommandTable constructor");
    }

    private JobCommand buildJobCommand(ResultSet rs) throws SQLException {
        JobCommand jobCommand = null;
        jobCommand = new JobCommand(rs.getInt(TYPE_FIELD));
        jobCommand.setStatus(rs.getInt(STATUS_TYPE_FIELD));
        jobCommand.setDescription(rs.getString(DESCRIPTION_FIELD));
        jobCommand.setFailureReason(rs.getString(FAILURE_REASON_FIELD));
        jobCommand.setJobId(rs.getString(JOB_ID_FIELD));
        jobCommand.setCommandExecutorName(rs.getString(CMD_EXECUTOR_NAME_FIELD));
        jobCommand.setUserId(rs.getString(USER_ID_FIELD));

        Timestamp timeStampField = null;
        Calendar calendar = null;

        timeStampField = rs.getTimestamp(START_SCHEDULING_TIME_FIELD);
        if (timeStampField != null) {
            calendar = Calendar.getInstance();
            calendar.setTimeInMillis(timeStampField.getTime());
            jobCommand.setStartSchedulingTime(calendar);
        }
        
        timeStampField = rs.getTimestamp(START_PROCESSING_TIME_FIELD);
        if (timeStampField != null) {
            calendar = Calendar.getInstance();
            calendar.setTimeInMillis(timeStampField.getTime());
            jobCommand.setStartProcessingTime(calendar);
        }
        
        timeStampField = rs.getTimestamp(EXECUTION_COMPLETED_TIME_FIELD);
        if (timeStampField != null) {
            calendar = Calendar.getInstance();
            calendar.setTimeInMillis(timeStampField.getTime());
            jobCommand.setExecutionCompletedTime(calendar);
        }
        
        timeStampField = rs.getTimestamp(CREATION_TIME_FIELD);
        if (timeStampField != null) {
            calendar = Calendar.getInstance();
            calendar.setTimeInMillis(timeStampField.getTime());
            jobCommand.setCreationTime(calendar);
        }
        
        int id = rs.getInt(ID_FIELD);
        jobCommand.setId(id);

        return jobCommand;
    }

    private List<JobCommand> buildJobCommandDBList(ResultSet rs) throws SQLException {
        List<JobCommand> jobCommandList = new ArrayList<JobCommand>();
        while (rs.next()) {
            jobCommandList.add(this.buildJobCommand(rs));
        }
        return jobCommandList;
    }

    public int executeDelete(String jobId, Connection connection) throws SQLException {
        logger.debug("Begin executeDelete");
        // logger.debug("deleteQuery (JobCommandTable)= " + deleteQuery);
        PreparedStatement deletePreparedStatement = null;
        int rowCount = 0;
        try {
            deletePreparedStatement = connection.prepareStatement(deleteQuery);
            deletePreparedStatement = fillDeletePreparedStatement(jobId, deletePreparedStatement);
            // execute query, and return number of rows created
            rowCount = deletePreparedStatement.executeUpdate();
        } catch (SQLException sqle) {
            throw sqle;
        } finally {
            if (deletePreparedStatement != null) {
                try {
                    deletePreparedStatement.close();
                } catch (SQLException sqle1) {
                    logger.error(sqle1);
                }
            }
        }
        logger.debug("End executeDelete");
        return rowCount;
    }

    public int executeInsert(List<JobCommand> jobCommandList, Connection connection) throws SQLException {
        logger.debug("Begin executeInsert");
        // logger.debug("insertQuery (JobCommandTable)= " + insertQuery);
        PreparedStatement insertPreparedStatement = null;
        ResultSet rset = null;
        int rowCountTot = 0;
        try {
            insertPreparedStatement = connection.prepareStatement(insertQuery, PreparedStatement.RETURN_GENERATED_KEYS);

            int rowCount = 0;
            for (int index = 0; index < jobCommandList.size(); index++) {
                insertPreparedStatement = fillInsertPreparedStatement(jobCommandList.get(index), insertPreparedStatement);
                logger.debug("insertCommand query = " + insertPreparedStatement.toString());

                // execute query, and return number of rows created
                rowCount = insertPreparedStatement.executeUpdate();

                // Get autogenerated keys
                long commandId = -1;
                rset = insertPreparedStatement.getGeneratedKeys();

                if (rset.next()) {
                    commandId = rset.getLong(1);
                    jobCommandList.get(index).setId(commandId);
                } else {
                    throw new SQLException("Problem in retrieving autogenerated keys");
                }

                insertPreparedStatement.clearParameters();
                rowCountTot += rowCount;
            }
            logger.debug("Rows inserted: " + rowCountTot);
        } catch (SQLException sqle) {
            throw sqle;
        } finally {
            if (insertPreparedStatement != null) {
                try {
                    insertPreparedStatement.close();
                } catch (SQLException sqle1) {
                    logger.error(sqle1);
                }
            }
        }
        logger.debug("End executeInsert");
        return rowCountTot;
    }

    public List<JobCommand> executeSelectJobCommandHistory(String jobId, Connection connection) throws SQLException {
        return this.selectJobCommandHistory(jobId, connection);
    }

    public JobCommand executeSelectLastJobCommmand(String jobId, Connection connection) throws SQLException {
        logger.debug("Begin executeSelectLastJobCommmand");
        List<JobCommand> jobCommandList = null;
        JobCommand jobCommand = null;
        jobCommandList = this.executeSelectJobCommandHistory(jobId, connection);
        if (jobCommandList.size() > 0) {
            logger.debug("jobStatusList.size() = " + jobCommandList.size());
            jobCommand = jobCommandList.get(jobCommandList.size() - 1);
        }
        logger.debug("End executeSelectLastJobCommmand");
        return jobCommand;
    }

    public List<String> executeSelectToRetrieveJobIdByDate(List<String> jobId, String userId, Calendar startDate, Calendar endDate, Connection connection) throws SQLException {
        logger.debug("Begin executeSelectToRetrieveJobIdByDate");
        List<String> jobIdList = new ArrayList<String>();
        String selectQuery = getSelectQuery(jobId, userId, startDate, endDate);
                
        logger.debug("selectQuery (JobCommandTable)= " + selectQuery);
        PreparedStatement selectPreparedStatement = null;
        
        try {
            int index = 0;
            selectPreparedStatement = connection.prepareStatement(selectQuery);
            
            if (startDate != null) {
                selectPreparedStatement.setTimestamp(++index, new java.sql.Timestamp(startDate.getTimeInMillis()));
            }
            
            if (endDate != null) {
                selectPreparedStatement.setTimestamp(++index, new java.sql.Timestamp(endDate.getTimeInMillis()));
            }

            if (userId != null) {
                selectPreparedStatement.setString(++index, userId);
            }
            
            // execute query, and return number of rows created
            ResultSet rs = selectPreparedStatement.executeQuery();
            if ((rs != null)) {
                while (rs.next()) {
                    jobIdList.add(rs.getString(JobCommandTable.JOB_ID_FIELD));
                }
            }
        } catch (SQLException sqle) {
            throw sqle;
        } finally {
            if (selectPreparedStatement != null) {
                try {
                    selectPreparedStatement.close();
                } catch (SQLException sqle1) {
                    logger.error(sqle1);
                }
            }
        }
        logger.debug("End executeSelectToRetrieveJobIdByDate");
        return jobIdList;
    }

    public void executeUpdateCommandHistory(String jobId, List<JobCommand> jobCommandList, Connection connection) throws SQLException {
        logger.debug("Begin executeUpdateCommandHistory for jobid = " + jobId);
        List<JobCommand> jobCommandDBList = selectJobCommandHistory(jobId, connection);
        if (jobCommandList.size() != jobCommandDBList.size()) {
            String errorMsg = "Error updating CommandHistory for jobid = " + jobId + ". Num Commands in db = " + jobCommandDBList.size() + " Num Commands in updating = " + jobCommandList.size();
            logger.error(errorMsg);
            throw new SQLException(errorMsg);
        }
        // jobCommandDBList = updateJobCommandDBList(jobCommandList,
        // jobCommandDBList);
        for (int index = 0; index < jobCommandDBList.size(); index++) {
            updateJobCommand(jobCommandDBList.get(index), connection);
        }
        logger.debug("End executeUpdateCommandHistory for jobid = " + jobId);
    }

    public void executeUpdateJobCommand(JobCommand jobCommand, Connection connection) throws SQLException {
        this.updateJobCommand(jobCommand, connection);
    }

    private PreparedStatement fillDeletePreparedStatement(String jobId, PreparedStatement deletePreparedStatement) throws SQLException {
        deletePreparedStatement.setString(1, jobId);
        return deletePreparedStatement;
    }

    private PreparedStatement fillInsertPreparedStatement(JobCommand jobCommand, PreparedStatement insertPreparedStatement) throws SQLException {
        insertPreparedStatement.setString(1, jobCommand.getDescription());
        insertPreparedStatement.setInt(2, jobCommand.getStatus());
        insertPreparedStatement.setString(3, jobCommand.getFailureReason());
        insertPreparedStatement.setString(4, jobCommand.getJobId());
        insertPreparedStatement.setString(5, jobCommand.getCommandExecutorName());
        String userId = jobCommand.getUserId();
        
        if (userId == null) {
            userId = JobCommandTable.USERID_ADMINISTRATOR;
        }
        insertPreparedStatement.setString(6, userId);
        
        if (jobCommand.getStartSchedulingTime() != null) {
            insertPreparedStatement.setTimestamp(7, new java.sql.Timestamp(jobCommand.getStartSchedulingTime().getTimeInMillis()));
        } else {
            insertPreparedStatement.setTimestamp(7, null);
        }

        if (jobCommand.getStartProcessingTime() != null) {
            insertPreparedStatement.setTimestamp(8, new java.sql.Timestamp(jobCommand.getStartProcessingTime().getTimeInMillis()));
        } else {
            insertPreparedStatement.setTimestamp(8, null);
        }
        
        if (jobCommand.getExecutionCompletedTime() != null) {
            insertPreparedStatement.setTimestamp(9, new java.sql.Timestamp(jobCommand.getExecutionCompletedTime().getTimeInMillis()));
        } else {
            insertPreparedStatement.setTimestamp(9, null);
        }
        
        if (jobCommand.getCreationTime() != null) {
            insertPreparedStatement.setTimestamp(10, new java.sql.Timestamp(jobCommand.getCreationTime().getTimeInMillis()));
        } else {
            insertPreparedStatement.setTimestamp(10, null);
        }
        
        insertPreparedStatement.setInt(11, jobCommand.getType());
        return insertPreparedStatement;
    }

    private PreparedStatement fillSelectHistoryPreparedStatement(String jobId, PreparedStatement selectHistoryPreparedStatement) throws SQLException {
        selectHistoryPreparedStatement.setString(1, jobId);
        return selectHistoryPreparedStatement;
    }

    private PreparedStatement fillUpdatePreparedStatement(JobCommand jobCommand, PreparedStatement updatePreparedStatement) throws SQLException {
        updatePreparedStatement.setString(1, jobCommand.getDescription());
        updatePreparedStatement.setInt(2, jobCommand.getStatus());
        updatePreparedStatement.setString(3, jobCommand.getFailureReason());
        updatePreparedStatement.setString(4, jobCommand.getCommandExecutorName());

        if (jobCommand.getStartSchedulingTime() != null) {
            updatePreparedStatement.setTimestamp(5, new java.sql.Timestamp(jobCommand.getStartSchedulingTime().getTimeInMillis()));
        } else {
            updatePreparedStatement.setTimestamp(5, null);
        }
        
        if (jobCommand.getStartProcessingTime() != null) {
            updatePreparedStatement.setTimestamp(6, new java.sql.Timestamp(jobCommand.getStartProcessingTime().getTimeInMillis()));
        } else {
            updatePreparedStatement.setTimestamp(6, null);
        }
        
        if (jobCommand.getExecutionCompletedTime() != null) {
            updatePreparedStatement.setTimestamp(7, new java.sql.Timestamp(jobCommand.getExecutionCompletedTime().getTimeInMillis()));
        } else {
            updatePreparedStatement.setTimestamp(7, null);
        }
        
        if (jobCommand.getCreationTime() != null) {
            updatePreparedStatement.setTimestamp(8, new java.sql.Timestamp(jobCommand.getCreationTime().getTimeInMillis()));
        } else {
            updatePreparedStatement.setTimestamp(8, null);
        }
        
        updatePreparedStatement.setInt(9, jobCommand.getType());
        updatePreparedStatement.setLong(10, jobCommand.getId());
        updatePreparedStatement.setString(11, jobCommand.getJobId());
        return updatePreparedStatement;
    }

    // private static String getUpdateAllUnterminatedJobCommandQuery() {
    // StringBuffer updateQuery = new StringBuffer();
    // updateQuery.append("update ");
    // updateQuery.append(JobCommandTable.NAME_TABLE);
    // updateQuery.append(" set " + JobCommandTable.STATUS_TYPE_FIELD + " = ? "
    // + ", ");
    // updateQuery.append(JobCommandTable.FAILURE_REASON_FIELD + " = ? " + ",
    // ");
    // updateQuery.append(JobCommandTable.EXECUTION_COMPLETED_TIME_FIELD + " = ?
    // " + ", ");
    // updateQuery.append("where ");
    // updateQuery.append(JobCommandTable.STATUS_TYPE_FIELD + " IN ?");
    // return updateQuery.toString();
    // }

    private long getIDSequence(Connection connection) throws SQLException {
        long id = -1;
        PreparedStatement sequencePreparedStatement = null;
        try {
            sequencePreparedStatement = connection.prepareStatement(GET_SEQUENCE_QUERY);
            ResultSet rs = sequencePreparedStatement.executeQuery();
            if ((rs != null) && rs.next()) {
                id = rs.getLong(1);
            } else {
                throw new SQLException("Impossible to retrieve sequence id for " + JobCommandTable.NAME_TABLE);
            }
        } catch (SQLException sqle) {
            throw sqle;
        } finally {
            if (sequencePreparedStatement != null) {
                try {
                    sequencePreparedStatement.close();
                } catch (SQLException sqle1) {
                    logger.error(sqle1);
                }
            }
        }
        return id;
    }

    private List<JobCommand> selectJobCommandHistory(String jobId, Connection connection) throws SQLException {
        logger.debug("Begin selectJobCommandHistory");
        List<JobCommand> jobCommandList = null;
        // logger.debug("selectHistoryQuery (JobCommandTable)= " +
        // selectHistoryQuery);
        PreparedStatement selectHistoryPreparedStatement = null;
        try {
            selectHistoryPreparedStatement = connection.prepareStatement(selectHistoryQuery);
            selectHistoryPreparedStatement = fillSelectHistoryPreparedStatement(jobId, selectHistoryPreparedStatement);
            // execute query, and return number of rows created
            ResultSet rs = selectHistoryPreparedStatement.executeQuery();
            if ((rs != null)) {
                jobCommandList = this.buildJobCommandDBList(rs);
            }
        } catch (SQLException sqle) {
            throw sqle;
        } finally {
            if (selectHistoryPreparedStatement != null) {
                try {
                    selectHistoryPreparedStatement.close();
                } catch (SQLException sqle1) {
                    logger.error(sqle1);
                }
            }
        }
        logger.debug("End selectJobCommandHistory");
        return jobCommandList;
    }

    private void updateJobCommand(JobCommand jobCommand, Connection connection) throws SQLException {
        logger.debug("Begin updateJobCommand");
        // logger.debug("updateQuery (JobCommandTable)= " + updateQuery);
        PreparedStatement updatePreparedStatement = null;
        try {
            updatePreparedStatement = connection.prepareStatement(updateQuery);
            updatePreparedStatement = fillUpdatePreparedStatement(jobCommand, updatePreparedStatement);
            updatePreparedStatement.executeUpdate();
        } catch (SQLException sqle) {
            throw sqle;
        } finally {
            if (updatePreparedStatement != null) {
                try {
                    updatePreparedStatement.close();
                } catch (SQLException sqle1) {
                    logger.error(sqle1);
                }
            }
        }
        logger.debug("End updateJobCommand");
    }

    public int executeUpdateAllUnterminatedJobCommandQuery(int newStatus, int[] oldStatus, String failureReason, Calendar executionCompletedTime, Connection connection) throws SQLException {
        logger.debug("Begin executeUpdateAllUnterminatedJobCommandQuery");
        StringBuffer updateQuery = new StringBuffer();
        updateQuery.append("update ");
        updateQuery.append(JobCommandTable.NAME_TABLE);
        updateQuery.append(" set " + JobCommandTable.STATUS_TYPE_FIELD + "=?");

        if (failureReason != null) {
            updateQuery.append(", " + JobCommandTable.FAILURE_REASON_FIELD + "=?");
        }

        if (executionCompletedTime != null) {
            updateQuery.append(", " + JobCommandTable.EXECUTION_COMPLETED_TIME_FIELD + "=?");
        }

        updateQuery.append(" where ");
        updateQuery.append(JobCommandTable.STATUS_TYPE_FIELD + " IN (");

        for (int i = 0; i < oldStatus.length; i++) {
            updateQuery.append("'" + oldStatus[i] + "'");
            if (i < oldStatus.length - 1) {
                updateQuery.append(", ");
            }
        }
        updateQuery.append(")");

        PreparedStatement updatePreparedStatement = null;
        int result;
        try {
            int index = 0;
            
            updatePreparedStatement = connection.prepareStatement(updateQuery.toString());
            updatePreparedStatement.setInt(++index, newStatus);
            
            if (failureReason != null) {
                updatePreparedStatement.setString(++index, failureReason);
            }

            if (executionCompletedTime != null) {
                updatePreparedStatement.setTimestamp(++index, new Timestamp(executionCompletedTime.getTimeInMillis()));
            }
            
            result = updatePreparedStatement.executeUpdate();
        } catch (SQLException sqle) {
            throw sqle;
        } finally {
            if (updatePreparedStatement != null) {
                try {
                    updatePreparedStatement.close();
                } catch (SQLException sqle1) {
                    logger.error(sqle1);
                }
            }
        }
        logger.debug("End executeUpdateAllUnterminatedJobCommandQuery");
        return result;
    }
}
