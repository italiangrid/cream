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
import org.glite.ce.creamapi.eventmanagement.Event;
import org.glite.ce.creamapi.eventmanagement.Property;
import org.glite.ce.creamapi.jobmanagement.JobStatus;
import org.glite.ce.creamapi.jobmanagement.cmdexecutor.JobStatusEventManager;
import org.glite.ce.creamapi.jobmanagement.db.table.JobStatusTableInterface;

public class JobStatusTable implements JobStatusTableInterface {

    public final static String NAME_TABLE = "job_status";
    public final static String TYPE_FIELD = "type";
    public final static String EXIT_CODE_FIELD = "exitCode";
    public final static String FAILURE_REASON_FIELD = "failureReason";
    public final static String DESCRIPTION_FIELD = "description";
    public final static String TIMESTAMP_FIELD = "time_stamp";
    public final static String JOB_ID_FIELD = "jobId";
    public final static String ID_FIELD = "id";

    private final static String JOB_STATUS_SEQUENCE_NAME = "job_status_id_seq";
    private final static String GET_SEQUENCE_QUERY = "select nextVal('" + JOB_STATUS_SEQUENCE_NAME + " ')";

    private final static String insertQuery = getInsertQuery();
    private final static String deleteQuery = getDeleteQuery();
    private final static String selectHistoryQuery = getSelectHistoryQuery();
    private final static String updateQuery = getUpdateQuery();

    private final static Logger logger = Logger.getLogger(JobStatusTable.class.getName());

    public JobStatusTable() throws SQLException {
        logger.debug("Call JobStatusTable constructor");
    }

    public int executeInsert(List<JobStatus> jobStatusList, Connection connection) throws SQLException {
        logger.debug("Begin executeInsert");
        // logger.debug("insertQuery (JobStatusTable)= " + insertQuery);
        PreparedStatement insertPreparedStatement = null;
        ResultSet rset = null;
        int rowCountTot = 0;

        try {
            insertPreparedStatement = connection.prepareStatement(insertQuery, PreparedStatement.RETURN_GENERATED_KEYS);

            int rowCount = 0;
            for (int index = 0; index < jobStatusList.size(); index++) {
                insertPreparedStatement = fillInsertPreparedStatement(jobStatusList.get(index), insertPreparedStatement);
                // execute query, and return number of rows created
                rowCount = insertPreparedStatement.executeUpdate();

                // Get autogenerated keys
                long statusId = -1;
                rset = insertPreparedStatement.getGeneratedKeys();

                if (rset.next()) {
                    statusId = rset.getLong(1);
                    jobStatusList.get(index).setId(statusId);
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

    private PreparedStatement fillInsertPreparedStatement(JobStatus jobStatus, PreparedStatement insertPreparedStatement) throws SQLException {
        insertPreparedStatement.setInt(1, jobStatus.getType());
        insertPreparedStatement.setString(2, jobStatus.getExitCode());
        insertPreparedStatement.setString(3, jobStatus.getFailureReason());
        insertPreparedStatement.setString(4, jobStatus.getDescription());
        insertPreparedStatement.setTimestamp(5, new java.sql.Timestamp(jobStatus.getTimestamp().getTimeInMillis()));
        insertPreparedStatement.setString(6, jobStatus.getJobId());
        return insertPreparedStatement;
    }

    public int executeDelete(String jobId, Connection connection) throws SQLException {
        logger.debug("Begin executeDelete");
        // logger.debug("deleteQuery (JobStatusTable)= " + deleteQuery);
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

    private PreparedStatement fillDeletePreparedStatement(String jobId, PreparedStatement deletePreparedStatement) throws SQLException {
        deletePreparedStatement.setString(1, jobId);
        return deletePreparedStatement;
    }

    public JobStatus executeSelectLastJobStatus(String jobId, Connection connection) throws SQLException {
        logger.debug("Begin executeSelectLastJobStatus");
        List<JobStatus> jobStatusList = null;
        JobStatus jobStatus = null;
        jobStatusList = this.executeSelectJobStatusHistory(jobId, connection);
        if (jobStatusList.size() > 0) {
            logger.debug("jobStatusList.size() = " + jobStatusList.size());
            jobStatus = jobStatusList.get(jobStatusList.size() - 1);
        }
        logger.debug("End executeSelectLastJobStatus");
        return jobStatus;
    }

    public List<JobStatus> executeSelectJobStatusHistory(String jobId, Connection connection) throws SQLException {
        return this.selectJobStatusHistory(jobId, connection);
    }

    private List<JobStatus> selectJobStatusHistory(String jobId, Connection connection) throws SQLException {
        logger.debug("Begin selectJobStatusHistory");
        List<JobStatus> jobStatusDBList = null;
        // logger.debug("selectHistoryQuery (JobStatusTable)= " +
        // selectHistoryQuery);
        PreparedStatement selectHistoryPreparedStatement = null;
        try {
            selectHistoryPreparedStatement = connection.prepareStatement(selectHistoryQuery);
            selectHistoryPreparedStatement = fillSelectHistoryPreparedStatement(jobId, selectHistoryPreparedStatement);
            // execute query, and return number of rows created
            ResultSet rs = selectHistoryPreparedStatement.executeQuery();
            if ((rs != null)) {
                jobStatusDBList = this.buildJobStatusDBList(rs);
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
        logger.debug("End selectJobStatusHistory");
        return jobStatusDBList;
    }

    public void executeUpdateJobStatus(JobStatus jobStatus, Connection connection) throws SQLException {
        logger.debug("Begin executeUpdateJobStatus");
        // logger.debug("updateQuery (JobStatusTable)= " + updateQuery);
        PreparedStatement updatePreparedStatement = null;
        try {
            updatePreparedStatement = connection.prepareStatement(updateQuery);
            updatePreparedStatement = fillUpdatePreparedStatement(jobStatus, updatePreparedStatement);
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
        logger.debug("End executeUpdateJobStatus");
    }

    private PreparedStatement fillUpdatePreparedStatement(JobStatus jobStatus, PreparedStatement updatePreparedStatement) throws SQLException {
        updatePreparedStatement.setInt(1, jobStatus.getType());
        updatePreparedStatement.setString(2, jobStatus.getExitCode());
        updatePreparedStatement.setString(3, jobStatus.getFailureReason());
        updatePreparedStatement.setString(4, jobStatus.getDescription());
        if (jobStatus.getTimestamp() != null) {
            updatePreparedStatement.setTimestamp(5, new java.sql.Timestamp(jobStatus.getTimestamp().getTimeInMillis()));
        } else {
            updatePreparedStatement.setTimestamp(5, null);
        }
        updatePreparedStatement.setLong(6, jobStatus.getId());
        updatePreparedStatement.setString(7, jobStatus.getJobId());
        return updatePreparedStatement;
    }

    private PreparedStatement fillSelectHistoryPreparedStatement(String jobId, PreparedStatement selectHistoryPreparedStatement) throws SQLException {
        selectHistoryPreparedStatement.setString(1, jobId);
        return selectHistoryPreparedStatement;
    }

    private long getIDSequence(Connection connection) throws SQLException {
        long id = -1;
        PreparedStatement sequencePreparedStatement = null;
        try {
            sequencePreparedStatement = connection.prepareStatement(GET_SEQUENCE_QUERY);
            ResultSet rs = sequencePreparedStatement.executeQuery();
            if ((rs != null) && rs.next()) {
                id = rs.getLong(1);
            } else {
                throw new SQLException("Impossible retrieve sequence id for " + JobStatusTable.NAME_TABLE);
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

    private List<JobStatus> buildJobStatusDBList(ResultSet rs) throws SQLException {
        List<JobStatus> jobStatusDBList = new ArrayList<JobStatus>();
        while (rs.next()) {
            jobStatusDBList.add(this.buildJobStatusDB(rs));
        }
        return jobStatusDBList;
    }

    private JobStatus buildJobStatusDB(ResultSet rs) throws SQLException {
        JobStatus jobStatus = null;
        jobStatus = new JobStatus(rs.getInt(TYPE_FIELD));
        jobStatus.setExitCode(rs.getString(EXIT_CODE_FIELD));
        jobStatus.setFailureReason(rs.getString(FAILURE_REASON_FIELD));
        jobStatus.setDescription(rs.getString(DESCRIPTION_FIELD));
        Timestamp timeStampField = rs.getTimestamp(TIMESTAMP_FIELD);
        Calendar calendar = null;
        calendar = Calendar.getInstance();
        calendar.setTimeInMillis(timeStampField.getTime());
        jobStatus.setTimestamp(calendar);
        jobStatus.setJobId(rs.getString(JOB_ID_FIELD));
        int id = rs.getInt(ID_FIELD);
        jobStatus.setId(id);
        return jobStatus;
    }

    private static String getInsertQuery() {
        StringBuffer queryInsertJobStatusTable = new StringBuffer();
        queryInsertJobStatusTable.append("insert into ");
        queryInsertJobStatusTable.append(JobStatusTable.NAME_TABLE);
        queryInsertJobStatusTable.append(" (");
        queryInsertJobStatusTable.append(JobStatusTable.TYPE_FIELD + ", ");
        queryInsertJobStatusTable.append(JobStatusTable.EXIT_CODE_FIELD + ", ");
        queryInsertJobStatusTable.append(JobStatusTable.FAILURE_REASON_FIELD + ", ");
        queryInsertJobStatusTable.append(JobStatusTable.DESCRIPTION_FIELD + ", ");
        queryInsertJobStatusTable.append(JobStatusTable.TIMESTAMP_FIELD + ", ");
        queryInsertJobStatusTable.append(JobStatusTable.JOB_ID_FIELD);
        queryInsertJobStatusTable.append(") values(?, ?, ?, ?, ?, ?)");
        return queryInsertJobStatusTable.toString();
    }

    private static String getSelectHistoryQuery() {
        StringBuffer selectLastJobStatusQuery = new StringBuffer();
        selectLastJobStatusQuery.append("select ");
        selectLastJobStatusQuery.append(TYPE_FIELD + " AS " + TYPE_FIELD + ", ");
        selectLastJobStatusQuery.append(EXIT_CODE_FIELD + " AS " + EXIT_CODE_FIELD + ", ");
        selectLastJobStatusQuery.append(FAILURE_REASON_FIELD + " AS " + FAILURE_REASON_FIELD + ", ");
        selectLastJobStatusQuery.append(DESCRIPTION_FIELD + " AS " + DESCRIPTION_FIELD + ", ");
        selectLastJobStatusQuery.append(TIMESTAMP_FIELD + " AS " + TIMESTAMP_FIELD + ", ");
        selectLastJobStatusQuery.append(JOB_ID_FIELD + " AS " + JOB_ID_FIELD + ", ");
        selectLastJobStatusQuery.append(ID_FIELD + " AS " + ID_FIELD);
        selectLastJobStatusQuery.append(" from " + JobStatusTable.NAME_TABLE);
        selectLastJobStatusQuery.append(" where ");
        selectLastJobStatusQuery.append(JobStatusTable.JOB_ID_FIELD + "=? ORDER BY ");
        selectLastJobStatusQuery.append(JobStatusTable.ID_FIELD);
        return selectLastJobStatusQuery.toString();
    }

    private static String getSelectToRetrieveJobStatusQuery(String fromJobStatusId, String toJobStatusId, Calendar fromDate, Calendar toDate, int maxEvents, String iceId, String userId) {
        StringBuffer selectToRetrieveJobStatusQuery = new StringBuffer();
        selectToRetrieveJobStatusQuery.append("select ");
        selectToRetrieveJobStatusQuery.append(JobStatusTable.NAME_TABLE + "." + JobStatusTable.TYPE_FIELD + " AS " + TYPE_FIELD + ", ");
        selectToRetrieveJobStatusQuery.append(JobStatusTable.NAME_TABLE + "." + JobStatusTable.EXIT_CODE_FIELD + " AS " + EXIT_CODE_FIELD + ", ");
        selectToRetrieveJobStatusQuery.append(JobStatusTable.NAME_TABLE + "." + JobStatusTable.FAILURE_REASON_FIELD + " AS " + FAILURE_REASON_FIELD + ", ");
        selectToRetrieveJobStatusQuery.append(JobStatusTable.NAME_TABLE + "." + JobStatusTable.DESCRIPTION_FIELD + " AS " + DESCRIPTION_FIELD + ", ");
        selectToRetrieveJobStatusQuery.append(JobStatusTable.NAME_TABLE + "." + JobStatusTable.TIMESTAMP_FIELD + " AS " + TIMESTAMP_FIELD + ", ");
        selectToRetrieveJobStatusQuery.append(JobStatusTable.NAME_TABLE + "." + JobStatusTable.JOB_ID_FIELD + " AS " + JOB_ID_FIELD + ", ");
        selectToRetrieveJobStatusQuery.append(JobStatusTable.NAME_TABLE + "." + JobStatusTable.ID_FIELD + " AS " + ID_FIELD);
        selectToRetrieveJobStatusQuery.append(" from " + JobStatusTable.NAME_TABLE);
        selectToRetrieveJobStatusQuery.append(", " + JobTable.NAME_TABLE);
        selectToRetrieveJobStatusQuery.append(" where " + JobStatusTable.NAME_TABLE + "." + JobStatusTable.JOB_ID_FIELD + " = " + JobTable.NAME_TABLE + "." + JobTable.ID_FIELD);

        if (!isEmptyField(userId)) {
            selectToRetrieveJobStatusQuery.append(" and " + JobTable.NAME_TABLE + "." + JobTable.USER_ID_FIELD + "=?");
        }

        if (!isEmptyField(iceId)) {
            selectToRetrieveJobStatusQuery.append(" and " + JobTable.NAME_TABLE + "." + JobTable.ICE_ID_FIELD + "=?");
        }

        if (!isEmptyField(fromJobStatusId)) {
            selectToRetrieveJobStatusQuery.append(" and " + JobStatusTable.NAME_TABLE + "." + JobStatusTable.ID_FIELD + " >= ?");
        }
        
        if (!isEmptyField(toJobStatusId)) {
            selectToRetrieveJobStatusQuery.append(" and " + JobStatusTable.NAME_TABLE + "." + JobStatusTable.ID_FIELD + " <= ?");
        }

        if (fromDate != null) {
            selectToRetrieveJobStatusQuery.append(" and " + JobStatusTable.TIMESTAMP_FIELD + " >= ?");
        }
        
        if (toDate != null) {
            selectToRetrieveJobStatusQuery.append(" and " + JobStatusTable.TIMESTAMP_FIELD + " <= ?");
        }
        
        selectToRetrieveJobStatusQuery.append(" ORDER BY " + JobStatusTable.NAME_TABLE + "." + JobStatusTable.ID_FIELD);
        selectToRetrieveJobStatusQuery.append(" limit " + maxEvents);
        logger.debug("selectToRetrieveJobStatusQuery = " + selectToRetrieveJobStatusQuery.toString());
        return selectToRetrieveJobStatusQuery.toString();
    }

    private static String getSelectToRetrieveJobStatusAsEventQuery(String fromJobStatusId, String toJobStatusId, Calendar fromDate, Calendar toDate, int[] jobStatusType, int maxEvents, String iceId, String userId) {
        StringBuffer selectToRetrieveJobStatusTableJobTableAsEventQuery = new StringBuffer();
        selectToRetrieveJobStatusTableJobTableAsEventQuery.append("select ");
        selectToRetrieveJobStatusTableJobTableAsEventQuery.append(JobStatusTable.NAME_TABLE + "." + JobStatusTable.TYPE_FIELD + " AS " + TYPE_FIELD + ",");
        selectToRetrieveJobStatusTableJobTableAsEventQuery.append(JobStatusTable.NAME_TABLE + "." + JobStatusTable.EXIT_CODE_FIELD + " AS " + EXIT_CODE_FIELD + ",");
        selectToRetrieveJobStatusTableJobTableAsEventQuery.append(JobStatusTable.NAME_TABLE + "." + JobStatusTable.FAILURE_REASON_FIELD + " AS " + FAILURE_REASON_FIELD + ",");
        selectToRetrieveJobStatusTableJobTableAsEventQuery.append(JobStatusTable.NAME_TABLE + "." + JobStatusTable.DESCRIPTION_FIELD + " AS " + DESCRIPTION_FIELD + ",");
        selectToRetrieveJobStatusTableJobTableAsEventQuery.append(JobStatusTable.NAME_TABLE + "." + JobStatusTable.TIMESTAMP_FIELD + " AS " + TIMESTAMP_FIELD + ",");
        selectToRetrieveJobStatusTableJobTableAsEventQuery.append(JobStatusTable.NAME_TABLE + "." + JobStatusTable.JOB_ID_FIELD + " AS " + JOB_ID_FIELD + ",");
        selectToRetrieveJobStatusTableJobTableAsEventQuery.append(JobStatusTable.NAME_TABLE + "." + JobStatusTable.ID_FIELD + " AS " + ID_FIELD + ",");
        selectToRetrieveJobStatusTableJobTableAsEventQuery.append(JobTable.NAME_TABLE + "." + JobTable.GRID_JOB_ID_FIELD + " AS " + JobTable.GRID_JOB_ID_FIELD + ",");
        selectToRetrieveJobStatusTableJobTableAsEventQuery.append(JobTable.NAME_TABLE + "." + JobTable.WORKER_NODE_FIELD + " AS " + JobTable.WORKER_NODE_FIELD);
        selectToRetrieveJobStatusTableJobTableAsEventQuery.append(" from " + JobStatusTable.NAME_TABLE + ", " + JobTable.NAME_TABLE);
        selectToRetrieveJobStatusTableJobTableAsEventQuery.append(" where " + JobStatusTable.NAME_TABLE + "." + JobStatusTable.JOB_ID_FIELD + " = " + JobTable.NAME_TABLE + "." + JobTable.ID_FIELD);

        if ((jobStatusType != null) && (jobStatusType.length > 0)) {
            StringBuffer jobStatusTypeList = new StringBuffer();

            for (int i = 0; i < jobStatusType.length; i++) {
                jobStatusTypeList.append(", '" + jobStatusType[i] + "'");
            }

            jobStatusTypeList.deleteCharAt(0);
            selectToRetrieveJobStatusTableJobTableAsEventQuery.append(" and " + JobStatusTable.NAME_TABLE + "." + JobStatusTable.TYPE_FIELD + " IN (" + jobStatusTypeList.toString() + ")");
        }

        if (!isEmptyField(userId)) {
            selectToRetrieveJobStatusTableJobTableAsEventQuery.append(" and " + JobTable.NAME_TABLE + "." + JobTable.USER_ID_FIELD + "=?");
        }

        if (!isEmptyField(iceId)) {
            selectToRetrieveJobStatusTableJobTableAsEventQuery.append(" and " + JobTable.NAME_TABLE + "." + JobTable.ICE_ID_FIELD + "=?");
        }

        if (!isEmptyField(fromJobStatusId)) {
            selectToRetrieveJobStatusTableJobTableAsEventQuery.append(" and " + JobStatusTable.NAME_TABLE + "." + JobStatusTable.ID_FIELD + " >= ?");
        }

        if (!isEmptyField(toJobStatusId)) {
            selectToRetrieveJobStatusTableJobTableAsEventQuery.append(" and " + JobStatusTable.NAME_TABLE + "." + JobStatusTable.ID_FIELD + " <= ?");
        }

        if (fromDate != null) {
            selectToRetrieveJobStatusTableJobTableAsEventQuery.append(" and " + JobStatusTable.TIMESTAMP_FIELD + " >= ?");
        }

        if (toDate != null) {
            selectToRetrieveJobStatusTableJobTableAsEventQuery.append(" and " + JobStatusTable.TIMESTAMP_FIELD + " <= ?");
        }

        selectToRetrieveJobStatusTableJobTableAsEventQuery.append(" ORDER BY " + JobStatusTable.NAME_TABLE + "." + JobStatusTable.ID_FIELD);
        selectToRetrieveJobStatusTableJobTableAsEventQuery.append(" limit " + maxEvents);

        logger.debug("selectToRetrieveJobStatusQuery = " + selectToRetrieveJobStatusTableJobTableAsEventQuery.toString());

        return selectToRetrieveJobStatusTableJobTableAsEventQuery.toString();
    }

    /**
     * Update for max id and jobId.
     * 
     * @return
     */

    private static String getUpdateQuery() {
        StringBuffer queryUpdateJobStatusTable = new StringBuffer();
        queryUpdateJobStatusTable.append("update ");
        queryUpdateJobStatusTable.append(JobStatusTable.NAME_TABLE);
        queryUpdateJobStatusTable.append(" set ");
        queryUpdateJobStatusTable.append(JobStatusTable.TYPE_FIELD + " = ?, ");
        queryUpdateJobStatusTable.append(JobStatusTable.EXIT_CODE_FIELD + " = ?, ");
        queryUpdateJobStatusTable.append(JobStatusTable.FAILURE_REASON_FIELD + " = ?, ");
        queryUpdateJobStatusTable.append(JobStatusTable.DESCRIPTION_FIELD + " = ?, ");
        queryUpdateJobStatusTable.append(JobStatusTable.TIMESTAMP_FIELD + " = ? ");
        queryUpdateJobStatusTable.append(" where ");
        queryUpdateJobStatusTable.append(JobStatusTable.ID_FIELD + " = ? ");
        queryUpdateJobStatusTable.append(" and " + JobStatusTable.JOB_ID_FIELD + " = ?");
        return queryUpdateJobStatusTable.toString();
    }

    private static String getDeleteQuery() {
        StringBuffer queryInsertJobStatusTable = new StringBuffer();
        queryInsertJobStatusTable.append("delete from ");
        queryInsertJobStatusTable.append(JobStatusTable.NAME_TABLE);
        queryInsertJobStatusTable.append(" where ");
        queryInsertJobStatusTable.append(JobStatusTable.JOB_ID_FIELD + " = ? ");
        return queryInsertJobStatusTable.toString();
    }

    public void executeUpdateStatusHistory(String jobId, List<JobStatus> jobStatusList, Connection connection) throws SQLException {
        logger.debug("Begin executeUpdateStatusHistory for jobid = " + jobId);
        List<JobStatus> jobStatusDBList = selectJobStatusHistory(jobId, connection);
        if (jobStatusList.size() != jobStatusDBList.size()) {
            String errorMsg = "Error updating StatusHistory for jobid = " + jobId + ". Num status in db = " + jobStatusDBList.size() + " Num status in updating = " + jobStatusList.size();
            logger.error(errorMsg);
            throw new SQLException(errorMsg);
        }
        for (int index = 0; index < jobStatusDBList.size(); index++) {
            executeUpdateJobStatus(jobStatusDBList.get(index), connection);
        }
        logger.debug("End executeUpdateStatusHistory for jobid = " + jobId);
    }

    public List<JobStatus> executeSelectToRetrieveJobStatus(String fromJobStatusId, String toJobStatusId, Calendar fromDate, Calendar toDate, int maxElements, String iceId, String userId, Connection connection) throws SQLException {
        logger.debug("Begin executeSelectToRetrieveJobStatus");
        List<JobStatus> jobStatusDBList = null;
        PreparedStatement selectToRetrieveJobStatusPreparedStatement = null;
        try {
            int index = 0;
            
            selectToRetrieveJobStatusPreparedStatement = connection.prepareStatement(getSelectToRetrieveJobStatusQuery(fromJobStatusId, toJobStatusId, fromDate, toDate, maxElements, iceId, userId));

            if (!isEmptyField(userId)) {
                selectToRetrieveJobStatusPreparedStatement.setString(++index, userId);
            }

            if (!isEmptyField(iceId)) {
                selectToRetrieveJobStatusPreparedStatement.setString(++index, iceId);
            }

            if (!isEmptyField(fromJobStatusId)) {
                selectToRetrieveJobStatusPreparedStatement.setString(++index, fromJobStatusId);
            }
            
            if (!isEmptyField(toJobStatusId)) {
                selectToRetrieveJobStatusPreparedStatement.setString(++index, toJobStatusId);
            }

            if (fromDate != null) {
                selectToRetrieveJobStatusPreparedStatement.setTimestamp(++index, new java.sql.Timestamp(fromDate.getTimeInMillis()));
            }
            
            if (toDate != null) {
                selectToRetrieveJobStatusPreparedStatement.setTimestamp(++index, new java.sql.Timestamp(toDate.getTimeInMillis()));
            }
                        
            // execute query, and return number of rows created
            ResultSet rs = selectToRetrieveJobStatusPreparedStatement.executeQuery();
            if ((rs != null)) {
                jobStatusDBList = this.buildJobStatusDBList(rs);
            }
        } catch (SQLException sqle) {
            throw sqle;
        } finally {
            if (selectToRetrieveJobStatusPreparedStatement != null) {
                try {
                    selectToRetrieveJobStatusPreparedStatement.close();
                } catch (SQLException sqle1) {
                    logger.error(sqle1);
                }
            }
        }
        logger.debug("End executeSelectToRetrieveJobStatus");
        return jobStatusDBList;
    }

    public List<Event> executeSelectToRetrieveJobStatusAsEvent(String fromJobStatusId, String toJobStatusId, Calendar fromDate, Calendar toDate, int[] jobStatusType, int maxElements, String iceId, String userId, Connection connection) throws SQLException {
        logger.debug("Begin executeSelectToRetrieveJobStatusAsEvent");
        List<Event> eventList = null;
        PreparedStatement selectToRetrieveJobStatusAsEventPreparedStatement = null;
        try {
            int index = 0;
            
            selectToRetrieveJobStatusAsEventPreparedStatement = connection.prepareStatement(getSelectToRetrieveJobStatusAsEventQuery(fromJobStatusId, toJobStatusId, fromDate, toDate, jobStatusType, maxElements, iceId, userId));

            if (!isEmptyField(userId)) {
                selectToRetrieveJobStatusAsEventPreparedStatement.setString(++index, userId);
            }

            if (!isEmptyField(iceId)) {
                selectToRetrieveJobStatusAsEventPreparedStatement.setString(++index, iceId);
            }

            if (!isEmptyField(fromJobStatusId)) {
                selectToRetrieveJobStatusAsEventPreparedStatement.setString(++index, fromJobStatusId);
            }

            if (!isEmptyField(toJobStatusId)) {
                selectToRetrieveJobStatusAsEventPreparedStatement.setString(++index, toJobStatusId);
            }

            if (fromDate != null) {
                selectToRetrieveJobStatusAsEventPreparedStatement.setTimestamp(++index, new java.sql.Timestamp(fromDate.getTimeInMillis()));
            }

            if (toDate != null) {
                selectToRetrieveJobStatusAsEventPreparedStatement.setTimestamp(++index, new java.sql.Timestamp(toDate.getTimeInMillis()));
            }
            
            // execute query, and return number of rows created
            ResultSet rs = selectToRetrieveJobStatusAsEventPreparedStatement.executeQuery();
            if ((rs != null)) {
                eventList = this.buildEventsFromJobStatusAsEvent(rs);
            }
        } catch (SQLException sqle) {
            throw sqle;
        } finally {
            if (selectToRetrieveJobStatusAsEventPreparedStatement != null) {
                try {
                    selectToRetrieveJobStatusAsEventPreparedStatement.close();
                } catch (SQLException sqle1) {
                    logger.error(sqle1);
                }
            }
        }
        logger.debug("End executeSelectToRetrieveJobStatusAsEvent");
        return eventList;
    }

    private List<Event> buildEventsFromJobStatusAsEvent(ResultSet rs) throws SQLException {
        List<Event> eventList = new ArrayList<Event>(0);
        while (rs.next()) {
            eventList.add(this.buildEventFromJobStatusAsEvent(rs));
        }
        return eventList;
    }

    private Event buildEventFromJobStatusAsEvent(ResultSet rs) throws SQLException {
        Calendar timeStampEvent = null;
        timeStampEvent = Calendar.getInstance();
        timeStampEvent.setTimeInMillis(rs.getTimestamp(TIMESTAMP_FIELD).getTime());

        Event event = new Event(String.valueOf(rs.getInt(ID_FIELD)), JobStatusEventManager.MANAGER_TYPE, timeStampEvent);
        List<Property> properties = new ArrayList<Property>();
        properties.add(new Property(JobStatusEventManager.JOB_ID_PROPERTYNAME, rs.getString(JOB_ID_FIELD), "String"));
        properties.add(new Property(JobStatusEventManager.FAILURE_REASON_PROPERTYNAME, rs.getString(FAILURE_REASON_FIELD), "String"));
        properties.add(new Property(JobStatusEventManager.EXIT_CODE_PROPERTYNAME, rs.getString(EXIT_CODE_FIELD), "String"));
        properties.add(new Property(JobStatusEventManager.DESCRIPTION_PROPERTYNAME, rs.getString(DESCRIPTION_FIELD), "String"));
        properties.add(new Property(JobStatusEventManager.TYPE_PROPERTYNAME, "" + rs.getInt(TYPE_FIELD), "String"));
        properties.add(new Property(JobStatusEventManager.GRID_JOB_ID_PROPERTYNAME, rs.getString(JobTable.GRID_JOB_ID_FIELD), "String"));
        properties.add(new Property(JobStatusEventManager.WORKER_NODE_PROPERTYNAME, rs.getString(JobTable.WORKER_NODE_FIELD), "String"));
        event.setProperty(properties);
        return event;
    }

    private static boolean isEmptyField(String field) {
        return ((field == null) || ("".equals(field.trim())));
    }
}
