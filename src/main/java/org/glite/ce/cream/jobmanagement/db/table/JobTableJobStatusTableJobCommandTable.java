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
import org.glite.ce.creamapi.jobmanagement.db.table.JobTableJobStatusJobCommandInterface;

public class JobTableJobStatusTableJobCommandTable implements JobTableJobStatusJobCommandInterface {
    private final static Logger logger = Logger.getLogger(JobTableJobStatusTableJobCommandTable.class.getName());

    public JobTableJobStatusTableJobCommandTable() throws SQLException {
        logger.debug("Call JobTableJobStatusTableJobCommandTable constructor");
    }

    public void executeInsert(JobCommand jobCommand, String delegationId, int[] jobStatusType, Connection connection) throws SQLException {
        logger.debug("Begin executeInsert");

        StringBuffer insertQuery = new StringBuffer("insert into ");
        insertQuery.append(JobCommandTable.NAME_TABLE).append(" (");
        insertQuery.append(JobCommandTable.JOB_ID_FIELD).append(", ");
        insertQuery.append(JobCommandTable.STATUS_TYPE_FIELD).append(", ");
        insertQuery.append(JobCommandTable.TYPE_FIELD).append(", ");
        insertQuery.append(JobCommandTable.DESCRIPTION_FIELD).append(", ");
        insertQuery.append(JobCommandTable.FAILURE_REASON_FIELD).append(", ");
        insertQuery.append(JobCommandTable.CMD_EXECUTOR_NAME_FIELD).append(", ");
        insertQuery.append(JobCommandTable.START_SCHEDULING_TIME_FIELD).append(", ");
        insertQuery.append(JobCommandTable.START_PROCESSING_TIME_FIELD).append(", ");
        insertQuery.append(JobCommandTable.EXECUTION_COMPLETED_TIME_FIELD).append(", ");
        insertQuery.append(JobCommandTable.CREATION_TIME_FIELD).append(", ");
        insertQuery.append(JobCommandTable.USER_ID_FIELD).append(") select js.jobId, '");
        insertQuery.append(jobCommand.getStatus()).append("', '");
        insertQuery.append(jobCommand.getType()).append("', ");

        if (jobCommand.getDescription() != null) {
            insertQuery.append("'").append(jobCommand.getDescription()).append("', ");
        } else {
            insertQuery.append(" null, ");
        }

        if (jobCommand.getFailureReason() != null) {
            insertQuery.append("'").append(jobCommand.getFailureReason()).append("', ");
        } else {
            insertQuery.append(" null, ");
        }

        if (jobCommand.getCommandExecutorName() != null) {
            insertQuery.append("'").append(jobCommand.getCommandExecutorName()).append("', ");
        } else {
            insertQuery.append(" null, ");
        }

        if (jobCommand.getStartSchedulingTime() != null) {
            insertQuery.append("'").append(new java.sql.Timestamp(jobCommand.getStartSchedulingTime().getTimeInMillis())).append("', ");
        } else {
            insertQuery.append(" null, ");
        }

        if (jobCommand.getStartProcessingTime() != null) {
            insertQuery.append("'").append(new java.sql.Timestamp(jobCommand.getStartProcessingTime().getTimeInMillis())).append("', ");
        } else {
            insertQuery.append(" null, ");
        }

        if (jobCommand.getExecutionCompletedTime() != null) {
            insertQuery.append("'").append(new java.sql.Timestamp(jobCommand.getExecutionCompletedTime().getTimeInMillis())).append("', ");
        } else {
            insertQuery.append(" null, ");
        }

        if (jobCommand.getCreationTime() != null) {
            insertQuery.append("'").append(new java.sql.Timestamp(jobCommand.getCreationTime().getTimeInMillis())).append("', ");
        } else {
            insertQuery.append(" null, ");
        }

        String userId = jobCommand.getUserId();
        if (userId == null) {
            userId = JobCommandTable.USERID_ADMINISTRATOR;
        }

        insertQuery.append("'").append(userId).append("'");
        insertQuery.append(" from ").append(JobStatusTable.NAME_TABLE).append(" as js left outer join (");
        insertQuery.append(JobStatusTable.NAME_TABLE).append(" as jslatest) on jslatest.").append(JobStatusTable.JOB_ID_FIELD).append(" = js.").append(JobStatusTable.JOB_ID_FIELD);
        insertQuery.append(" and js.").append(JobStatusTable.ID_FIELD).append(" < jslatest.").append(JobStatusTable.ID_FIELD);
        insertQuery.append(" where jslatest.").append(JobStatusTable.ID_FIELD).append(" is null");

        if ((jobStatusType != null) && (jobStatusType.length > 0)) {
            insertQuery.append(" and js.").append(JobStatusTable.TYPE_FIELD).append(" in (");
            for (int i = 0; i < jobStatusType.length; i++) {
                insertQuery.append("'" + jobStatusType[i] + "', ");
            }
            insertQuery.replace(insertQuery.length() - 2, insertQuery.length(), ")");
        }

        insertQuery.append(" and js.").append(JobStatusTable.JOB_ID_FIELD).append(" in (select ");
        insertQuery.append(JobTable.NAME_TABLE).append(".").append(JobTable.ID_FIELD).append(" from ").append(JobTable.NAME_TABLE);
        insertQuery.append(" where ").append(JobTable.NAME_TABLE).append(".").append(JobTable.DELEGATION_PROXY_ID_FIELD).append(" = '").append(delegationId).append("' and ");
        insertQuery.append(JobTable.NAME_TABLE).append(".").append(JobTable.USER_ID_FIELD).append(" = '").append(userId).append("')");

        logger.debug("insertQuery = " + insertQuery.toString());

        PreparedStatement st = null;
        try {
            st = connection.prepareStatement(insertQuery.toString());
            // execute query, and return number of rows created
            int rowCount = st.executeUpdate();
            logger.debug("" + rowCount + " jobCommand items inserted!");
        } catch (SQLException sqle) {
            throw sqle;
        } finally {
            if (st != null) {
                try {
                    st.close();
                } catch (SQLException sqle1) {
                    logger.error(sqle1);
                }
            }
        }

        logger.debug("End executeInsert");
    }

    public List<String> executeSelectToRetrieveJobId(String userId, String delegationId, int[] jobStatusType, String leaseId, Calendar startStatusDate, Calendar endStatusDate, Calendar startRegisterCommandDate, Calendar endRegisterCommandDate, Connection connection) throws SQLException {
        logger.debug("Begin executeSelectToRetrieveJobId");

        List<String> jobIdList = new ArrayList<String>(0);
        String selectQuery = getSelectToRetrieveJobIdQuery(userId, delegationId, jobStatusType, leaseId, startStatusDate, endStatusDate, startRegisterCommandDate, endRegisterCommandDate);

        logger.debug("selectQuery = " + selectQuery);

        PreparedStatement selectToRetrieveJobIdPreparedStatement = connection.prepareStatement(selectQuery);
        int index = 0;

        if (userId != null) {
            selectToRetrieveJobIdPreparedStatement.setString(++index, userId);
        }

        if (delegationId != null) {
            selectToRetrieveJobIdPreparedStatement.setString(++index, delegationId);
        }

        if (leaseId != null) {
            selectToRetrieveJobIdPreparedStatement.setString(++index, leaseId);
        }

        if (startRegisterCommandDate != null) {
            selectToRetrieveJobIdPreparedStatement.setTimestamp(++index, new java.sql.Timestamp(startRegisterCommandDate.getTimeInMillis()));
        }

        if (endRegisterCommandDate != null) {
            selectToRetrieveJobIdPreparedStatement.setTimestamp(++index, new java.sql.Timestamp(endRegisterCommandDate.getTimeInMillis()));
        }

        if ((jobStatusType != null) && (jobStatusType.length > 0)) {
            if (startStatusDate != null) {
                selectToRetrieveJobIdPreparedStatement.setTimestamp(++index, new java.sql.Timestamp(startStatusDate.getTimeInMillis()));
            }

            if (endStatusDate != null) {
                selectToRetrieveJobIdPreparedStatement.setTimestamp(++index, new java.sql.Timestamp(endStatusDate.getTimeInMillis()));
            }
        }

        // execute query, and return number of rows created
        ResultSet rs = selectToRetrieveJobIdPreparedStatement.executeQuery();
        if (rs != null) {
            while (rs.next()) {
                jobIdList.add(rs.getString(JobTable.ID_FIELD));
            }
        }

        logger.debug("End executeSelectToRetrieveJobId");
        return jobIdList;
    }

    private static String getSelectToRetrieveJobIdQuery(String userId, String delegationId, int[] jobStatusType, String leaseId, Calendar startStatusDate, Calendar endStatusDate, Calendar startRegisterCommandDate, Calendar endRegisterCommandDate) {
        StringBuffer selectToRetrieveJobIdQuery = new StringBuffer();
        selectToRetrieveJobIdQuery.append("select distinct ");
        selectToRetrieveJobIdQuery.append(JobTable.NAME_TABLE + "." + JobTable.ID_FIELD + " AS " + JobTable.ID_FIELD);
        selectToRetrieveJobIdQuery.append(" from " + JobTable.NAME_TABLE);
        selectToRetrieveJobIdQuery.append(", " + JobCommandTable.NAME_TABLE);

        if ((jobStatusType != null) && (jobStatusType.length > 0)) {
            selectToRetrieveJobIdQuery.append(", " + JobStatusTable.NAME_TABLE + " AS " + JobStatusTable.NAME_TABLE + " LEFT OUTER JOIN " + JobStatusTable.NAME_TABLE + " AS jslatest ");
            selectToRetrieveJobIdQuery.append("ON jslatest." + JobStatusTable.JOB_ID_FIELD + " = " + JobStatusTable.NAME_TABLE + "." + JobStatusTable.JOB_ID_FIELD);
            selectToRetrieveJobIdQuery.append(" and " + JobStatusTable.NAME_TABLE + "." + JobStatusTable.ID_FIELD + " < jslatest." + JobStatusTable.ID_FIELD);
        }

        // trick
        selectToRetrieveJobIdQuery.append(" where true ");

        if ((userId != null) && (delegationId != null)) {
            selectToRetrieveJobIdQuery.append("and (");
            selectToRetrieveJobIdQuery.append("(" + JobTable.ICE_ID_FIELD + " IS NOT NULL ");
            selectToRetrieveJobIdQuery.append(" and " + JobTable.MY_PROXY_SERVER_FIELD + " IS NOT NULL) or (");
            selectToRetrieveJobIdQuery.append(JobTable.ICE_ID_FIELD);
            selectToRetrieveJobIdQuery.append(" IS  NULL))");
        }

        if (userId != null) {
            selectToRetrieveJobIdQuery.append(" and " + JobTable.NAME_TABLE + "." + JobTable.USER_ID_FIELD + "=?");
        }

        if (delegationId != null) {
            selectToRetrieveJobIdQuery.append(" and " + JobTable.DELEGATION_PROXY_ID_FIELD + "=?");
        }

        if (leaseId != null) {
            selectToRetrieveJobIdQuery.append(" and " + JobTable.LEASE_ID_FIELD + "=?");
        }

        selectToRetrieveJobIdQuery.append(" and " + JobCommandTable.NAME_TABLE + "." + JobCommandTable.TYPE_FIELD + "=0 ");

        if (startRegisterCommandDate != null) {
            selectToRetrieveJobIdQuery.append(" and " + JobCommandTable.NAME_TABLE + "." + JobCommandTable.CREATION_TIME_FIELD + " >= ?");
        }

        if (endRegisterCommandDate != null) {
            selectToRetrieveJobIdQuery.append(" and " + JobCommandTable.NAME_TABLE + "." + JobCommandTable.CREATION_TIME_FIELD + " <= ?");
        }

        selectToRetrieveJobIdQuery.append(" and " + JobTable.NAME_TABLE + "." + JobTable.ID_FIELD + " = " + JobCommandTable.NAME_TABLE + "." + JobCommandTable.JOB_ID_FIELD);

        if ((jobStatusType != null) && (jobStatusType.length > 0)) {
            selectToRetrieveJobIdQuery.append(" and jslatest." + JobStatusTable.ID_FIELD + " IS NULL");
            StringBuffer jobStatusTypeList = new StringBuffer();
            for (int i = 0; i < jobStatusType.length; i++) {
                jobStatusTypeList.append(", " + "'" + jobStatusType[i] + "'");
            }

            jobStatusTypeList.deleteCharAt(0);

            if (startStatusDate != null) {
                selectToRetrieveJobIdQuery.append(" and " + JobStatusTable.NAME_TABLE + "." + JobStatusTable.TIMESTAMP_FIELD + " > ?");
            }

            if (endStatusDate != null) {
                selectToRetrieveJobIdQuery.append(" and " + JobStatusTable.NAME_TABLE + "." + JobStatusTable.TIMESTAMP_FIELD + " <= ?");
            }
            selectToRetrieveJobIdQuery.append(" and " + JobStatusTable.NAME_TABLE + "." + JobStatusTable.TYPE_FIELD + " IN (" + jobStatusTypeList.toString() + ")");
            selectToRetrieveJobIdQuery.append(" and " + JobStatusTable.NAME_TABLE + "." + JobStatusTable.JOB_ID_FIELD + " = " + JobTable.NAME_TABLE + "." + JobTable.ID_FIELD);
        }

        logger.debug("selectToRetrieveJobIdQuery = " + selectToRetrieveJobIdQuery.toString());
        return selectToRetrieveJobIdQuery.toString();
    }
}
