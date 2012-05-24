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
import org.glite.ce.creamapi.jobmanagement.JobStatus;
import org.glite.ce.creamapi.jobmanagement.db.table.JobTableJobStatusInterface;

public class JobTableJobStatusTable implements JobTableJobStatusInterface {
    private final static Logger logger = Logger.getLogger(JobTableJobStatusTable.class);

    private static final String COUNT_JOB = "COUNT_JOB";

    public JobTableJobStatusTable() throws SQLException {
        logger.debug("Call JobTableJobStatusTable constructor");
    }

    public List<String> executeSelectToRetrieveJobId(String userId, List<String> jobId, String delegationId, int[] jobStatusType, String leaseId, Calendar startStatusDate, Calendar endStatusDate, String queueName, String batchSystem, Connection connection)
            throws SQLException {
        logger.debug("Begin executeSelectToRetrieveJobId");

        List<String> jobIdList = new ArrayList<String>(0);
        String selectQuery = getSelectToRetrieveJobIdQuery(userId, jobId, delegationId, jobStatusType, leaseId, startStatusDate, endStatusDate, queueName, batchSystem);

        logger.debug("selectQuery = " + selectQuery);

        PreparedStatement selectToRetrieveJobIdPreparedStatement = connection.prepareStatement(selectQuery);

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

    private static String getSelectToRetrieveJobIdQuery(String userId, List<String> jobId, String delegationId, int[] jobStatusType, String leaseId, Calendar startStatusDate, Calendar endStatusDate, String queueName, String batchSystem) {
        StringBuffer selectToRetrieveJobIdQuery = new StringBuffer();
        selectToRetrieveJobIdQuery.append("select ");
        selectToRetrieveJobIdQuery.append(JobTable.NAME_TABLE + "." + JobTable.ID_FIELD + " AS " + JobTable.ID_FIELD);
        selectToRetrieveJobIdQuery.append(" from " + JobTable.NAME_TABLE);

        if ((jobStatusType != null) && (jobStatusType.length > 0)) {
            selectToRetrieveJobIdQuery.append(", " +JobStatusTable.NAME_TABLE + " AS " + JobStatusTable.NAME_TABLE + " LEFT OUTER JOIN " + JobStatusTable.NAME_TABLE + " AS jslatest ");          
            selectToRetrieveJobIdQuery.append("ON jslatest." + JobStatusTable.JOB_ID_FIELD +  " = " + JobStatusTable.NAME_TABLE + "." + JobStatusTable.JOB_ID_FIELD); 
            selectToRetrieveJobIdQuery.append(" and " + JobStatusTable.NAME_TABLE + "." + JobStatusTable.ID_FIELD + " < jslatest." + JobStatusTable.ID_FIELD); 
        }

        // trick
        selectToRetrieveJobIdQuery.append(" where true ");

        if ((userId != null) && (delegationId != null)) {
            selectToRetrieveJobIdQuery.append(" and (");
            selectToRetrieveJobIdQuery.append("(" + JobTable.ICE_ID_FIELD + " IS NOT NULL ");
            selectToRetrieveJobIdQuery.append(" and " + JobTable.MY_PROXY_SERVER_FIELD + " IS NOT NULL " + ") ");
            selectToRetrieveJobIdQuery.append(" or (");
            selectToRetrieveJobIdQuery.append(JobTable.ICE_ID_FIELD + " IS  NULL " + ")");
            selectToRetrieveJobIdQuery.append(")");
        }

        if (userId != null) {
            selectToRetrieveJobIdQuery.append(" and " + JobTable.USER_ID_FIELD + " = " + "'" + userId + "'");
        }

        if (delegationId != null) {
            selectToRetrieveJobIdQuery.append(" and " + JobTable.DELEGATION_PROXY_ID_FIELD + " = " + "'" + delegationId + "'");
        }

        if (leaseId != null) {
            selectToRetrieveJobIdQuery.append(" and " + JobTable.LEASE_ID_FIELD + " = " + "'" + leaseId + "'");
        }

        if ((jobId != null) && (jobId.size() > 0)) {
            StringBuffer jobIdList = new StringBuffer();
            for (int i = 0; i < jobId.size(); i++) {
                jobIdList.append(", " + "'" + jobId.get(i) + "'");
            }
            jobIdList.deleteCharAt(0);
            selectToRetrieveJobIdQuery.append(" and " + JobTable.NAME_TABLE + "." + JobTable.ID_FIELD + " IN (" + jobIdList.toString() + ")");
        }

        if ((jobStatusType != null) && (jobStatusType.length > 0)) {
            selectToRetrieveJobIdQuery.append(" and jslatest." + JobStatusTable.ID_FIELD + " IS NULL");
        	StringBuffer jobStatusTypeList = new StringBuffer();
            for (int i = 0; i < jobStatusType.length; i++) {
                jobStatusTypeList.append(", " + "'" + jobStatusType[i] + "'");
            }
            jobStatusTypeList.deleteCharAt(0);
            
            if (startStatusDate != null) {
                Timestamp startStatusDateTimestampField = new java.sql.Timestamp(startStatusDate.getTimeInMillis());
                selectToRetrieveJobIdQuery.append(" and " + JobStatusTable.NAME_TABLE + "." + JobStatusTable.TIMESTAMP_FIELD + " >= '" + startStatusDateTimestampField.toString() + "'");
            }
            if (endStatusDate != null) {
                Timestamp endStatusDateTimestampField = new java.sql.Timestamp(endStatusDate.getTimeInMillis());
                selectToRetrieveJobIdQuery.append(" and " + JobStatusTable.NAME_TABLE + "." + JobStatusTable.TIMESTAMP_FIELD + " <= '" + endStatusDateTimestampField.toString() + "'");
            }
            selectToRetrieveJobIdQuery.append(" and " + JobStatusTable.NAME_TABLE + "." + JobStatusTable.TYPE_FIELD + " IN (" + jobStatusTypeList.toString() + ")");
            selectToRetrieveJobIdQuery.append(" and " + JobStatusTable.NAME_TABLE + "." + JobStatusTable.JOB_ID_FIELD + " = " + JobTable.NAME_TABLE + "." + JobTable.ID_FIELD);
        }
        
        if (queueName != null) {
            selectToRetrieveJobIdQuery.append(" and " + JobTable.QUEUE_FIELD + " = " + "'" + queueName + "'");
        }
        
        if (batchSystem != null) {
            selectToRetrieveJobIdQuery.append(" and " + JobTable.BATCH_SYSTEM_FIELD + " = " + "'" + batchSystem + "'");
        }

        logger.debug("selectToRetrieveJobIdQuery = " + selectToRetrieveJobIdQuery.toString());
        return selectToRetrieveJobIdQuery.toString();
    }

    public String executeSelectToRetrieveOlderJobIdQuery(int[] jobStatusType, String batchSystem, String userId, Connection connection) throws SQLException {
        String jobId = null;
        String selectToRetrieveOlderJobIdQuery = getSelectToRetrieveOlderJobIdQuery(userId, jobStatusType, batchSystem);

        logger.debug("SelectToRetrieveOlderJobIdQuery = " + selectToRetrieveOlderJobIdQuery);

        PreparedStatement ps = connection.prepareStatement(selectToRetrieveOlderJobIdQuery);

        // execute query.
        ResultSet rs = ps.executeQuery();
        if ((rs != null) && rs.next()) {
            jobId = rs.getString(JobStatusTable.JOB_ID_FIELD);
        }

        return jobId;
    }
//select jobId from (select job_status.jobId, max(job_status.type) as type, job_status.time_stamp from job_status group by job_status.jobId) as tbl where type in (1,2,3,4) order by time_stamp;
    
    private static String getSelectToRetrieveOlderJobIdQuery(String userId, int[] jobStatusType, String batchSystem) {
        StringBuffer selectToRetrieveOlderJobIdQuery = new StringBuffer();
        selectToRetrieveOlderJobIdQuery.append("select ");
        selectToRetrieveOlderJobIdQuery.append(JobStatusTable.NAME_TABLE + "." + JobStatusTable.JOB_ID_FIELD);
        selectToRetrieveOlderJobIdQuery.append(" from  ");
        selectToRetrieveOlderJobIdQuery.append(JobTable.NAME_TABLE);
        selectToRetrieveOlderJobIdQuery.append(", " +JobStatusTable.NAME_TABLE + " AS " + JobStatusTable.NAME_TABLE + " LEFT OUTER JOIN " + JobStatusTable.NAME_TABLE + " AS jslatest ");          
        selectToRetrieveOlderJobIdQuery.append("ON jslatest." + JobStatusTable.JOB_ID_FIELD +  " = " + JobStatusTable.NAME_TABLE + "." + JobStatusTable.JOB_ID_FIELD); 
        selectToRetrieveOlderJobIdQuery.append(" and " + JobStatusTable.NAME_TABLE + "." + JobStatusTable.ID_FIELD + " < jslatest." + JobStatusTable.ID_FIELD); 
        
        selectToRetrieveOlderJobIdQuery.append(" where ");
        selectToRetrieveOlderJobIdQuery.append(JobStatusTable.NAME_TABLE + "." + JobStatusTable.JOB_ID_FIELD + " = " + JobTable.NAME_TABLE + "." + JobTable.ID_FIELD);
        
        if (userId != null) {
        	selectToRetrieveOlderJobIdQuery.append(" and " + JobTable.NAME_TABLE + "." + JobTable.USER_ID_FIELD + " = '" + userId + "'");
        }
        
        if (batchSystem != null) {
        	selectToRetrieveOlderJobIdQuery.append(" and " + JobTable.NAME_TABLE + "." + JobTable.BATCH_SYSTEM_FIELD + " = '" + batchSystem + "'");
        }
            
        if (jobStatusType != null && jobStatusType.length > 0) {
        	selectToRetrieveOlderJobIdQuery.append(" and jslatest." + JobStatusTable.ID_FIELD + " IS NULL");
            StringBuffer jobStatusTypeList = new StringBuffer();
            for (int i = 0; i < jobStatusType.length; i++) {
                jobStatusTypeList.append(", " + "'" + jobStatusType[i] + "'");
            }
            jobStatusTypeList.deleteCharAt(0);
            
            selectToRetrieveOlderJobIdQuery.append(" and " + JobStatusTable.NAME_TABLE + "." + JobStatusTable.TYPE_FIELD + " IN (" + jobStatusTypeList.toString() + ")");
        }
        selectToRetrieveOlderJobIdQuery.append(" order by ");
        selectToRetrieveOlderJobIdQuery.append(JobStatusTable.NAME_TABLE + "." + JobStatusTable.TIMESTAMP_FIELD);
        selectToRetrieveOlderJobIdQuery.append(" limit 1");

        return selectToRetrieveOlderJobIdQuery.toString();
    }

    public List<String> executeSelectToRetrieveJobIdByLeaseTimeExpiredQuery(String userId, String delegationId, int[] jobStatusType, Connection connection) throws SQLException {
        logger.debug("Begin executeSelectToRetrieveJobIdByLeaseTimeExpiredQuery");
        List<String> jobIdList = new ArrayList<String>();
        String selectQuery = getSelectToRetrieveJobIdByLeaseTimeExpiredQuery(userId, delegationId, jobStatusType);
        logger.debug("selectQuery = " + selectQuery);
        PreparedStatement selectToRetrieveJobIdByLeaseTimeExpiredPreparedStatement = connection.prepareStatement(selectQuery);
        // execute query.
        ResultSet rs = selectToRetrieveJobIdByLeaseTimeExpiredPreparedStatement.executeQuery();
        if (rs != null) {
            while (rs.next()) {
                jobIdList.add(rs.getString(JobTable.ID_FIELD));
            }
        }
        logger.debug("End executeSelectToRetrieveJobIdByLeaseTimeExpiredQuery");
        return jobIdList;
    }

    private static String getSelectToRetrieveJobIdByLeaseTimeExpiredQuery(String userId, String delegationId, int[] jobStatusType) {
        StringBuffer selectToRetrieveJobIdByLeaseTimeExpiredQuery = new StringBuffer();
        selectToRetrieveJobIdByLeaseTimeExpiredQuery.append("select ");
        selectToRetrieveJobIdByLeaseTimeExpiredQuery.append(JobTable.NAME_TABLE + "." + JobTable.ID_FIELD + " AS " + JobTable.ID_FIELD);
        selectToRetrieveJobIdByLeaseTimeExpiredQuery.append(" from " + JobTable.NAME_TABLE);

        if ((jobStatusType != null) && (jobStatusType.length > 0)) {
            selectToRetrieveJobIdByLeaseTimeExpiredQuery.append(", " +JobStatusTable.NAME_TABLE + " AS " + JobStatusTable.NAME_TABLE + " LEFT OUTER JOIN " + JobStatusTable.NAME_TABLE + " AS jslatest ");          
            selectToRetrieveJobIdByLeaseTimeExpiredQuery.append("ON jslatest." + JobStatusTable.JOB_ID_FIELD +  " = " + JobStatusTable.NAME_TABLE + "." + JobStatusTable.JOB_ID_FIELD); 
            selectToRetrieveJobIdByLeaseTimeExpiredQuery.append(" and " + JobStatusTable.NAME_TABLE + "." + JobStatusTable.ID_FIELD + " < jslatest." + JobStatusTable.ID_FIELD); 
        }

        // trick
        selectToRetrieveJobIdByLeaseTimeExpiredQuery.append(" where true ");

        if (userId != null) {
            selectToRetrieveJobIdByLeaseTimeExpiredQuery.append(" and " + JobTable.USER_ID_FIELD + " = " + "'" + userId + "'");
        }

        if (delegationId != null) {
            selectToRetrieveJobIdByLeaseTimeExpiredQuery.append(" and " + JobTable.DELEGATION_PROXY_ID_FIELD + " = " + "'" + delegationId + "'");
        }

        selectToRetrieveJobIdByLeaseTimeExpiredQuery.append(" and " + JobTable.LEASE_TIME_FIELD + " IS NOT NULL");

        if ((jobStatusType != null) && (jobStatusType.length > 0)) {
            selectToRetrieveJobIdByLeaseTimeExpiredQuery.append(" and jslatest." + JobStatusTable.ID_FIELD + " IS NULL");
        	StringBuffer jobStatusTypeList = new StringBuffer();
            for (int i = 0; i < jobStatusType.length; i++) {
                jobStatusTypeList.append(", " + "'" + jobStatusType[i] + "'");
            }
            jobStatusTypeList.deleteCharAt(0);
            selectToRetrieveJobIdByLeaseTimeExpiredQuery.append(" and " + JobStatusTable.NAME_TABLE + "." + JobStatusTable.TYPE_FIELD + " IN (" + jobStatusTypeList.toString()
                    + ")");
            selectToRetrieveJobIdByLeaseTimeExpiredQuery.append(" and " + JobStatusTable.NAME_TABLE + "." + JobStatusTable.JOB_ID_FIELD + " = " + JobTable.NAME_TABLE + "."
                    + JobTable.ID_FIELD);
        }
        logger.debug("selectToRetrieveJobIdByLeaseTimeExpiredQuery = " + selectToRetrieveJobIdByLeaseTimeExpiredQuery.toString());
        return selectToRetrieveJobIdByLeaseTimeExpiredQuery.toString();
    }

    public long executeSelectToJobCountByStatus(int[] jobStatusType, String userId, Connection connection) throws SQLException {
        long jobCountByStatus = 0;
        String selectToJobCountByStatusQuery = getSelectToJobCountByStatusQuery(jobStatusType, userId);
        logger.debug("selectToJobCountByStatusQuery = " + selectToJobCountByStatusQuery);
        PreparedStatement selectToJobCountByStatusPreparedStatement = connection.prepareStatement(selectToJobCountByStatusQuery);
        // execute query.
        ResultSet rs = selectToJobCountByStatusPreparedStatement.executeQuery();
        if ((rs != null) && rs.next()) {
            jobCountByStatus = rs.getLong(JobTableJobStatusTable.COUNT_JOB);
        }
        return jobCountByStatus;
    }

    public void executeUpdateDelegationProxyInfo(String delegationId, String delegationProxyInfo, String userId, Connection connection) throws SQLException, IllegalArgumentException {
        if (delegationId == null || "".equals(delegationId)) {
            logger.error("delegationId not specified!");
            throw new IllegalArgumentException("delegationId not specified!");
        }
        
        if (delegationProxyInfo == null || "".equals(delegationProxyInfo)) {
            logger.error("delegationProxyInfo not specified!");
            throw new IllegalArgumentException("delegationProxyInfo not specified!");
        }
        
        if (userId == null || "".equals(userId)) {
            logger.error("userId not specified!");
            throw new IllegalArgumentException("userId not specified!");
        }
        
        if (connection == null) {
            logger.error("connection not specified!");
            throw new IllegalArgumentException("connection not specified!");
        }
        
        final String jobTableJobId = JobTable.NAME_TABLE + "." + JobTable.ID_FIELD;
        final String jobTableUserId = JobTable.NAME_TABLE + "." + JobTable.USER_ID_FIELD;
        final String jobTableDelegationProxyId = JobTable.NAME_TABLE + "." + JobTable.DELEGATION_PROXY_ID_FIELD;
        final String jobStatusTableJobId = JobStatusTable.NAME_TABLE + "." + JobStatusTable.JOB_ID_FIELD;
        final String jobStatusTableId = JobStatusTable.NAME_TABLE + "." + JobStatusTable.ID_FIELD;
        final String jobStatusTableType = JobStatusTable.NAME_TABLE + "." + JobStatusTable.TYPE_FIELD;
        final String jslatestId = "jslatest." + JobStatusTable.ID_FIELD;

        StringBuffer updateDelegationProxyInfoQuery = new StringBuffer("update ");
        updateDelegationProxyInfoQuery.append(JobTable.NAME_TABLE).append(" set ").append(JobTable.DELEGATION_PROXY_INFO_FIELD);
        updateDelegationProxyInfoQuery.append(" = '").append(delegationProxyInfo).append("' where ").append(JobTable.ID_FIELD).append(" = (select ");
        updateDelegationProxyInfoQuery.append(jobStatusTableJobId).append(" from ").append(JobStatusTable.NAME_TABLE).append(" AS ");
        updateDelegationProxyInfoQuery.append(JobStatusTable.NAME_TABLE).append(" LEFT OUTER JOIN ").append(JobStatusTable.NAME_TABLE).append(" AS jslatest ON jslatest.");
        updateDelegationProxyInfoQuery.append(JobStatusTable.JOB_ID_FIELD).append(" = ").append(jobStatusTableJobId);
        updateDelegationProxyInfoQuery.append(" and ").append(jobStatusTableId).append(" < ").append(jslatestId);
        updateDelegationProxyInfoQuery.append(" where ").append(jslatestId).append(" IS NULL and ").append(jobStatusTableType);
        updateDelegationProxyInfoQuery.append(" IN ('").append(JobStatus.REGISTERED).append("', '");
        updateDelegationProxyInfoQuery.append(JobStatus.HELD).append("', '").append(JobStatus.IDLE).append("', '");
        updateDelegationProxyInfoQuery.append(JobStatus.PENDING).append("', '").append(JobStatus.REALLY_RUNNING).append("', '");
        updateDelegationProxyInfoQuery.append(JobStatus.RUNNING).append("') and ").append(jobStatusTableJobId).append(" = ");
        updateDelegationProxyInfoQuery.append(jobTableJobId).append(" and ").append(jobTableDelegationProxyId);
        updateDelegationProxyInfoQuery.append(" = '").append(delegationId).append("' and ");
        updateDelegationProxyInfoQuery.append(jobTableUserId).append(" = '").append(userId).append("')");

        logger.debug("updateDelegationProxyInfoQuery = " + updateDelegationProxyInfoQuery.toString());

        PreparedStatement updateDelegationProxyInfoPreparedStatement = null;

        try {
            updateDelegationProxyInfoPreparedStatement = connection.prepareStatement(updateDelegationProxyInfoQuery.toString());
            updateDelegationProxyInfoPreparedStatement.executeUpdate();
        } catch (SQLException sqle) {
            throw sqle;
        } finally {
            if (updateDelegationProxyInfoPreparedStatement != null) {
                try {
                    updateDelegationProxyInfoPreparedStatement.close();
                } catch (SQLException sqle1) {
                    logger.error(sqle1);
                }
            }
        }
    }
    
    private static String getSelectToJobCountByStatusQuery(int[] jobStatusType, String userId) {
        StringBuffer selectToJobCountByStatusQuery = new StringBuffer();
        selectToJobCountByStatusQuery.append("select count(*) AS " + JobTableJobStatusTable.COUNT_JOB);
        selectToJobCountByStatusQuery.append(" from " + JobTable.NAME_TABLE);

        if ((jobStatusType != null) && (jobStatusType.length > 0)) {
            selectToJobCountByStatusQuery.append(", " +JobStatusTable.NAME_TABLE + " AS " + JobStatusTable.NAME_TABLE + " LEFT OUTER JOIN " + JobStatusTable.NAME_TABLE + " AS jslatest ");          
            selectToJobCountByStatusQuery.append("ON jslatest." + JobStatusTable.JOB_ID_FIELD +  " = " + JobStatusTable.NAME_TABLE + "." + JobStatusTable.JOB_ID_FIELD); 
            selectToJobCountByStatusQuery.append(" and " + JobStatusTable.NAME_TABLE + "." + JobStatusTable.ID_FIELD + " < jslatest." + JobStatusTable.ID_FIELD); 
        }

        // trick
        selectToJobCountByStatusQuery.append(" where true ");

        if (userId != null) {
            selectToJobCountByStatusQuery.append(" and " + JobTable.USER_ID_FIELD + " = " + "'" + userId + "'");
        }

        if ((jobStatusType != null) && (jobStatusType.length > 0)) {
        	selectToJobCountByStatusQuery.append(" and jslatest." + JobStatusTable.ID_FIELD + " IS NULL");
            StringBuffer jobStatusTypeList = new StringBuffer();
            for (int i = 0; i < jobStatusType.length; i++) {
                jobStatusTypeList.append(", " + "'" + jobStatusType[i] + "'");
            }
            jobStatusTypeList.deleteCharAt(0);
            selectToJobCountByStatusQuery.append(" and " + JobStatusTable.NAME_TABLE + "." + JobStatusTable.TYPE_FIELD + " IN (" + jobStatusTypeList.toString() + ")");
            selectToJobCountByStatusQuery.append(" and " + JobStatusTable.NAME_TABLE + "." + JobStatusTable.JOB_ID_FIELD + " = " + JobTable.NAME_TABLE + "." + JobTable.ID_FIELD);
        }

        return selectToJobCountByStatusQuery.toString();
    }
       
}
