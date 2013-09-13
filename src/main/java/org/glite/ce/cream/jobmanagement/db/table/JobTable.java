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

import org.glite.ce.creamapi.jobmanagement.Job;
import org.glite.ce.creamapi.jobmanagement.Lease;
import org.glite.ce.creamapi.jobmanagement.db.table.JobTableInterface;

public class JobTable implements JobTableInterface {

    public final static String NAME_TABLE = "job";

    public final static String CREAM_URL_FIELD = "creamURL";
    public final static String ID_FIELD = "id";
    public final static String CEREQUIREMENTS_FIELD = "cerequirements";
    public final static String VIRTUAL_ORGANIZATION_FIELD = "virtualOrganization";
    public final static String USER_ID_FIELD = "userId";
    public final static String BATCH_SYSTEM_FIELD = "batchSystem";
    public final static String QUEUE_FIELD = "queue";
    public final static String STANDARD_INPUT_FIELD = "standardInput";
    public final static String STANDARD_OUTPUT_FIELD = "standardOutput";
    public final static String STANDARD_ERROR_FIELD = "standardError";
    public final static String EXECUTABLE_FIELD = "executable";
    public final static String DELEGATION_PROXY_CERT_PATH_FIELD = "delegationProxyCertPath";
    public final static String AUTHN_PROXY_CERT_PATH_FIELD = "authNProxyCertPath";
    public final static String HLR_LOCATION_FIELD = "hlrLocation";
    public final static String LOGGER_DEST_URI_FIELD = "loggerDestURI";
    public final static String TOKEN_URL_FIELD = "tokenURL";
    public final static String PERUSAL_FILES_DEST_URI_FIELD = "perusalFilesDestURI";
    public final static String PERUSAL_LIST_FILE_URI_FIELD = "perusalListFileURI";
    public final static String PERUSAL_TIME_INTERVAL_FIELD = "perusalTimeInterval";
    public final static String NODES_FIELD = "nodes";
    public final static String PROLOGUE_ARGUMENTS_FIELD = "prologueArguments";
    public final static String PROLOGUE_FIELD = "prologue";
    public final static String EPILOGUE_FIELD = "epilogue";
    public final static String EPILOGUE_ARGUMENTS_FIELD = "epilogueArguments";
    public final static String SEQUENCE_CODE_FIELD = "sequenceCode";
    public final static String LRMS_JOB_ID_FIELD = "lrmsJobId";
    public final static String LRMS_ABS_LAYER_JOB_ID_FIELD = "lrmsAbsLayerJobId";
    public final static String GRID_JOB_ID_FIELD = "gridJobId";
    public final static String ICE_ID_FIELD = "iceId";
    public final static String FATHER_JOB_ID_FIELD = "fatherJobId";
    public final static String CE_ID_FIELD = "ceId";
    public final static String TYPE_FIELD = "type";
    public final static String CREAM_INPUT_SANDBOX_URI_FIELD = "creamInputSandboxURI";
    public final static String CREAM_OUTPUT_SANDBOX_URI_FIELD = "creamOutputSandboxURI";
    public final static String SANDBOX_BASE_PATH_FIELD = "sandboxBasePath";
    public final static String INPUT_SANDBOX_BASE_URI_FIELD = "inputSandboxBaseURI";
    public final static String OUTPUT_SANDBOX_BASE_DEST_URI_FIELD = "outputSandboxBaseDestURI";
    public final static String WORKER_NODE_FIELD = "workerNode";
    public final static String JDL_FIELD = "jdl";
    public final static String LOCAL_USER_FIELD = "localUser";
    public final static String DELEGATION_PROXY_ID_FIELD = "delegationProxyId";
    public final static String DELEGATION_PROXY_INFO_FIELD = "delegationProxyInfo";
    public final static String WORKING_DIRECTORY_FIELD = "workingDirectory";
    public final static String LEASE_ID_FIELD = "leaseId";
    public final static String LEASE_TIME_FIELD = "leaseTime";
    public final static String MY_PROXY_SERVER_FIELD = "myProxyServer";

    // Constants for the management of the LeaseId field.
    public final static String LEASEID_EXPIRED = "LEASEID_EXPIRED";
    public final static String JOB_NOT_FOUND = "JOB_NOT_FOUND";

    private final static String insertQuery = getInsertQuery();
    private final static String deleteQuery = getDeleteQuery();

    private final static Logger logger = Logger.getLogger(JobTable.class);

    public JobTable() throws SQLException {
        logger.debug("Call JobTable constructor");
    }

    public String executeSelectUserId(String jobId, Connection connection) throws SQLException {
        logger.debug("Begin executeSelectUserId");
        String userId = null;
        PreparedStatement selectUserIdPreparedStatement = null;
        try {
            selectUserIdPreparedStatement = connection.prepareStatement(getSelectUserIdQuery());
            selectUserIdPreparedStatement = fillSelectUserIdPreparedStatement(jobId, selectUserIdPreparedStatement);
            // execute query, and return number of rows created
            ResultSet resultSet = selectUserIdPreparedStatement.executeQuery();
            if ((resultSet != null) && resultSet.next()) {
                userId = resultSet.getString(JobTable.USER_ID_FIELD);
            }
        } catch (SQLException sqle) {
            throw sqle;
        } finally {
            if (selectUserIdPreparedStatement != null) {
                try {
                    selectUserIdPreparedStatement.close();
                } catch (SQLException sqle1) {
                    logger.error(sqle1);
                }
            }
        }
        logger.debug("End executeSelectUserId");
        return userId;
    }

    public boolean isUserEnable(String jobId, String userId, Connection connection) {
        logger.debug("Begin isUserEnable");
        boolean isEnable = false;
        if (userId == null) {
            isEnable = true; // administrator
        } else {
            try {
                String userIdFromDB = executeSelectUserId(jobId, connection);
                logger.debug("userIdFromDB = " + userIdFromDB + " for jobId = " + jobId);
                if (userId.equals(userIdFromDB)) {
                    isEnable = true;
                }
            } catch (SQLException sqle) {
                logger.error("Problem to retrieve userId from DB : " + sqle.getMessage());
            }
        }
        logger.debug("End isUserEnable");
        return isEnable;
    }

    public int executeDelete(String jobId, Connection connection) throws SQLException {
        logger.debug("Begin executeDelete");
        // logger.debug("deleteQuery (JobTable)= " + deleteQuery);
        PreparedStatement deletePreparedStatement = null;
        int rowCount = 0;
        try {
            deletePreparedStatement = connection.prepareStatement(deleteQuery);
            deletePreparedStatement = fillDeletePreparedStatement(jobId, deletePreparedStatement);
            rowCount = deletePreparedStatement.executeUpdate();
            logger.info("Job deleted. JobId = " + jobId);
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

    private PreparedStatement fillSelectUserIdPreparedStatement(String jobId, PreparedStatement selectUserIdPreparedStatement) throws SQLException {
        selectUserIdPreparedStatement.setString(1, jobId);
        return selectUserIdPreparedStatement;
    }

    public int executeUpdate(Job job, Connection connection) throws SQLException {
        logger.debug("Begin executeUpdate");
        // logger.debug("updateQuery (JobTable)= " + updateQuery);
        PreparedStatement updatePreparedStatement = null;
        int rowCount = 0;
        try {
            updatePreparedStatement = connection.prepareStatement(getUpdateQuery(job));
            updatePreparedStatement = fillUpdatePreparedStatement(job, updatePreparedStatement);
            // execute query, and return number of rows created
            rowCount = updatePreparedStatement.executeUpdate();
            logger.debug("Job updated. (rowCount = " + rowCount + ")");
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
        logger.debug("End executeUpdate");
        return rowCount;
    }

    public int executeInsert(Job job, Connection connection) throws SQLException {
        logger.debug("Begin executeInsert");
        // logger.debug("insertQuery (JobTable)= " + insertQuery);
        PreparedStatement insertPreparedStatement = null;
        int rowCount = 0;
        // check leaseId field.
        /*
         * if ( (job.getLease() != null) &&
         * (!JobTable.isLeaseIdAllowed(job.getLease().getLeaseId()))){ throw new
         * IllegalArgumentException("LeaseId is invalid. It mustn't contain " +
         * JobTable.UNDERSCORE + " and the '" + JobTable.EXPIRED_STRING +
         * "' string."); }
         */
        try {
            insertPreparedStatement = connection.prepareStatement(insertQuery);
            insertPreparedStatement = fillInsertPreparedStatement(job, insertPreparedStatement);
            // execute query, and return number of rows created
            rowCount = insertPreparedStatement.executeUpdate();
            logger.info("Job inserted. JobId = " + job.getId());
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
        return rowCount;
    }

    private PreparedStatement fillInsertPreparedStatement(Job job, PreparedStatement insertPreparedStatement) throws SQLException {
        insertPreparedStatement.setString(1, job.getId());
        insertPreparedStatement.setString(2, job.getCreamURL());
        insertPreparedStatement.setString(3, job.getCeRequirements());
        insertPreparedStatement.setString(4, job.getVirtualOrganization());
        insertPreparedStatement.setString(5, job.getUserId());
        insertPreparedStatement.setString(6, job.getBatchSystem());
        insertPreparedStatement.setString(7, job.getQueue());
        insertPreparedStatement.setString(8, job.getStandardInput());
        insertPreparedStatement.setString(9, job.getStandardOutput());
        insertPreparedStatement.setString(10, job.getStandardError());
        insertPreparedStatement.setString(11, job.getExecutable());
        insertPreparedStatement.setString(12, job.getDelegationProxyCertPath());
        insertPreparedStatement.setString(13, job.getAuthNProxyCertPath());
        insertPreparedStatement.setString(14, job.getHlrLocation());
        insertPreparedStatement.setString(15, job.getLoggerDestURI());
        insertPreparedStatement.setString(16, job.getTokenURL());
        insertPreparedStatement.setString(17, job.getPerusalFilesDestURI());
        insertPreparedStatement.setString(18, job.getPerusalListFileURI());
        insertPreparedStatement.setInt(19, job.getPerusalTimeInterval());
        insertPreparedStatement.setInt(20, job.getNodeNumber());
        insertPreparedStatement.setString(21, job.getPrologueArguments());
        insertPreparedStatement.setString(22, job.getPrologue());
        insertPreparedStatement.setString(23, job.getEpilogue());
        insertPreparedStatement.setString(24, job.getEpilogueArguments());
        insertPreparedStatement.setString(25, job.getSequenceCode());
        insertPreparedStatement.setString(26, job.getLRMSJobId());
        insertPreparedStatement.setString(27, job.getLRMSAbsLayerJobId());
        insertPreparedStatement.setString(28, job.getGridJobId());
        insertPreparedStatement.setString(29, job.getICEId());
        insertPreparedStatement.setString(30, job.getFatherJobId());
        insertPreparedStatement.setString(31, job.getCeId());
        insertPreparedStatement.setString(32, job.getType());
        insertPreparedStatement.setString(33, job.getCREAMInputSandboxURI());
        insertPreparedStatement.setString(34, job.getCREAMOutputSandboxURI());
        insertPreparedStatement.setString(35, job.getSandboxBasePath());
        insertPreparedStatement.setString(36, job.getInputSandboxBaseURI());
        insertPreparedStatement.setString(37, job.getOutputSandboxBaseDestURI());
        insertPreparedStatement.setString(38, job.getWorkerNode());
        insertPreparedStatement.setString(39, job.getJDL());
        insertPreparedStatement.setString(40, job.getLocalUser());
        insertPreparedStatement.setString(41, job.getDelegationProxyId());
        insertPreparedStatement.setString(42, job.getDelegationProxyInfo());
        insertPreparedStatement.setString(43, job.getWorkingDirectory());
        if ((job.getLease() != null) && (job.getLease().getLeaseId() != null)) {
            insertPreparedStatement.setString(44, job.getLease().getLeaseId());
        } else {
            insertPreparedStatement.setNull(44, java.sql.Types.VARCHAR);
        }
        insertPreparedStatement.setTimestamp(45, null);

        if ((job.getMyProxyServer() != null) && (!"".equals(job.getMyProxyServer()))) {
            insertPreparedStatement.setString(46, job.getMyProxyServer());
        } else {
            insertPreparedStatement.setNull(46, java.sql.Types.VARCHAR);
        }
        return insertPreparedStatement;
    }

    private PreparedStatement fillUpdatePreparedStatement(Job job, PreparedStatement updatePreparedStatement) throws SQLException {
        updatePreparedStatement.setString(1, job.getCreamURL());
        updatePreparedStatement.setString(2, job.getCeRequirements());
        updatePreparedStatement.setString(3, job.getBatchSystem());
        updatePreparedStatement.setString(4, job.getQueue());
        updatePreparedStatement.setString(5, job.getStandardInput());
        updatePreparedStatement.setString(6, job.getStandardOutput());
        updatePreparedStatement.setString(7, job.getStandardError());
        updatePreparedStatement.setString(8, job.getExecutable());
        updatePreparedStatement.setString(9, job.getDelegationProxyCertPath());
        updatePreparedStatement.setString(10, job.getAuthNProxyCertPath());
        updatePreparedStatement.setString(11, job.getHlrLocation());
        updatePreparedStatement.setString(12, job.getLoggerDestURI());
        updatePreparedStatement.setString(13, job.getTokenURL());
        updatePreparedStatement.setString(14, job.getPerusalFilesDestURI());
        updatePreparedStatement.setString(15, job.getPerusalListFileURI());
        updatePreparedStatement.setInt(16, job.getPerusalTimeInterval());
        updatePreparedStatement.setInt(17, job.getNodeNumber());
        updatePreparedStatement.setString(18, job.getPrologueArguments());
        updatePreparedStatement.setString(19, job.getPrologue());
        updatePreparedStatement.setString(20, job.getEpilogue());
        updatePreparedStatement.setString(21, job.getEpilogueArguments());
        updatePreparedStatement.setString(22, job.getSequenceCode());
        updatePreparedStatement.setString(23, job.getICEId());
        updatePreparedStatement.setString(24, job.getCeId());
        updatePreparedStatement.setString(25, job.getType());
        updatePreparedStatement.setString(26, job.getCREAMInputSandboxURI());
        updatePreparedStatement.setString(27, job.getCREAMOutputSandboxURI());
        updatePreparedStatement.setString(28, job.getSandboxBasePath());
        updatePreparedStatement.setString(29, job.getInputSandboxBaseURI());
        updatePreparedStatement.setString(30, job.getOutputSandboxBaseDestURI());
        updatePreparedStatement.setString(31, job.getWorkerNode());
        updatePreparedStatement.setString(32, job.getJDL());
        updatePreparedStatement.setString(33, job.getLocalUser());
        updatePreparedStatement.setString(34, job.getDelegationProxyId());
        updatePreparedStatement.setString(35, job.getDelegationProxyInfo());
        updatePreparedStatement.setString(36, job.getWorkingDirectory());

        int index = 37;
        if ((job.getLRMSJobId() != null) && (!Job.NOT_AVAILABLE_VALUE.equals(job.getLRMSJobId()))) {
            updatePreparedStatement.setString(index, job.getLRMSJobId());
            index++;
        }
        if ((job.getLRMSAbsLayerJobId() != null) && (!Job.NOT_AVAILABLE_VALUE.equals(job.getLRMSAbsLayerJobId()))) {
            updatePreparedStatement.setString(index, job.getLRMSAbsLayerJobId());
            index++;
        }
        updatePreparedStatement.setString(index, job.getId());
        return updatePreparedStatement;
    }

    public Job executeSelectJobTable(String jobId, String userId, Connection connection) throws SQLException {
        logger.debug("Begin executeSelectJobTable");
        Job job = null;
        PreparedStatement selectPreparedStatement = null;
        try {
            int index = 0;
            
            selectPreparedStatement = connection.prepareStatement(getSelectQuery(jobId, userId));
            selectPreparedStatement.setString(++index, jobId);
            
            if (userId != null) {
                selectPreparedStatement.setString(++index, userId);
            }
            
            // execute query, and return number of rows created
            ResultSet rs = selectPreparedStatement.executeQuery();
            if ((rs != null) && rs.next()) {
                job = this.buildJobTableObj(rs);
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
        
        logger.debug("End executeSelectJobTable");
        return job;
    }

    private Job buildJobTableObj(ResultSet rs) throws SQLException {
        Job job = new Job();
        job.setId(rs.getString(JobTable.ID_FIELD));
        job.setCreamURL(rs.getString(JobTable.CREAM_URL_FIELD));
        job.setCeRequirements(rs.getString(JobTable.CEREQUIREMENTS_FIELD));
        job.setVirtualOrganization(rs.getString(JobTable.VIRTUAL_ORGANIZATION_FIELD));
        job.setUserId(rs.getString(JobTable.USER_ID_FIELD));
        job.setBatchSystem(rs.getString(JobTable.BATCH_SYSTEM_FIELD));
        job.setQueue(rs.getString(JobTable.QUEUE_FIELD));
        job.setStandardInput(rs.getString(JobTable.STANDARD_INPUT_FIELD));
        job.setStandardOutput(rs.getString(JobTable.STANDARD_OUTPUT_FIELD));
        job.setStandardError(rs.getString(JobTable.STANDARD_ERROR_FIELD));
        job.setExecutable(rs.getString(JobTable.EXECUTABLE_FIELD));
        job.setDelegationProxyCertPath(rs.getString(JobTable.DELEGATION_PROXY_CERT_PATH_FIELD));
        job.setAuthNProxyCertPath(rs.getString(JobTable.AUTHN_PROXY_CERT_PATH_FIELD));
        job.setHlrLocation(rs.getString(JobTable.HLR_LOCATION_FIELD));
        job.setLoggerDestURI(rs.getString(JobTable.LOGGER_DEST_URI_FIELD));
        job.setTokenURL(rs.getString(JobTable.TOKEN_URL_FIELD));
        job.setPerusalFilesDestURI(rs.getString(JobTable.PERUSAL_FILES_DEST_URI_FIELD));
        job.setPerusalListFileURI(rs.getString(JobTable.PERUSAL_LIST_FILE_URI_FIELD));
        job.setPerusalTimeInterval(rs.getInt(JobTable.PERUSAL_TIME_INTERVAL_FIELD));
        job.setNodeNumber(rs.getInt(JobTable.NODES_FIELD));
        job.setPrologueArguments(rs.getString(JobTable.PROLOGUE_ARGUMENTS_FIELD));
        job.setPrologue(rs.getString(JobTable.PROLOGUE_FIELD));
        job.setEpilogue(rs.getString(JobTable.EPILOGUE_FIELD));
        job.setEpilogueArguments(rs.getString(JobTable.EPILOGUE_ARGUMENTS_FIELD));
        job.setSequenceCode(rs.getString(JobTable.SEQUENCE_CODE_FIELD));
        job.setLRMSJobId(rs.getString(JobTable.LRMS_JOB_ID_FIELD));
        job.setLRMSAbsLayerJobId(rs.getString(JobTable.LRMS_ABS_LAYER_JOB_ID_FIELD));
        job.setGridJobId(rs.getString(JobTable.GRID_JOB_ID_FIELD));
        job.setICEId(rs.getString(JobTable.ICE_ID_FIELD));
        job.setFatherJobId(rs.getString(JobTable.FATHER_JOB_ID_FIELD));
        job.setCeId(rs.getString(JobTable.CE_ID_FIELD));
        job.setType(rs.getString(JobTable.TYPE_FIELD));
        job.setCREAMInputSandboxURI(rs.getString(JobTable.CREAM_INPUT_SANDBOX_URI_FIELD));
        job.setCREAMOutputSandboxURI(rs.getString(JobTable.CREAM_OUTPUT_SANDBOX_URI_FIELD));
        job.setSandboxBasePath(rs.getString(JobTable.SANDBOX_BASE_PATH_FIELD));
        job.setInputSandboxBaseURI(rs.getString(JobTable.INPUT_SANDBOX_BASE_URI_FIELD));
        job.setOutputSandboxBaseDestURI(rs.getString(JobTable.OUTPUT_SANDBOX_BASE_DEST_URI_FIELD));
        job.setWorkerNode(rs.getString(JobTable.WORKER_NODE_FIELD));
        job.setJDL(rs.getString(JobTable.JDL_FIELD));
        job.setLocalUser(rs.getString(JobTable.LOCAL_USER_FIELD));
        job.setDelegationProxyId(rs.getString(JobTable.DELEGATION_PROXY_ID_FIELD));
        job.setDelegationProxyInfo(rs.getString(JobTable.DELEGATION_PROXY_INFO_FIELD));
        job.setWorkingDirectory(rs.getString(JobTable.WORKING_DIRECTORY_FIELD));

        String leaseId = rs.getString(JobTable.LEASE_ID_FIELD);
        if (leaseId != null) {
            Timestamp leaseTimeExpiredField = rs.getTimestamp(JobTable.LEASE_TIME_FIELD);
            Calendar leaseTimeExpired = null;
            if (leaseTimeExpiredField != null) {
                leaseTimeExpired = Calendar.getInstance();
                leaseTimeExpired.setTimeInMillis(leaseTimeExpiredField.getTime());
            }
            Lease lease = this.getLeaseObj(leaseId, leaseTimeExpired, job.getUserId());
            job.setLease(lease);
        }
        
        String myProxyServer = rs.getString(JobTable.MY_PROXY_SERVER_FIELD);
        if (myProxyServer != null) {
            job.setMyProxyServer(myProxyServer);
        }
        return job;
    }

    private Lease getLeaseObj(String leaseId, Calendar leaseTimeExpired, String userId) {
        Lease lease = new Lease();
        lease.setUserId(userId);
        
        if (leaseTimeExpired != null) {
            lease.setLeaseTime(leaseTimeExpired);
        }
        lease.setLeaseId(leaseId);
        return lease;
    }

    private static String getSelectQuery(String jobId, String userId) {
        StringBuffer selectQuery = new StringBuffer();
        selectQuery.append("select ");
        selectQuery.append(JobTable.ID_FIELD + " AS " + JobTable.ID_FIELD + ", ");
        selectQuery.append(JobTable.CREAM_URL_FIELD + " AS " + JobTable.CREAM_URL_FIELD + ", ");
        selectQuery.append(JobTable.CEREQUIREMENTS_FIELD + " AS " + JobTable.CEREQUIREMENTS_FIELD + ", ");
        selectQuery.append(JobTable.VIRTUAL_ORGANIZATION_FIELD + " AS " + JobTable.VIRTUAL_ORGANIZATION_FIELD + ", ");
        selectQuery.append(JobTable.USER_ID_FIELD + " AS " + JobTable.USER_ID_FIELD + ", ");
        selectQuery.append(JobTable.BATCH_SYSTEM_FIELD + " AS " + JobTable.BATCH_SYSTEM_FIELD + ", ");
        selectQuery.append(JobTable.QUEUE_FIELD + " AS " + JobTable.QUEUE_FIELD + ", ");
        selectQuery.append(JobTable.STANDARD_INPUT_FIELD + " AS " + JobTable.STANDARD_INPUT_FIELD + ", ");
        selectQuery.append(JobTable.STANDARD_OUTPUT_FIELD + " AS " + JobTable.STANDARD_OUTPUT_FIELD + ", ");
        selectQuery.append(JobTable.STANDARD_ERROR_FIELD + " AS " + JobTable.STANDARD_ERROR_FIELD + ", ");
        selectQuery.append(JobTable.EXECUTABLE_FIELD + " AS " + JobTable.EXECUTABLE_FIELD + ", ");
        selectQuery.append(JobTable.DELEGATION_PROXY_CERT_PATH_FIELD + " AS " + JobTable.DELEGATION_PROXY_CERT_PATH_FIELD + ", ");
        selectQuery.append(JobTable.AUTHN_PROXY_CERT_PATH_FIELD + " AS " + JobTable.AUTHN_PROXY_CERT_PATH_FIELD + ", ");
        selectQuery.append(JobTable.HLR_LOCATION_FIELD + " AS " + JobTable.HLR_LOCATION_FIELD + ", ");
        selectQuery.append(JobTable.LOGGER_DEST_URI_FIELD + " AS " + JobTable.LOGGER_DEST_URI_FIELD + ", ");
        selectQuery.append(JobTable.TOKEN_URL_FIELD + " AS " + JobTable.TOKEN_URL_FIELD + ", ");
        selectQuery.append(JobTable.PERUSAL_FILES_DEST_URI_FIELD + " AS " + JobTable.PERUSAL_FILES_DEST_URI_FIELD + ", ");
        selectQuery.append(JobTable.PERUSAL_LIST_FILE_URI_FIELD + " AS " + JobTable.PERUSAL_LIST_FILE_URI_FIELD + ", ");
        selectQuery.append(JobTable.PERUSAL_TIME_INTERVAL_FIELD + " AS " + JobTable.PERUSAL_TIME_INTERVAL_FIELD + ", ");
        selectQuery.append(JobTable.NODES_FIELD + " AS " + JobTable.NODES_FIELD + ", ");
        selectQuery.append(JobTable.PROLOGUE_ARGUMENTS_FIELD + " AS " + JobTable.PROLOGUE_ARGUMENTS_FIELD + ", ");
        selectQuery.append(JobTable.PROLOGUE_FIELD + " AS " + JobTable.PROLOGUE_FIELD + ", ");
        selectQuery.append(JobTable.EPILOGUE_FIELD + " AS " + JobTable.EPILOGUE_FIELD + ", ");
        selectQuery.append(JobTable.EPILOGUE_ARGUMENTS_FIELD + " AS " + JobTable.EPILOGUE_ARGUMENTS_FIELD + ", ");
        selectQuery.append(JobTable.SEQUENCE_CODE_FIELD + " AS " + JobTable.SEQUENCE_CODE_FIELD + ", ");
        selectQuery.append(JobTable.LRMS_JOB_ID_FIELD + " AS " + JobTable.LRMS_JOB_ID_FIELD + ", ");
        selectQuery.append(JobTable.LRMS_ABS_LAYER_JOB_ID_FIELD + " AS " + JobTable.LRMS_ABS_LAYER_JOB_ID_FIELD + ", ");
        selectQuery.append(JobTable.GRID_JOB_ID_FIELD + " AS " + JobTable.GRID_JOB_ID_FIELD + ", ");
        selectQuery.append(JobTable.ICE_ID_FIELD + " AS " + JobTable.ICE_ID_FIELD + ", ");
        selectQuery.append(JobTable.FATHER_JOB_ID_FIELD + " AS " + JobTable.FATHER_JOB_ID_FIELD + ", ");
        selectQuery.append(JobTable.CE_ID_FIELD + " AS " + JobTable.CE_ID_FIELD + ", ");
        selectQuery.append(JobTable.TYPE_FIELD + " AS " + JobTable.TYPE_FIELD + ", ");
        selectQuery.append(JobTable.CREAM_INPUT_SANDBOX_URI_FIELD + " AS " + JobTable.CREAM_INPUT_SANDBOX_URI_FIELD + ", ");
        selectQuery.append(JobTable.CREAM_OUTPUT_SANDBOX_URI_FIELD + " AS " + JobTable.CREAM_OUTPUT_SANDBOX_URI_FIELD + ", ");
        selectQuery.append(JobTable.SANDBOX_BASE_PATH_FIELD + " AS " + JobTable.SANDBOX_BASE_PATH_FIELD + ", ");
        selectQuery.append(JobTable.INPUT_SANDBOX_BASE_URI_FIELD + " AS " + JobTable.INPUT_SANDBOX_BASE_URI_FIELD + ", ");
        selectQuery.append(JobTable.OUTPUT_SANDBOX_BASE_DEST_URI_FIELD + " AS " + JobTable.OUTPUT_SANDBOX_BASE_DEST_URI_FIELD + ", ");
        selectQuery.append(JobTable.WORKER_NODE_FIELD + " AS " + JobTable.WORKER_NODE_FIELD + ", ");
        selectQuery.append(JobTable.JDL_FIELD + " AS " + JobTable.JDL_FIELD + ", ");
        selectQuery.append(JobTable.LOCAL_USER_FIELD + " AS " + JobTable.LOCAL_USER_FIELD + ", ");
        selectQuery.append(JobTable.DELEGATION_PROXY_ID_FIELD + " AS " + JobTable.DELEGATION_PROXY_ID_FIELD + ", ");
        selectQuery.append(JobTable.DELEGATION_PROXY_INFO_FIELD + " AS " + JobTable.DELEGATION_PROXY_INFO_FIELD + ", ");
        selectQuery.append(JobTable.WORKING_DIRECTORY_FIELD + " AS " + JobTable.WORKING_DIRECTORY_FIELD + ", ");
        selectQuery.append(JobTable.LEASE_ID_FIELD + " AS " + JobTable.LEASE_ID_FIELD + ", ");
        selectQuery.append(JobTable.LEASE_TIME_FIELD + " AS " + JobTable.LEASE_TIME_FIELD + ", ");
        selectQuery.append(JobTable.MY_PROXY_SERVER_FIELD + " AS " + JobTable.MY_PROXY_SERVER_FIELD);
        selectQuery.append(" from " + JobTable.NAME_TABLE);
        selectQuery.append(" where ");
        selectQuery.append(JobTable.ID_FIELD + "=?");
        
        if (userId != null) {
            selectQuery.append(" and " + JobTable.USER_ID_FIELD + "?");
        }
        
        return selectQuery.toString();
    }

    public List<String> executeSelectToRetrieveJobId(String userId, List<String> jobId, String leaseId, String delegationId, List<String> gridJobId, Connection connection) throws SQLException {
        logger.debug("Begin executeSelectToRetrieveJobId");
        List<String> jobIdList = new ArrayList<String>();
        PreparedStatement selectToRetrieveJobIdPreparedStatement = null;
        try {
            int jobIdListSize = 0;
            if (jobId != null) {
                jobIdListSize = jobId.size();
            }
            
            int gridJobIdListSize = 0;
            if (gridJobId != null) {
                gridJobIdListSize = gridJobId.size();
            }
            
            selectToRetrieveJobIdPreparedStatement = connection.prepareStatement(getSelectToRetrieveJobIdQuery(userId, leaseId, delegationId, jobIdListSize, gridJobIdListSize));

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

            if (jobId != null) {
                for (String id : jobId) {
                    selectToRetrieveJobIdPreparedStatement.setString(++index, id);
                }
            }
            
            if (gridJobId != null) {
                for (String id : gridJobId) {
                    selectToRetrieveJobIdPreparedStatement.setString(++index, id);
                }
            }
            
            // execute query, and return number of rows created
            ResultSet rs = selectToRetrieveJobIdPreparedStatement.executeQuery();
            if (rs != null) {
                while (rs.next()) {
                    jobIdList.add(rs.getString(JobTable.ID_FIELD));
                }
            }
        } catch (SQLException sqle) {
            throw sqle;
        } finally {
            if (selectToRetrieveJobIdPreparedStatement != null) {
                try {
                    selectToRetrieveJobIdPreparedStatement.close();
                } catch (SQLException sqle1) {
                    logger.error(sqle1);
                }
            }
        }
        logger.debug("End executeSelectToRetrieveJobId");
        return jobIdList;
    }

    private static String getSelectToRetrieveJobIdQuery(String userId, String leaseId, String delegationId, int jobIdListSize, int gridJobIdListSize) {
        StringBuffer selectToRetrieveJobIdQuery = new StringBuffer();
        selectToRetrieveJobIdQuery.append("select ");
        selectToRetrieveJobIdQuery.append(JobTable.ID_FIELD + " AS " + JobTable.ID_FIELD);
        selectToRetrieveJobIdQuery.append(" from " + JobTable.NAME_TABLE);
        // trick
        selectToRetrieveJobIdQuery.append(" where true ");

        if ((userId != null) && (delegationId != null)) {
            selectToRetrieveJobIdQuery.append(" and (");
            selectToRetrieveJobIdQuery.append("(" + JobTable.ICE_ID_FIELD + " IS NOT NULL and ");
            selectToRetrieveJobIdQuery.append(JobTable.MY_PROXY_SERVER_FIELD + " IS NOT NULL) or (");
            selectToRetrieveJobIdQuery.append(JobTable.ICE_ID_FIELD + " IS  NULL))");
        }

        if (userId != null) {
            selectToRetrieveJobIdQuery.append(" and " + JobTable.USER_ID_FIELD + "=?");
        }

        if (delegationId != null) {
            selectToRetrieveJobIdQuery.append(" and " + JobTable.DELEGATION_PROXY_ID_FIELD + "=?");
        }

        if (leaseId != null) {
            selectToRetrieveJobIdQuery.append(" and " + JobTable.LEASE_ID_FIELD + "=?");
        }

        if (jobIdListSize > 0) {
            StringBuffer jobIdList = new StringBuffer(0);
            for (int i = 0; i < jobIdListSize; i++) {
                jobIdList.append(", ?");
            }
            jobIdList.deleteCharAt(0);
            selectToRetrieveJobIdQuery.append(" and " + JobTable.ID_FIELD + " IN (" + jobIdList.toString() + ")");
        }

        if (gridJobIdListSize > 0) {
            StringBuffer gridJobIdList = new StringBuffer(0);
            for (int i = 0; i < gridJobIdListSize; i++) {
                gridJobIdList.append(", ?");
            }
            gridJobIdList.deleteCharAt(0);
            selectToRetrieveJobIdQuery.append(" and " + JobTable.GRID_JOB_ID_FIELD + " IN (" + gridJobIdList.toString() + ")");
        }
        // logger.debug("selectToRetrieveJobIdQuery = " +
        // selectToRetrieveJobIdQuery.toString());
        return selectToRetrieveJobIdQuery.toString();
    }

    private static String getDeleteQuery() {
        StringBuffer deleteQuery = new StringBuffer();
        deleteQuery.append("delete from ");
        deleteQuery.append(JobTable.NAME_TABLE);
        deleteQuery.append(" where ");
        deleteQuery.append(JobTable.ID_FIELD + " = ?");
        return deleteQuery.toString();
    }

    private static String getSelectUserIdQuery() {
        StringBuffer selectUserIdQuery = new StringBuffer();
        selectUserIdQuery.append("select " + JobTable.USER_ID_FIELD);
        selectUserIdQuery.append(" from ");
        selectUserIdQuery.append(JobTable.NAME_TABLE);
        selectUserIdQuery.append(" where ");
        selectUserIdQuery.append(JobTable.ID_FIELD + " = ?");
        return selectUserIdQuery.toString();
    }

    private static String getInsertQuery() {
        StringBuffer insertQuery = new StringBuffer();
        insertQuery.append("insert into ");
        insertQuery.append(JobTable.NAME_TABLE);
        insertQuery.append(" ( ");
        insertQuery.append(JobTable.ID_FIELD + ", ");
        insertQuery.append(JobTable.CREAM_URL_FIELD + ", ");
        insertQuery.append(JobTable.CEREQUIREMENTS_FIELD + ", ");
        insertQuery.append(JobTable.VIRTUAL_ORGANIZATION_FIELD + ", ");
        insertQuery.append(JobTable.USER_ID_FIELD + ", ");
        insertQuery.append(JobTable.BATCH_SYSTEM_FIELD + ", ");
        insertQuery.append(JobTable.QUEUE_FIELD + ", ");
        insertQuery.append(JobTable.STANDARD_INPUT_FIELD + ", ");
        insertQuery.append(JobTable.STANDARD_OUTPUT_FIELD + ", ");
        insertQuery.append(JobTable.STANDARD_ERROR_FIELD + ", ");
        insertQuery.append(JobTable.EXECUTABLE_FIELD + ", ");
        insertQuery.append(JobTable.DELEGATION_PROXY_CERT_PATH_FIELD + ", ");
        insertQuery.append(JobTable.AUTHN_PROXY_CERT_PATH_FIELD + ", ");
        insertQuery.append(JobTable.HLR_LOCATION_FIELD + ", ");
        insertQuery.append(JobTable.LOGGER_DEST_URI_FIELD + ", ");
        insertQuery.append(JobTable.TOKEN_URL_FIELD + ", ");
        insertQuery.append(JobTable.PERUSAL_FILES_DEST_URI_FIELD + ", ");
        insertQuery.append(JobTable.PERUSAL_LIST_FILE_URI_FIELD + ", ");
        insertQuery.append(JobTable.PERUSAL_TIME_INTERVAL_FIELD + ", ");
        insertQuery.append(JobTable.NODES_FIELD + ", ");
        insertQuery.append(JobTable.PROLOGUE_ARGUMENTS_FIELD + ", ");
        insertQuery.append(JobTable.PROLOGUE_FIELD + ", ");
        insertQuery.append(JobTable.EPILOGUE_FIELD + ", ");
        insertQuery.append(JobTable.EPILOGUE_ARGUMENTS_FIELD + ", ");
        insertQuery.append(JobTable.SEQUENCE_CODE_FIELD + ", ");
        insertQuery.append(JobTable.LRMS_JOB_ID_FIELD + ", ");
        insertQuery.append(JobTable.LRMS_ABS_LAYER_JOB_ID_FIELD + ", ");
        insertQuery.append(JobTable.GRID_JOB_ID_FIELD + ", ");
        insertQuery.append(JobTable.ICE_ID_FIELD + ", ");
        insertQuery.append(JobTable.FATHER_JOB_ID_FIELD + ", ");
        insertQuery.append(JobTable.CE_ID_FIELD + ", ");
        insertQuery.append(JobTable.TYPE_FIELD + ", ");
        insertQuery.append(JobTable.CREAM_INPUT_SANDBOX_URI_FIELD + ", ");
        insertQuery.append(JobTable.CREAM_OUTPUT_SANDBOX_URI_FIELD + ", ");
        insertQuery.append(JobTable.SANDBOX_BASE_PATH_FIELD + ", ");
        insertQuery.append(JobTable.INPUT_SANDBOX_BASE_URI_FIELD + ", ");
        insertQuery.append(JobTable.OUTPUT_SANDBOX_BASE_DEST_URI_FIELD + ", ");
        insertQuery.append(JobTable.WORKER_NODE_FIELD + ", ");
        insertQuery.append(JobTable.JDL_FIELD + ", ");
        insertQuery.append(JobTable.LOCAL_USER_FIELD + ", ");
        insertQuery.append(JobTable.DELEGATION_PROXY_ID_FIELD + ", ");
        insertQuery.append(JobTable.DELEGATION_PROXY_INFO_FIELD + ", ");
        insertQuery.append(JobTable.WORKING_DIRECTORY_FIELD + ", ");
        insertQuery.append(JobTable.LEASE_ID_FIELD + ", ");
        insertQuery.append(JobTable.LEASE_TIME_FIELD + ", ");
        insertQuery.append(JobTable.MY_PROXY_SERVER_FIELD);
        insertQuery.append(" ) ");
        insertQuery.append("values(");
        insertQuery.append(" ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,");
        insertQuery.append(" ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,");
        insertQuery.append(" ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,");
        insertQuery.append(" ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,");
        insertQuery.append(" ?, ?, ?, ?, ?, ?)");
        return insertQuery.toString();
    }

    private static String getUpdateQuery(Job job) {
        StringBuffer updateQuery = new StringBuffer();
        updateQuery.append("update ");
        updateQuery.append(JobTable.NAME_TABLE);
        updateQuery.append(" set ");
        updateQuery.append(JobTable.CREAM_URL_FIELD + " = ?, ");
        updateQuery.append(JobTable.CEREQUIREMENTS_FIELD + " = ?, ");
        updateQuery.append(JobTable.BATCH_SYSTEM_FIELD + " = ?, ");
        updateQuery.append(JobTable.QUEUE_FIELD + " = ?, ");
        updateQuery.append(JobTable.STANDARD_INPUT_FIELD + " = ?, ");
        updateQuery.append(JobTable.STANDARD_OUTPUT_FIELD + " = ?, ");
        updateQuery.append(JobTable.STANDARD_ERROR_FIELD + " = ?, ");
        updateQuery.append(JobTable.EXECUTABLE_FIELD + " = ?, ");
        updateQuery.append(JobTable.DELEGATION_PROXY_CERT_PATH_FIELD + " = ?, ");
        updateQuery.append(JobTable.AUTHN_PROXY_CERT_PATH_FIELD + " = ?, ");
        updateQuery.append(JobTable.HLR_LOCATION_FIELD + " = ?, ");
        updateQuery.append(JobTable.LOGGER_DEST_URI_FIELD + " = ?, ");
        updateQuery.append(JobTable.TOKEN_URL_FIELD + " = ?, ");
        updateQuery.append(JobTable.PERUSAL_FILES_DEST_URI_FIELD + " = ?, ");
        updateQuery.append(JobTable.PERUSAL_LIST_FILE_URI_FIELD + " = ?, ");
        updateQuery.append(JobTable.PERUSAL_TIME_INTERVAL_FIELD + " = ?, ");
        updateQuery.append(JobTable.NODES_FIELD + " = ?, ");
        updateQuery.append(JobTable.PROLOGUE_ARGUMENTS_FIELD + " = ?, ");
        updateQuery.append(JobTable.PROLOGUE_FIELD + " = ?, ");
        updateQuery.append(JobTable.EPILOGUE_FIELD + " = ?, ");
        updateQuery.append(JobTable.EPILOGUE_ARGUMENTS_FIELD + " = ?, ");
        updateQuery.append(JobTable.SEQUENCE_CODE_FIELD + " = ?, ");
        updateQuery.append(JobTable.ICE_ID_FIELD + " = ?, ");
        updateQuery.append(JobTable.CE_ID_FIELD + " = ?, ");
        updateQuery.append(JobTable.TYPE_FIELD + " = ?, ");
        updateQuery.append(JobTable.CREAM_INPUT_SANDBOX_URI_FIELD + " = ?, ");
        updateQuery.append(JobTable.CREAM_OUTPUT_SANDBOX_URI_FIELD + " = ?, ");
        updateQuery.append(JobTable.SANDBOX_BASE_PATH_FIELD + " = ?, ");
        updateQuery.append(JobTable.INPUT_SANDBOX_BASE_URI_FIELD + " = ?, ");
        updateQuery.append(JobTable.OUTPUT_SANDBOX_BASE_DEST_URI_FIELD + " = ?, ");
        updateQuery.append(JobTable.WORKER_NODE_FIELD + " = ?, ");
        updateQuery.append(JobTable.JDL_FIELD + " = ?, ");
        updateQuery.append(JobTable.LOCAL_USER_FIELD + " = ?, ");
        updateQuery.append(JobTable.DELEGATION_PROXY_ID_FIELD + " = ?, ");
        updateQuery.append(JobTable.DELEGATION_PROXY_INFO_FIELD + " = ?, ");
        updateQuery.append(JobTable.WORKING_DIRECTORY_FIELD + " = ?  ");
        
        if ((job.getLRMSJobId() != null) && (!Job.NOT_AVAILABLE_VALUE.equals(job.getLRMSJobId()))) {
            updateQuery.append(", ");
            updateQuery.append(JobTable.LRMS_JOB_ID_FIELD + " = ?  ");
        }
        
        if ((job.getLRMSAbsLayerJobId() != null) && (!Job.NOT_AVAILABLE_VALUE.equals(job.getLRMSAbsLayerJobId()))) {
            updateQuery.append(", ");
            updateQuery.append(JobTable.LRMS_ABS_LAYER_JOB_ID_FIELD + " = ? ");
        }
        
        updateQuery.append(" where ");
        updateQuery.append(JobTable.ID_FIELD + " = ? ");
        return updateQuery.toString();
    }

    private static String getUpdateLeaseExpiredQuery(Calendar leaseTimeExpired, String userId, String leaseId, String jobId) throws SQLException {
        StringBuffer updateQuery = new StringBuffer();
        Timestamp leaseTimestampField = null;

        if (userId == null && leaseId == null && jobId == null) {
            throw new SQLException("invalid parameters");
        }

        updateQuery.append("update  ");
        updateQuery.append(JobTable.NAME_TABLE);
        updateQuery.append(" set ");
        updateQuery.append(JobTable.LEASE_TIME_FIELD + "=? where true");

        if (userId != null) {
            updateQuery.append(" and " + JobTable.USER_ID_FIELD + "=?");
        }

        if (leaseId != null) {
            updateQuery.append(" and " + JobTable.LEASE_ID_FIELD + "=?");
        }

        if (jobId != null) {
            updateQuery.append(" and " + JobTable.ID_FIELD + "=?");
        }

        return updateQuery.toString();
    }

    public void setLeaseExpired(String jobId, Lease jobLease, Connection connection) throws SQLException {
        logger.debug("Begin setLeaseExpired");
        // String expiredLeaseIdString = null;
        // expiredLeaseIdString = getExpiredLeaseIdString(jobLease);
        PreparedStatement updateLeaseExpiredPreparedStatement = null;
        int rowCount = 0;

        try {
            Calendar leaseTimeExpired = jobLease.getLeaseTime();
            updateLeaseExpiredPreparedStatement = connection.prepareStatement(getUpdateLeaseExpiredQuery(leaseTimeExpired, jobLease.getUserId(), jobLease.getLeaseId(), jobId));

            int index = 0;

            if (leaseTimeExpired != null) {
                updateLeaseExpiredPreparedStatement.setTimestamp(++index, new java.sql.Timestamp(leaseTimeExpired.getTimeInMillis()));
            }
            
            if (jobLease.getUserId() != null) {
                updateLeaseExpiredPreparedStatement.setString(++index, jobLease.getUserId());
            }

            if (jobLease.getLeaseId() != null) {
                updateLeaseExpiredPreparedStatement.setString(++index, jobLease.getLeaseId());
            }

            if (jobId != null) {
                updateLeaseExpiredPreparedStatement.setString(++index, jobId);
            }

            // execute query, and return number of rows created
            rowCount = updateLeaseExpiredPreparedStatement.executeUpdate();

            logger.debug("LeaseExpired updated.  (rowCount = " + rowCount + ")");
        } catch (SQLException sqle) {
            throw sqle;
        } finally {
            if (updateLeaseExpiredPreparedStatement != null) {
                try {
                    updateLeaseExpiredPreparedStatement.close();
                } catch (SQLException sqle1) {
                    logger.error(sqle1);
                }
            }
        }
        logger.debug("End setLeaseExpired");
    }

    public String getLeaseId(String jobId, String userId, Connection connection) throws SQLException {
        logger.debug("Begin getLeaseId");
        String leaseId = null;
        PreparedStatement selectToRetrieveLeaseIdPreparedStatement = null;
        try {
            int index = 0;
            
            selectToRetrieveLeaseIdPreparedStatement = connection.prepareStatement(getSelectToRetrieveLeaseIdQuery(jobId, userId));
            
            if (jobId != null) {                
                selectToRetrieveLeaseIdPreparedStatement.setString(++index, jobId);
            }
            
            if (userId != null) {                
                selectToRetrieveLeaseIdPreparedStatement.setString(++index, userId);
            }
            
            // execute query, and return number of rows created
            ResultSet rs = selectToRetrieveLeaseIdPreparedStatement.executeQuery();
            if ((rs != null) && (rs.next())) {
                leaseId = rs.getString(JobTable.LEASE_ID_FIELD);
            }
        } catch (SQLException sqle) {
            throw sqle;
        } finally {
            if (selectToRetrieveLeaseIdPreparedStatement != null) {
                try {
                    selectToRetrieveLeaseIdPreparedStatement.close();
                } catch (SQLException sqle1) {
                    logger.error(sqle1);
                }
            }
        }
        logger.debug("End getLeaseId");
        return leaseId;
    }

    public String getReasonFaultSetLeaseId(String jobId, String userId, Connection connection) throws SQLException {
        logger.debug("Begin getReasonFaultSetLeaseId");
        String faultReason = null;
        PreparedStatement selectToRetrieveReasonFaultSetLeaseIdPreparedStatement = null;
        try {
            int index = 0;
        
            selectToRetrieveReasonFaultSetLeaseIdPreparedStatement = connection.prepareStatement(getSelectToRetrieveReasonFaultSetLeaseIdQuery(jobId, userId));

            if (jobId != null) {                
                selectToRetrieveReasonFaultSetLeaseIdPreparedStatement.setString(++index, jobId);
            }
            
            if (userId != null) {                
                selectToRetrieveReasonFaultSetLeaseIdPreparedStatement.setString(++index, userId);
            }

            // execute query, and return number of rows created
            ResultSet rs = selectToRetrieveReasonFaultSetLeaseIdPreparedStatement.executeQuery();
            if ((rs != null) && (rs.next())) {
                faultReason = JobTable.LEASEID_EXPIRED;
            } else {
                faultReason = JobTable.JOB_NOT_FOUND;
            }
            logger.debug("faultReason = " + faultReason);
        } catch (SQLException sqle) {
            throw sqle;
        } finally {
            if (selectToRetrieveReasonFaultSetLeaseIdPreparedStatement != null) {
                try {
                    selectToRetrieveReasonFaultSetLeaseIdPreparedStatement.close();
                } catch (SQLException sqle1) {
                    logger.error(sqle1);
                }
            }
        }
        
        logger.debug("End getReasonFaultSetLeaseId");
        return faultReason;
    }

    private static String getSelectToRetrieveLeaseIdQuery(String jobId, String userId) {
        StringBuffer selectToRetrieveLeaseId = new StringBuffer();
        selectToRetrieveLeaseId.append("select " + JobTable.LEASE_ID_FIELD + " AS " + JobTable.LEASE_ID_FIELD);
        selectToRetrieveLeaseId.append(" from " + JobTable.NAME_TABLE);
        selectToRetrieveLeaseId.append(" where true");

        if (jobId != null) {
            selectToRetrieveLeaseId.append(" and " + JobTable.ID_FIELD + "=?");
        }
        
        if (userId != null) {
            selectToRetrieveLeaseId.append(" and " + JobTable.USER_ID_FIELD + "=?");
        }
        
        return selectToRetrieveLeaseId.toString();
    }

    private static String getSelectToRetrieveReasonFaultSetLeaseIdQuery(String jobId, String userId) {
        StringBuffer selectToRetrieveReasonFaultSetLeaseId = new StringBuffer();
        selectToRetrieveReasonFaultSetLeaseId.append("select " + JobTable.LEASE_TIME_FIELD + " AS " + JobTable.LEASE_TIME_FIELD);
        selectToRetrieveReasonFaultSetLeaseId.append(" from " + JobTable.NAME_TABLE);
        selectToRetrieveReasonFaultSetLeaseId.append(" where true");

        if (jobId != null) {
            selectToRetrieveReasonFaultSetLeaseId.append(" and " + JobTable.ID_FIELD + "=?");
        }
        
        if (userId != null) {
            selectToRetrieveReasonFaultSetLeaseId.append(" and " + JobTable.USER_ID_FIELD + "=?");
        }
                
        return selectToRetrieveReasonFaultSetLeaseId.toString();
    }
}
