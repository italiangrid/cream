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

package org.glite.ce.cream.jobmanagement.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;

import org.apache.log4j.Logger;
import org.glite.ce.commonj.db.DatabaseException;
import org.glite.ce.commonj.db.DatasourceManager;
import org.glite.ce.cream.jobmanagement.db.table.ArgumentTable;
import org.glite.ce.cream.jobmanagement.db.table.EnviromentTable;
import org.glite.ce.cream.jobmanagement.db.table.ExtraAttributeTable;
import org.glite.ce.cream.jobmanagement.db.table.InputFileTable;
import org.glite.ce.cream.jobmanagement.db.table.JobChildTable;
import org.glite.ce.cream.jobmanagement.db.table.JobCommandTable;
import org.glite.ce.cream.jobmanagement.db.table.JobStatusTable;
import org.glite.ce.cream.jobmanagement.db.table.JobTable;
import org.glite.ce.cream.jobmanagement.db.table.JobTableJobStatusTable;
import org.glite.ce.cream.jobmanagement.db.table.JobTableJobStatusTableJobCommandTable;
import org.glite.ce.cream.jobmanagement.db.table.LeaseTable;
import org.glite.ce.cream.jobmanagement.db.table.LeaseTableJobTable;
import org.glite.ce.cream.jobmanagement.db.table.LeaseTableJobTableJobStatusTable;
import org.glite.ce.cream.jobmanagement.db.table.OutputFileTable;
import org.glite.ce.cream.jobmanagement.db.table.OutputSandboxDestURITable;
import org.glite.ce.creamapi.eventmanagement.Event;
import org.glite.ce.creamapi.jobmanagement.Job;
import org.glite.ce.creamapi.jobmanagement.JobStatus;
import org.glite.ce.creamapi.jobmanagement.Lease;
import org.glite.ce.creamapi.jobmanagement.command.JobCommand;
import org.glite.ce.creamapi.jobmanagement.db.table.ArgumentTableInterface;
import org.glite.ce.creamapi.jobmanagement.db.table.EnviromentTableInterface;
import org.glite.ce.creamapi.jobmanagement.db.table.ExtraAttributeTableInterface;
import org.glite.ce.creamapi.jobmanagement.db.table.InputFileTableInterface;
import org.glite.ce.creamapi.jobmanagement.db.table.JobChildTableInterface;
import org.glite.ce.creamapi.jobmanagement.db.table.JobCommandTableInterface;
import org.glite.ce.creamapi.jobmanagement.db.table.JobStatusTableInterface;
import org.glite.ce.creamapi.jobmanagement.db.table.JobTableInterface;
import org.glite.ce.creamapi.jobmanagement.db.table.JobTableJobStatusInterface;
import org.glite.ce.creamapi.jobmanagement.db.table.JobTableJobStatusJobCommandInterface;
import org.glite.ce.creamapi.jobmanagement.db.table.LeaseTableInterface;
import org.glite.ce.creamapi.jobmanagement.db.table.LeaseTableJobTableInterface;
import org.glite.ce.creamapi.jobmanagement.db.table.LeaseTableJobTableJobStatusInterface;
import org.glite.ce.creamapi.jobmanagement.db.table.OutputFileTableInterface;
import org.glite.ce.creamapi.jobmanagement.db.table.OutputSandboxDestURITableInterface;

public class JobDBManager {
    private static final Logger logger = Logger.getLogger(JobDBManager.class.getName());

    private JobTableInterface jobTable = null;
    private JobStatusTableInterface jobStatusTable = null;
    private JobTableJobStatusInterface jobTableJobStatusTable = null;
    private JobTableJobStatusJobCommandInterface jobTableJobStatusJobCommandTable = null;
    private JobCommandTableInterface jobCommandTable = null;
    private EnviromentTableInterface environmentTable = null;
    private ExtraAttributeTableInterface extraAttributeTable = null;
    private ArgumentTableInterface argumentTable = null;
    private JobChildTableInterface jobChildTable = null;
    private InputFileTableInterface inputFileTable = null;
    private OutputFileTableInterface outputFileTable = null;
    private OutputSandboxDestURITableInterface outputSandboxDestURITable = null;
    private LeaseTableInterface leaseTable = null;
    private LeaseTableJobTableJobStatusInterface leaseTableJobTableJobStatus = null;
    private LeaseTableJobTableInterface leaseTableJobTable = null;
    private String datasourceName = null;

    public JobDBManager(String datasourceName) throws IllegalArgumentException, DatabaseException {
        if (datasourceName == null) {
            throw new IllegalArgumentException("datasourceName not specified!");
        }

        try {
            this.datasourceName = datasourceName;
            this.jobTable = new JobTable();
            this.jobStatusTable = new JobStatusTable();
            this.jobTableJobStatusTable = new JobTableJobStatusTable();
            this.jobTableJobStatusJobCommandTable = new JobTableJobStatusTableJobCommandTable();
            this.jobCommandTable = new JobCommandTable();
            this.environmentTable = new EnviromentTable();
            this.extraAttributeTable = new ExtraAttributeTable();
            this.argumentTable = new ArgumentTable();
            this.jobChildTable = new JobChildTable();
            this.inputFileTable = new InputFileTable();
            this.outputFileTable = new OutputFileTable();
            this.outputSandboxDestURITable = new OutputSandboxDestURITable();
            this.leaseTable = new LeaseTable();
            this.leaseTableJobTableJobStatus = new LeaseTableJobTableJobStatusTable();
            this.leaseTableJobTable = new LeaseTableJobTable();
        } catch (SQLException e) {
            throw new DatabaseException(e.getMessage());
        }
    }

    public void deleteJob(String jobId, String userId) throws DatabaseException {
        logger.debug("Begin deleteJob");

        Connection connection = DatasourceManager.getConnection(this.datasourceName);

        try {
            if (!jobTable.isUserEnable(jobId, userId, connection)) {
                throw new IllegalArgumentException("UserId = " + userId + " is not enable for that operation!");
            }
            jobTable.executeDelete(jobId, connection);

            connection.commit();
            logger.debug("job deleted");
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException sqle) {
                throw new DatabaseException("Rollback is fault. ERRORCODE = " + sqle.getErrorCode() + " Message =  " + sqle.getMessage());
            }
            throw new DatabaseException("Rollback executed due to: " + e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }

        logger.debug("End deleteJob");
    }

    public void deleteJobLease(String leaseId, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("Begin deleteJobLease");

        Connection connection = DatasourceManager.getConnection(this.datasourceName);

        try {
            leaseTable.executeDelete(leaseId, userId, connection);
            connection.commit();
        } catch (SQLException sqle) {
            logger.error(sqle.getMessage());
            throw new DatabaseException(sqle.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }

        logger.debug("End deleteJobLease");
    }

    public List<Lease> executeSelectJobLease(String leaseId, String userId, Calendar leaseTime) throws DatabaseException, IllegalArgumentException {
        logger.debug("Begin executeSelectJobLease");

        List<Lease> leaseList = null;
        Connection connection = DatasourceManager.getConnection(this.datasourceName);

        try {
            leaseList = leaseTable.executeSelect(leaseId, userId, leaseTime, connection);
        } catch (SQLException e) {
            logger.error(e.getMessage());
            throw new DatabaseException(e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }

        logger.debug("End executeSelectJobLease");
        return leaseList;
    }

    public long executeSelectToJobCountByStatus(int[] jobStatusType, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("Begin executeSelectToJobCountByStatus");

        long jobCountByStatus = 0;
        Connection connection = DatasourceManager.getConnection(this.datasourceName);

        try {
            jobCountByStatus = jobTableJobStatusTable.executeSelectToJobCountByStatus(jobStatusType, userId, connection);
        } catch (SQLException e) {
            logger.error(e.getMessage());
            throw new DatabaseException(e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }

        logger.debug("End executeSelectToJobCountByStatus");
        return jobCountByStatus;
    }

    public String executeSelectToOlderJobId(int[] jobStatusType, String batchSystem, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("Begin executeSelectToOlderJobId");

        Connection connection = DatasourceManager.getConnection(this.datasourceName);
        String jobId = null;

        try {
            jobId = jobTableJobStatusTable.executeSelectToRetrieveOlderJobIdQuery(jobStatusType, batchSystem, userId, connection);
        } catch (SQLException e) {
            logger.error(e.getMessage());
            throw new DatabaseException(e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }

        logger.debug("End executeSelectToOlderJobId");
        return jobId;
    }

    public List<String> executeSelectToRetrieveJobId(String userId, List<String> jobId, String leaseId, String delegationId, List<String> gridJobId) throws DatabaseException, IllegalArgumentException {
        logger.debug("Begin executeSelectToRetrieveJobId");

        List<String> jobIdList = null;
        Connection connection = DatasourceManager.getConnection(this.datasourceName);

        try {
            jobIdList = jobTable.executeSelectToRetrieveJobId(userId, jobId, leaseId, delegationId, gridJobId, connection);
        } catch (SQLException e) {
            logger.error(e.getMessage());
            throw new DatabaseException(e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }

        logger.debug("End executeSelectToRetrieveJobId");
        return jobIdList;
    }

    public List<String> executeSelectToRetrieveJobId(String userId, String delegationId, int[] jobStatusType, Calendar startStatusDate, Calendar endStatusDate, String leaseId, Calendar registerCommandStartDate,
            Calendar registerCommandEndDate) throws DatabaseException, IllegalArgumentException {
        List<String> jobIdList = null;
        Connection connection = DatasourceManager.getConnection(this.datasourceName);

        try {
            jobIdList = jobTableJobStatusJobCommandTable.executeSelectToRetrieveJobId(userId, delegationId, jobStatusType, leaseId, startStatusDate, endStatusDate, registerCommandStartDate, registerCommandEndDate,
                    connection);
        } catch (SQLException e) {
            logger.error(e.getMessage());
            throw new DatabaseException(e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }

        logger.debug("End executeSelectToRetrieveJobId");
        return jobIdList;
    }

    public List<String> executeSelectToRetrieveJobIdByDate(List<String> jobId, String userId, Calendar startDate, Calendar endDate) throws DatabaseException, IllegalArgumentException {
        logger.debug("Begin executeSelectToRetrieveJobIdByDate");

        List<String> jobIdList = null;
        Connection connection = DatasourceManager.getConnection(this.datasourceName);

        try {
            jobIdList = jobCommandTable.executeSelectToRetrieveJobIdByDate(jobId, userId, startDate, endDate, connection);
        } catch (SQLException e) {
            logger.error(e.getMessage());
            throw new DatabaseException(e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }

        logger.debug("End executeSelectToRetrieveJobIdByDate");
        return jobIdList;
    }

    public List<String> executeSelectToRetrieveJobIdByLease(int[] jobStatusType, String leaseId, Calendar maxLeaseTime, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("Begin executeSelectToRetrieveJobIdByLease");

        List<String> jobIdList = null;
        Connection connection = DatasourceManager.getConnection(this.datasourceName);

        try {
            if (maxLeaseTime == null) {
                jobIdList = jobTableJobStatusTable.executeSelectToRetrieveJobId(userId, null, null, jobStatusType, leaseId, null, null, null, null, connection);
            } else {
                jobIdList = leaseTableJobTableJobStatus.executeSelectToRetrieveJobIdByLease(jobStatusType, leaseId, maxLeaseTime, userId, connection);
            }
        } catch (SQLException e) {
            logger.error(e.getMessage());
            throw new DatabaseException(e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }

        logger.debug("End executeSelectToRetrieveJobIdByLease");
        return jobIdList;
    }

    public List<String> executeSelectToRetrieveJobIdByLeaseTimeExpired(String userId, String delegationId, int[] jobStatusType) throws DatabaseException, IllegalArgumentException {
        logger.debug("Begin executeSelectToRetrieveJobIdByLeaseTimeExpired");

        List<String> jobIdList = null;
        Connection connection = DatasourceManager.getConnection(this.datasourceName);

        try {
            jobIdList = jobTableJobStatusTable.executeSelectToRetrieveJobIdByLeaseTimeExpiredQuery(userId, delegationId, jobStatusType, connection);
        } catch (SQLException e) {
            logger.error(e.getMessage());
            throw new DatabaseException(e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }

        logger.debug("End executeSelectToRetrieveJobIdByLeaseTimeExpired");
        return jobIdList;
    }

    public List<String> executeSelectToRetrieveJobIdByStatus(String userId, List<String> jobId, String delegationId, int[] jobStatusType, Calendar startStatusDate, Calendar endStatusDate, String queueName,
            String batchSystem) throws DatabaseException, IllegalArgumentException {
        List<String> jobIdList = null;
        Connection connection = DatasourceManager.getConnection(this.datasourceName);

        try {
            jobIdList = jobTableJobStatusTable.executeSelectToRetrieveJobId(userId, jobId, delegationId, jobStatusType, null, startStatusDate, endStatusDate, queueName, batchSystem, connection);
        } catch (SQLException e) {
            logger.error(e.getMessage());
            throw new DatabaseException(e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }

        logger.debug("End executeSelectToRetrieveJobIdByStatus");
        return jobIdList;
    }

    public int executeUpdateAllUnterminatedJobCommand() throws DatabaseException {
        logger.debug("Begin executeUpdateAllUnterminatedJobCommand");

        Connection connection = DatasourceManager.getConnection(this.datasourceName);
        int result;

        try {
            result = jobCommandTable.executeUpdateAllUnterminatedJobCommandQuery(JobCommand.ABORTED, new int[] { JobCommand.PROCESSING },
                    "command ABORTED because its execution has been interrupted by the CREAM shutdown.", Calendar.getInstance(), connection);

            connection.commit();
        } catch (SQLException e) {
            logger.error(e.getMessage());
            throw new DatabaseException(e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }

        logger.debug("End executeUpdateAllUnterminatedJobCommand");
        return result;
    }

    public void executeUpdateJobLease(Lease jobLease) throws DatabaseException, IllegalArgumentException {
        logger.debug("Begin executeUpdateJobLease");

        Connection connection = DatasourceManager.getConnection(this.datasourceName);

        try {
            leaseTable.executeUpdate(jobLease, connection);
            connection.commit();
        } catch (SQLException e) {
            logger.error(e.getMessage());
            throw new DatabaseException(e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }

        logger.debug("End executeUpdateJobLease");
    }

    public void insertJob(Job job) throws DatabaseException {
        logger.debug("Begin insertJob");

        Connection connection = null;

        if (job != null) {
            connection = DatasourceManager.getConnection(this.datasourceName);
            try {
                jobTable.executeInsert(job, connection);
                if (job.getLease() != null) {
                    List<Lease> leaseList = leaseTable.executeSelect(job.getLease().getLeaseId(), job.getLease().getUserId(), null, connection);
                    // if not exists
                    if (leaseList.size() == 0) {
                        leaseTable.executeInsert(job.getLease(), connection);
                    }
                }
                if (job.getStatusHistory() != null && job.getStatusHistory().size() > 0) {
                    jobStatusTable.executeInsert(job.getStatusHistory(), connection);
                }
                if (job.getCommandHistory() != null && job.getCommandHistory().size() > 0) {
                    jobCommandTable.executeInsert(job.getCommandHistory(), connection);
                }
                if (job.getEnvironment() != null && job.getEnvironment().size() > 0) {
                    environmentTable.executeInsert(job.getId(), job.getEnvironment(), connection);
                }
                if (job.getExtraAttribute() != null && job.getExtraAttribute().size() > 0) {
                    extraAttributeTable.executeInsert(job.getId(), job.getExtraAttribute(), connection);
                }
                if (job.getArguments() != null && job.getArguments().size() > 0) {
                    argumentTable.executeInsert(job.getId(), job.getArguments(), connection);
                }
                if (job.getChildJobId() != null && job.getChildJobId().size() > 0) {
                    jobChildTable.executeInsert(job.getId(), job.getChildJobId(), connection);
                }
                if (job.getInputFiles() != null && job.getInputFiles().size() > 0) {
                    inputFileTable.executeInsert(job.getId(), job.getInputFiles(), connection);
                }
                if (job.getOutputFiles() != null && job.getOutputFiles().size() > 0) {
                    outputFileTable.executeInsert(job.getId(), job.getOutputFiles(), connection);
                }
                if (job.getOutputSandboxDestURI() != null && job.getOutputSandboxDestURI().size() > 0) {
                    outputSandboxDestURITable.executeInsert(job.getId(), job.getOutputSandboxDestURI(), connection);
                }
                connection.commit();
                logger.debug("Job committed");
            } catch (Exception e) {
                try {
                    connection.rollback();
                } catch (SQLException sqle) {
                    throw new DatabaseException("Rollback is fault. ERRORCODE = " + sqle.getErrorCode() + " Message =  " + sqle.getMessage());
                }
                throw new DatabaseException("Rollback executed due to: " + e.getMessage());
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException sqle) {
                        logger.error(sqle);
                    }
                }
            }
        } // if (job != null)
        logger.debug("End insertJob");
    }

    public void insertJobCommand(JobCommand jobCommand) throws DatabaseException, IllegalArgumentException {
        logger.debug("Begin insertJobCommand");

        Connection connection = DatasourceManager.getConnection(this.datasourceName);

        try {
            if (jobTable.isUserEnable(jobCommand.getJobId(), jobCommand.getUserId(), connection)) {
                List<JobCommand> jobCommandList = new ArrayList<JobCommand>(1);
                jobCommandList.add(jobCommand);
                jobCommandTable.executeInsert(jobCommandList, connection);
                connection.commit();
            } else {
                throw new IllegalArgumentException("UserId = " + jobCommand.getUserId() + " is not enable for that operation!");
            }
        } catch (SQLException e) {
            throw new DatabaseException(e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }

        logger.debug("End insertJobCommand");
    }
    
    public void insertJobCommand(JobCommand jobCommand, String delegationId, int[] jobStatusType) throws DatabaseException, IllegalArgumentException {
        logger.debug("BEGIN insertJobCommand");
        
        Connection connection = DatasourceManager.getConnection(this.datasourceName);

        try {
            jobTableJobStatusJobCommandTable.executeInsert(jobCommand, delegationId, jobStatusType, connection);
            connection.commit();  
        } catch (SQLException e) {
            throw new DatabaseException(e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }
        
        logger.debug("End insertJobCommand");
    }
    
    public void insertJobLease(Lease jobLease) throws DatabaseException, IllegalArgumentException {
        logger.debug("Begin insertJobLease");

        Connection connection = DatasourceManager.getConnection(this.datasourceName);

        try {
            leaseTable.executeInsert(jobLease, connection);
            connection.commit();
        } catch (SQLException sqle) {
            throw new DatabaseException(sqle.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }

        logger.debug("End insertJobLease");
    }

    public void insertStatus(JobStatus status, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("Begin insertStatus");

        Connection connection = DatasourceManager.getConnection(this.datasourceName);

        try {
            if (jobTable.isUserEnable(status.getJobId(), userId, connection)) {
                List<JobStatus> jobStatusList = new ArrayList<JobStatus>();
                jobStatusList.add(status);
                jobStatusTable.executeInsert(jobStatusList, connection);
                connection.commit();
            } else {
                throw new IllegalArgumentException("UserId = " + userId + " is not enable for that operation!");
            }
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException sqle) {
                throw new DatabaseException("Rollback is fault. ERRORCODE = " + sqle.getErrorCode() + " Message =  " + sqle.getMessage());
            }
            throw new DatabaseException("Rollback executed due to: " + e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }

        logger.debug("End insertStatus");
    }

    public Job retrieveJob(String jobId, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("Begin retrieveJob");

        Connection connection = DatasourceManager.getConnection(this.datasourceName);
        Job job = null;

        try {
            List<JobCommand> jobCommandList = null;
            if (jobTable.isUserEnable(jobId, userId, connection)) {
                job = jobTable.executeSelectJobTable(jobId, userId, connection);
                if (job != null) {
                    Lease lease = job.getLease();
                    if (lease != null) {
                        if (lease.getLeaseTime() == null) {
                            List<Lease> leaseList = leaseTable.executeSelect(lease.getLeaseId(), lease.getUserId(), null, connection);
                            if (leaseList.size() != 1) {
                                String msg = "LeaseId = " + lease.getLeaseId() + " and userId = " + lease.getUserId() + " no one entry in " + LeaseTable.NAME_TABLE + " table";
                                logger.error(msg);
                                throw new DatabaseException(msg);
                            } else {
                                job.setLease(leaseList.get(0));
                            }
                        }
                    }
                    job.setStatusHistory(jobStatusTable.executeSelectJobStatusHistory(jobId, connection));
                    job.setInputFiles(inputFileTable.executeSelect(jobId, connection));
                    job.setOutputFiles(outputFileTable.executeSelect(jobId, connection));
                    job.setOutputSandboxDestURI(outputSandboxDestURITable.executeSelect(jobId, connection));
                    job.setEnvironment(environmentTable.executeSelect(jobId, connection));
                    job.setExtraAttribute(extraAttributeTable.executeSelect(jobId, connection));
                    job.setArguments(argumentTable.executeSelect(jobId, connection));
                    job.setChildJobId(jobChildTable.executeSelect(jobId, connection));
                    jobCommandList = jobCommandTable.executeSelectJobCommandHistory(jobId, connection);
                    job.setCommandHistory(jobCommandList);
                    // trick to understand if a job has been deleted.
                    if ((jobCommandList != null) && (jobCommandList.size() == 0)) {
                        logger.debug("Job with jobId = " + job.getId() + " has been deleted.");
                        job = null;
                    }
                }
            } else {
                throw new IllegalArgumentException("UserId = " + userId + " is not enabled for that operation!");
            }
        } catch (SQLException sqle) {
            logger.error(sqle.getMessage());
            throw new DatabaseException(sqle.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }

        logger.debug("End retrieveJob");
        return job;
    }

    public List<JobCommand> retrieveJobCommandHistory(String jobId, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("Begin retrieveJobCommandHistory");

        List<JobCommand> jobCommandList = null;
        Connection connection = DatasourceManager.getConnection(this.datasourceName);

        try {
            if (jobTable.isUserEnable(jobId, userId, connection)) {
                jobCommandList = jobCommandTable.executeSelectJobCommandHistory(jobId, connection);

            } else {
                throw new IllegalArgumentException("UserId = " + userId + " is not enable for that operation!");
            }
        } catch (SQLException e) {
            throw new DatabaseException(e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }

        logger.debug("End retrieveJobStatusHistory");
        return jobCommandList;
    }

    public List<JobStatus> retrieveJobStatus(String fromJobStatusId, String toJobStatusId, Calendar fromDate, Calendar toDate, int maxElements, String iceId, String userId) throws DatabaseException {
        logger.debug("Begin retrieveJobStatus");
        Connection connection = DatasourceManager.getConnection(this.datasourceName);
        List<JobStatus> jobStatusList = null;
        try {
            jobStatusList = jobStatusTable.executeSelectToRetrieveJobStatus(fromJobStatusId, toJobStatusId, fromDate, toDate, maxElements, iceId, userId, connection);
        } catch (SQLException e) {
            logger.error(e.getMessage());
            throw new DatabaseException(e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }
        logger.debug("End retrieveJobStatus");
        return jobStatusList;
    }

    public List<Event> retrieveJobStatusAsEvent(String fromJobStatusId, String toJobStatusId, Calendar fromDate, Calendar toDate, int[] jobStatusType, int maxElements, String iceId, String userId) throws DatabaseException {
        logger.debug("Begin retrieveJobStatusAsEvent");
        Connection connection = DatasourceManager.getConnection(this.datasourceName);
        List<Event> eventList = null;
        try {
            eventList = jobStatusTable.executeSelectToRetrieveJobStatusAsEvent(fromJobStatusId, toJobStatusId, fromDate, toDate, jobStatusType, maxElements, iceId, userId, connection);
        } catch (SQLException e) {
            logger.error(e.getMessage());
            throw new DatabaseException(e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }
        logger.debug("End retrieveJobStatusAsEvent");
        return eventList;
    }

    public List<JobStatus> retrieveJobStatusHistory(String jobId, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("Begin retrieveJobStatusHistory");

        List<JobStatus> jobStatusList = null;
        Connection connection = DatasourceManager.getConnection(this.datasourceName);

        try {
            if (jobTable.isUserEnable(jobId, userId, connection)) {
                jobStatusList = jobStatusTable.executeSelectJobStatusHistory(jobId, connection);
            } else {
                throw new IllegalArgumentException("UserId = " + userId + " is not enable for that operation!");
            }
        } catch (SQLException e) {
            throw new DatabaseException(e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }

        logger.debug("End retrieveJobStatusHistory");
        return jobStatusList;
    }

    public JobCommand retrieveLastJobCommand(String jobId, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("Begin retrieveLastJobCommand");

        JobCommand jobCommand = null;
        Connection connection = DatasourceManager.getConnection(this.datasourceName);

        try {
            if (jobTable.isUserEnable(jobId, userId, connection)) {
                jobCommand = jobCommandTable.executeSelectLastJobCommmand(jobId, connection);

            } else {
                throw new IllegalArgumentException("UserId = " + userId + " is not enable for that operation!");
            }
        } catch (SQLException e) {
            throw new DatabaseException(e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }

        logger.debug("End retrieveLastJobCommand");
        return jobCommand;
    }

    public JobStatus retrieveLastJobStatus(String jobId, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("Begin retrieveLastJobStatus");

        JobStatus jobStatus = null;
        Connection connection = DatasourceManager.getConnection(this.datasourceName);

        try {
            if (jobTable.isUserEnable(jobId, userId, connection)) {
                jobStatus = jobStatusTable.executeSelectLastJobStatus(jobId, connection);
            } else {
                throw new IllegalArgumentException("UserId = " + userId + " is not enable for that operation!");
            }
        } catch (SQLException e) {
            throw new DatabaseException(e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }

        logger.debug("End retrieveLastJobStatus");
        return jobStatus;
    }

    public void setLeaseExpired(Lease jobLease) throws DatabaseException, IllegalArgumentException {
        logger.debug("Begin setLeaseExpired");

        Connection connection = DatasourceManager.getConnection(this.datasourceName);

        try {
            jobTable.setLeaseExpired(null, jobLease, connection);
            connection.commit();
        } catch (SQLException e) {
            logger.error(e.getMessage());
            throw new DatabaseException(e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }

        logger.debug("End setLeaseExpired");
    }

    public void setLeaseExpired(String jobId, Lease jobLease) throws DatabaseException, IllegalArgumentException {
        logger.debug("Begin setLeaseExpired. JobId= " + jobId);

        Connection connection = DatasourceManager.getConnection(this.datasourceName);

        try {
            jobTable.setLeaseExpired(jobId, jobLease, connection);
            connection.commit();
        } catch (SQLException e) {
            logger.error(e.getMessage());
            throw new DatabaseException(e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }

        logger.debug("End setLeaseExpired. JobId= " + jobId);
    }

    public void setLeaseId(String leaseId, String jobId, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("Begin setLeaseId");

        Connection connection = DatasourceManager.getConnection(this.datasourceName);
        int numLeaseIdUpdated = 0;

        try {
            numLeaseIdUpdated = leaseTableJobTable.setLeaseId(leaseId, jobId, userId, connection);
            connection.commit();
            logger.debug("numLeaseIdUpdated = " + numLeaseIdUpdated);
            if (numLeaseIdUpdated == 0) {
                String reasonFaultSetLeaseId = jobTable.getReasonFaultSetLeaseId(jobId, userId, connection);

                if (JobTable.LEASEID_EXPIRED.equals(reasonFaultSetLeaseId)) {
                    logger.warn("LeaseId '" + leaseId + "' not found! (JobId = " + jobId + ")");
                    throw new DatabaseException("LeaseId '" + leaseId + "' not found/or expired!");
                }
                if (JobTable.JOB_NOT_FOUND.equals(reasonFaultSetLeaseId)) {
                    logger.warn("LeaseId cannot be set. JobId = " + jobId + "doesn't exist on the database. ");
                    throw new DatabaseException("LeaseId cannot be set. JobId = " + jobId + "doesn't exist on the database. ");
                } else {
                    logger.warn("LeaseId cannot be set for JobId = " + jobId + ". Error setting LeaseId field on the database. ");
                    throw new DatabaseException("LeaseId cannot be set for JobId = " + jobId + ". Error setting LeaseId field on the database. ");
                }
            }
        } catch (SQLException e) {
            logger.error(e.getMessage());
            throw new DatabaseException(e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }

        logger.debug("End setLeaseId");
    }

    public void updateJob(Job job) throws DatabaseException {
        logger.debug("Begin updateJob");

        Connection connection = null;

        if (job != null) {
            connection = DatasourceManager.getConnection(this.datasourceName);
            try {
                if (!jobTable.isUserEnable(job.getId(), job.getUserId(), connection)) {
                    throw new IllegalArgumentException("UserId = " + job.getUserId() + " is not enable for that operation!");
                }
                jobTable.executeUpdate(job, connection);

                Hashtable<String, String> extraAttribute = job.getExtraAttribute();
                if (extraAttribute != null) {
                    String name = null;
                    String value = null;
                    int rowUpdated = 0;
                    for (Enumeration<String> e = extraAttribute.keys(); e.hasMoreElements();) {
                        name = e.nextElement();
                        value = extraAttribute.get(name);
                        rowUpdated = extraAttributeTable.executeUpdate(job.getId(), name, value, connection);
                        if (rowUpdated == 0) {
                            extraAttributeTable.executeInsert(job.getId(), name, value, connection);
                        }
                    }
                }

                /*
                 * if (job.getStatusHistory() != null &&
                 * job.getStatusHistory().size()>0){
                 * jobStatusTable.executeUpdateStatusHistory(job.getId(),
                 * job.getStatusHistory(), connection); } if
                 * (job.getCommandHistory() != null &&
                 * job.getCommandHistory().size()>0){
                 * jobCommandTable.executeUpdateCommandHistory(job.getId(),
                 * job.getCommandHistory(), connection); }
                 */
                connection.commit();
                logger.debug("Job updated");
            } catch (SQLException e) {
                try {
                    connection.rollback();
                } catch (SQLException sqle) {
                    throw new DatabaseException("Rollback is fault. ERRORCODE = " + sqle.getErrorCode() + " Message =  " + sqle.getMessage());
                }
                throw new DatabaseException("Rollback executed due to: " + e.getMessage());
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException sqle) {
                        logger.error(sqle);
                    }
                }
            }
        }

        logger.debug("End updateJob");
    }

    public void updateJobCommand(JobCommand jobCommand) throws DatabaseException, IllegalArgumentException {
        logger.debug("Begin updateJobCommand");

        Connection connection = DatasourceManager.getConnection(this.datasourceName);

        try {
            if (jobTable.isUserEnable(jobCommand.getJobId(), jobCommand.getUserId(), connection)) {
                List<JobCommand> jobCommandList = new ArrayList<JobCommand>();
                jobCommandList.add(jobCommand);
                jobCommandTable.executeUpdateJobCommand(jobCommand, connection);
                connection.commit();
            } else {
                throw new IllegalArgumentException("UserId = " + jobCommand.getUserId() + " is not enable for that operation!");
            }
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException sqle) {
                throw new DatabaseException("Rollback is fault. ERRORCODE = " + sqle.getErrorCode() + " Message =  " + sqle.getMessage());
            }
            throw new DatabaseException("Rollback executed due to: " + e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }

        logger.debug("End updateJobCommand");
    }

    public void updateStatus(JobStatus status, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("Begin updateLastStatus");

        Connection connection = DatasourceManager.getConnection(this.datasourceName);

        try {
            if (jobTable.isUserEnable(status.getJobId(), userId, connection)) {
                List<JobStatus> jobStatusList = new ArrayList<JobStatus>();
                jobStatusList.add(status);
                jobStatusTable.executeUpdateJobStatus(status, connection);
                connection.commit();
            } else {
                throw new IllegalArgumentException("UserId = " + userId + " is not enable for that operation!");
            }
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException sqle) {
                throw new DatabaseException("Rollback is fault. ERRORCODE = " + sqle.getErrorCode() + " Message =  " + sqle.getMessage());
            }
            throw new DatabaseException("Rollback executed due to: " + e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }

        logger.debug("End updateStatus");
    }

    public void executeUpdateDelegationProxyInfo(String delegationId, String delegationProxyInfo, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("Begin executeUpdateDelegationProxyInfo");
        
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
        
        Connection connection = DatasourceManager.getConnection(this.datasourceName);

        try {
            jobTableJobStatusTable.executeUpdateDelegationProxyInfo(delegationId, delegationProxyInfo, userId, connection);
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException sqle) {
                throw new DatabaseException("Rollback is fault. ERRORCODE = " + sqle.getErrorCode() + " Message =  " + sqle.getMessage());
            }
            throw new DatabaseException("Rollback executed due to: " + e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }

        logger.debug("End executeUpdateDelegationProxyInfo");
    }
}
