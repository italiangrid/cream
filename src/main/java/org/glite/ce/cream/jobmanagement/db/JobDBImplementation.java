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

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.log4j.Logger;
import org.glite.ce.commonj.db.DatabaseException;
import org.glite.ce.creamapi.eventmanagement.Event;
import org.glite.ce.creamapi.jobmanagement.Job;
import org.glite.ce.creamapi.jobmanagement.JobStatus;
import org.glite.ce.creamapi.jobmanagement.Lease;
import org.glite.ce.creamapi.jobmanagement.command.JobCommand;
import org.glite.ce.creamapi.jobmanagement.db.JobDBInterface;

public class JobDBImplementation implements JobDBInterface {
    private static final Logger logger = Logger.getLogger(JobDBImplementation.class.getName());
    private static JobDBImplementation jobDBImplementation = null;
    private JobDBManager jobDBManager = null;
    
    public static JobDBInterface getInstance() throws DatabaseException {
        if(jobDBImplementation == null) {
            jobDBImplementation = new JobDBImplementation();
        }
        
        return jobDBImplementation;
    }
    
    private JobDBImplementation() throws DatabaseException {                        
        try {
            jobDBManager = new JobDBManager(JobDBInterface.JOB_DATASOURCE_NAME);
        } catch (Exception e) {
            throw new DatabaseException(e.getMessage());
        }
    }

    public void delete(String jobId, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("BEGIN delete for: jobId = " + jobId + " userId = " + userId);
        if (jobId == null) {
            logger.error("jobId mustn't be null!");
            throw new IllegalArgumentException("jobId mustn't be null!");
        }
        jobDBManager.deleteJob(jobId, userId);
        logger.debug("END delete for: jobId = " + jobId + " userId = " + userId);
    }

    public void deleteJobLease(String leaseId, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("BEGIN deleteJobLease");
        if (userId == null) {
            logger.error("UserId mustn't be null!");
            throw new IllegalArgumentException("UserId mustn't be null!");
        }
        if ((leaseId == null) || ("".equals(leaseId))) {
            logger.error("LeaseId mustn't be null or empty!");
            throw new IllegalArgumentException("LeaseId mustn't be null or empty!");
        }
        jobDBManager.deleteJobLease(leaseId, userId);
        logger.debug("END deleteJobLease");
    }

    public void insert(Job job) throws DatabaseException, IllegalArgumentException {
        logger.debug("BEGIN insert");
        if (job == null) {
            logger.error("job mustn't be null!");
            throw new IllegalArgumentException("job mustn't be null!");
        }
        jobDBManager.insertJob(job);
        logger.debug("END insert");
    }

    public void insertJobCommand(JobCommand jobCommand) throws DatabaseException, IllegalArgumentException {
        logger.debug("BEGIN insertJobCommand");
        if (jobCommand == null) {
            logger.error("jobCommand mustn't be null!");
            throw new IllegalArgumentException("jobCommand mustn't be null!");
        }
        jobDBManager.insertJobCommand(jobCommand);
        logger.debug("END insertJobCommand");
    }
    
    public void insertJobCommand(JobCommand jobCommand, String delegationId, int[] jobStatusType) throws DatabaseException, IllegalArgumentException {
        logger.debug("BEGIN insertJobCommand");
        
        if (jobCommand == null) {
            logger.error("jobCommand mustn't be null!");
            throw new IllegalArgumentException("jobCommand mustn't be null!");
        }

        if (delegationId == null) {
            logger.error("delegationId mustn't be null!");
            throw new IllegalArgumentException("delegationId mustn't be null!");
        }

        jobDBManager.insertJobCommand(jobCommand, delegationId, jobStatusType);
        
        logger.debug("END insertJobCommand");
    }
    
    public void insertJobLease(Lease jobLease) throws DatabaseException, IllegalArgumentException {
        logger.debug("BEGIN insertJobLease");
        if (jobLease == null) {
            logger.error("jobLease mustn't be null!");
            throw new IllegalArgumentException("jobLease mustn't be null!");
        }
        if (jobLease.getUserId() == null) {
            logger.error("userId mustn't be null!");
            throw new IllegalArgumentException("userId mustn't be null!");
        }
        if ((jobLease.getLeaseId() == null) || ("".equals(jobLease.getLeaseId()))) {
            logger.error("leaseId mustn't be null or empty!");
            throw new IllegalArgumentException("leaseId mustn't be null or empty!");
        }
        if (jobLease.getLeaseTime() == null) {
            logger.error("leaseTime mustn't be null!");
            throw new IllegalArgumentException("leaseTime mustn't be null!");
        }

        jobDBManager.insertJobLease(jobLease);
        logger.debug("END insertJobLease");
    }

    public void insertStatus(JobStatus status, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("BEGIN insertStatus");
        if (status == null) {
            logger.error("status mustn't be null!");
            throw new IllegalArgumentException("status mustn't be null!");
        }
        jobDBManager.insertStatus(status, userId);
        logger.debug("END insertStatus");
    }

    public long jobCountByStatus(int[] jobStatusType, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("BEGIN jobCountByStatus");
        long countByStatus = 0;
        countByStatus = jobDBManager.executeSelectToJobCountByStatus(jobStatusType, userId);
        logger.debug("END jobCountByStatus");
        return countByStatus;
    }

    // jobCommand
    // type = 0
    // and using creationTime
    public List<String> retrieveByDate(List<String> jobId, String userId, Calendar startDate, Calendar endDate) throws DatabaseException, IllegalArgumentException {
        logger.debug("BEGIN retrieveByDate");
        List<String> jobIdList = null;
        jobIdList = jobDBManager.executeSelectToRetrieveJobIdByDate(jobId, userId, startDate, endDate);
        logger.debug("END retrieveByDate");
        return jobIdList;
    }

    public List<JobCommand> retrieveCommandHistory(String jobId, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("BEGIN retrieveCommandHistory");
        if (jobId == null) {
            logger.error("jobId mustn't be null!");
            throw new IllegalArgumentException("jobId mustn't be null!");
        }
        List<JobCommand> jobCommandList = null;
        jobCommandList = jobDBManager.retrieveJobCommandHistory(jobId, userId);
        logger.debug("END retrieveCommandHistory");
        return jobCommandList;
    }

    public Job retrieveJob(String jobId, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("BEGIN retrieveJob");
        if (jobId == null) {
            logger.error("jobId mustn't be null!");
            throw new IllegalArgumentException("jobId mustn't be null!");
        }
        Job job = null;
        job = jobDBManager.retrieveJob(jobId, userId);
        logger.debug("END retrieveJob");
        return job;
    }

    public List<String> retrieveJobId(int[] jobStatusType, String queueName, String batchSystem, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("BEGIN retrieveJobId");
        List<String> jobIdList = null;
        jobIdList = jobDBManager.executeSelectToRetrieveJobIdByStatus(userId, null, null, jobStatusType, null, null, queueName, batchSystem);
        logger.debug("END retrieveJobId");
        return jobIdList;
    }

    public List<String> retrieveJobId(List<String> jobId, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("BEGIN retrieveJobId");
        List<String> jobIdList = null;
        jobIdList = jobDBManager.executeSelectToRetrieveJobId(userId, jobId, null, null, null);
        logger.debug("END retrieveJobId");
        return jobIdList;
    }

    public List<String> retrieveJobId(List<String> jobId, String userId, int[] jobStatusType, Calendar startStatusDate, Calendar endStatusDate) throws DatabaseException, IllegalArgumentException {
        logger.debug("BEGIN retrieveJobId");
        List<String> jobIdList = null;
        jobIdList = jobDBManager.executeSelectToRetrieveJobIdByStatus(userId, jobId, null, jobStatusType, startStatusDate, endStatusDate, null , null);
        logger.debug("END retrieveJobId");
        return jobIdList;
    }

    public List<String> retrieveJobId(List<String> jobId, String delegId, String leaseId, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("BEGIN retrieveJobId");
        List<String> jobIdList = null;
        jobIdList = jobDBManager.executeSelectToRetrieveJobId(userId, jobId, leaseId, delegId, null);
        logger.debug("END retrieveJobId");
        return jobIdList;
    }

    public List<String> retrieveJobId(String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("BEGIN retrieveJobId");
        List<String> jobIdList = null;
        jobIdList = jobDBManager.executeSelectToRetrieveJobId(userId, null, null, null, null);
        logger.debug("END retrieveJobId");
        return jobIdList;
    }

    public List<String> retrieveJobId(String delegId, int[] jobStatusType, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("BEGIN retrieveJobId");
        List<String> jobIdList = null;
        jobIdList = jobDBManager.executeSelectToRetrieveJobIdByStatus(userId, null, delegId, jobStatusType, null, null, null, null);
        logger.debug("END retrieveJobId");
        return jobIdList;
    }

    public List<String> retrieveJobId(String delegId, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("BEGIN retrieveJobId");
        List<String> jobIdList = null;
        jobIdList = jobDBManager.executeSelectToRetrieveJobId(userId, null, null, delegId, null);
        logger.debug("END retrieveJobId");
        return jobIdList;
    }

    
    public List<String> retrieveJobId(String userId, String delegationId, int[] jobStatusType, Calendar startStatusDate, 
    		Calendar endStatusDate, String leaseId, Calendar registerCommandStartDate, Calendar registerCommandEndDate) 
    		throws DatabaseException, IllegalArgumentException{
    	logger.debug("BEGIN retrieveJobId");
        List<String> jobIdList = null;
        jobIdList = jobDBManager.executeSelectToRetrieveJobId(userId, delegationId, jobStatusType, startStatusDate, endStatusDate, leaseId, registerCommandStartDate, registerCommandEndDate);
        logger.debug("END retrieveJobId");
        return jobIdList;
    }
    
    
    public List<String> retrieveJobIdByGridJobId(List<String> gridJobId, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("BEGIN retrieveJobIdByGridJobId");
        List<String> jobIdList = null;
        jobIdList = jobDBManager.executeSelectToRetrieveJobId(userId, null, null, null, gridJobId);
        logger.debug("BEGIN retrieveJobIdByGridJobId");
        return jobIdList;
    }

    public List<String> retrieveJobIdByLease(int[] jobStatusType, Calendar maxLeaseTime, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("BEGIN retrieveJobIdByLease");
        if ((jobStatusType == null) || (jobStatusType.length == 0)) {
            logger.error("jobStatusType mustn't be null or empty!");
            throw new IllegalArgumentException("jobStatusType mustn't be null or empty!");
        }
        if (maxLeaseTime == null) {
            logger.error("maxLeaseTime mustn't be null!");
            throw new IllegalArgumentException("maxLeaseTime mustn't be null!");
        }
        List<String> jobIdList = null;
        jobIdList = jobDBManager.executeSelectToRetrieveJobIdByLease(jobStatusType, null, maxLeaseTime, userId);
        logger.debug("END retrieveJobIdByLease");
        return jobIdList;
    }

    public List<String> retrieveJobIdByLease(int[] jobStatusType, String leaseId, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("BEGIN retrieveJobIdByLease");
        if ((jobStatusType == null) || (jobStatusType.length == 0)) {
            logger.error("jobStatusType mustn't be null or empty!");
            throw new IllegalArgumentException("jobStatusType mustn't be null or empty!");
        }
        if (leaseId == null) {
            logger.error("leaseId mustn't be null!");
            throw new IllegalArgumentException("leaseId mustn't be null!");
        }
        List<String> jobIdList = null;
        jobIdList = jobDBManager.executeSelectToRetrieveJobIdByLease(jobStatusType, leaseId, null, userId);
        logger.debug("END retrieveJobIdByLease");
        return jobIdList;
    }

    public List<String> retrieveJobIdLeaseTimeExpired(int[] jobStatusType, String delegationId, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("BEGIN retrieveJobIdLeaseTimeExpired");
        if ((jobStatusType == null) || (jobStatusType.length == 0)) {
            logger.error("jobStatusType mustn't be null or empty!");
            throw new IllegalArgumentException("jobStatusType mustn't be null or empty!");
        }
        List<String> jobIdList = null;
        jobIdList = jobDBManager.executeSelectToRetrieveJobIdByLeaseTimeExpired(userId, delegationId, jobStatusType);
        logger.debug("END retrieveJobIdLeaseTimeExpired");
        return jobIdList;
    }

    public List<Lease> retrieveJobLease(Calendar maxLeaseTime, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("BEGIN retrieveJobLease");
        List<Lease> leaseList = null;
        leaseList = jobDBManager.executeSelectJobLease(null, userId, maxLeaseTime);
        logger.debug("END retrieveJobLease");
        return leaseList;
    }

    public List<Lease> retrieveJobLease(String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("BEGIN retrieveJobLease");
        List<Lease> leaseList = null;
        leaseList = jobDBManager.executeSelectJobLease(null, userId, null);
        logger.debug("END retrieveJobLease");
        return leaseList;
    }

    public Lease retrieveJobLease(String leaseId, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("BEGIN retrieveJobLease");
        if (userId == null) {
            logger.error("userId mustn't be null!");
            throw new IllegalArgumentException("userId mustn't be null!");
        }
        if ((leaseId == null) || ("".equals(leaseId))) {
            logger.error("leaseId mustn't be null or empty!");
            throw new IllegalArgumentException("leaseId mustn't be null or empty!");
        }
        List<Lease> leaseList = null;
        Lease lease = null;
        leaseList = jobDBManager.executeSelectJobLease(leaseId, userId, null);
        if (leaseList.size() > 0) {
            lease = leaseList.get(0);
        }
        logger.debug("END retrieveJobLease");
        return lease;
    }

    public List<JobStatus> retrieveJobStatusHistory(String jobId, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("BEGIN retrieveJobStatusHistory");
        List<JobStatus> jobStatusList = null;
        jobStatusList = jobDBManager.retrieveJobStatusHistory(jobId, userId);
        logger.debug("END retrieveJobStatusHistory");
        return jobStatusList;
    }

    public JobCommand retrieveLastCommand(String jobId, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("BEGIN retrieveLastCommand");
        JobCommand jobCommand = null;
        jobCommand = jobDBManager.retrieveLastJobCommand(jobId, userId);
        logger.debug("END retrieveLastCommand");
        return jobCommand;
    }

    public List<JobStatus> retrieveLastJobStatus(List<String> jobId, String userId) throws DatabaseException, IllegalArgumentException {
		logger.debug("BEGIN retrieveLastJobStatus on list");
		if (jobId == null) {
			logger.error("jobId mustn't be null!");
			throw new IllegalArgumentException("jobId mustn't be null!");
		}
		List<JobStatus> jobStatusList = new ArrayList<JobStatus>();
		JobStatus jobStatus = null;
		for (String id : jobId) {
			try {
				jobStatus = this.retrieveLastJobStatus(id, userId);
				if (jobStatus != null) {
					jobStatusList.add(jobStatus);
				}
			} catch (Exception e) {
				logger.warn(e.getMessage());
			}
		}
		logger.debug("END retrieveLastJobStatus on list");
		return jobStatusList;
	}

    public JobStatus retrieveLastJobStatus(String jobId, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("BEGIN retrieveLastJobStatus");
        JobStatus jobStatus = null;
        jobStatus = jobDBManager.retrieveLastJobStatus(jobId, userId);
        logger.debug("END retrieveLastJobStatus");
        return jobStatus;
    }

    public String retrieveOlderJobId(int[] jobStatusType, String batchSystem, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("BEGIN retrieveOlderJobId");
        String jobId = jobDBManager.executeSelectToOlderJobId(jobStatusType, batchSystem, userId);
        logger.debug("END retrieveOlderJobId");
        return jobId;
    }

    public void setLeaseExpired(Lease jobLease) throws DatabaseException, IllegalArgumentException {
        logger.debug("BEGIN setLeaseExpired");
        if (jobLease == null) {
            logger.error("jobLease mustn't be null!");
            throw new IllegalArgumentException("jobLease mustn't be null!");
        }
        if (jobLease.getUserId() == null) {
            logger.error("userId mustn't be null!");
            throw new IllegalArgumentException("userId mustn't be null!");
        }
        if ((jobLease.getLeaseId() == null) || ("".equals(jobLease.getLeaseId()))) {
            logger.error("leaseId mustn't be null or empty!");
            throw new IllegalArgumentException("leaseId mustn't be null or empty!");
        }
        if (jobLease.getLeaseTime() == null) {
            logger.error("leaseTime mustn't be null!");
            throw new IllegalArgumentException("leaseTime mustn't be null!");
        }
        jobDBManager.setLeaseExpired(jobLease);
        logger.debug("END setLeaseExpired");
    }

    public void setLeaseExpired(String jobId, Lease jobLease) throws DatabaseException, IllegalArgumentException {
        logger.debug("BEGIN setLeaseExpired. JobId= " + jobId);
        if (jobId == null) {
            logger.error("jobId mustn't be null!");
            throw new IllegalArgumentException("jobId mustn't be null!");
        }
        if (jobLease == null) {
            logger.error("jobLease mustn't be null!");
            throw new IllegalArgumentException("jobLease mustn't be null!");
        }
        if (jobLease.getUserId() == null) {
            logger.error("userId mustn't be null!");
            throw new IllegalArgumentException("userId mustn't be null!");
        }
        if ((jobLease.getLeaseId() == null) || ("".equals(jobLease.getLeaseId()))) {
            logger.error("leaseId mustn't be null or empty!");
            throw new IllegalArgumentException("leaseId mustn't be null or empty!");
        }
        if (jobLease.getLeaseTime() == null) {
            logger.error("leaseTime mustn't be null!");
            throw new IllegalArgumentException("leaseTime mustn't be null!");
        }
        jobDBManager.setLeaseExpired(jobId, jobLease);
        logger.debug("END setLeaseExpired. JobId= " + jobId);
    }

    public void setLeaseId(String leaseId, String jobId, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("BEGIN setLeaseId: leaseId = " + leaseId + " jobId = " + jobId + " userId = " + userId);
        if (jobId == null) {
            logger.error("jobId mustn't be null!");
            throw new IllegalArgumentException("jobId mustn't be null!");
        }
        if (userId == null) {
            logger.error("userId mustn't be null!");
            throw new IllegalArgumentException("userId mustn't be null!");
        }
        if ("".equals(leaseId)) {
            logger.error("leaseId mustn't be empty!");
            throw new IllegalArgumentException("leaseId mustn't be empty!");
        }
        jobDBManager.setLeaseId(leaseId, jobId, userId);
        logger.debug("END setLeaseId: leaseId = " + leaseId + " jobId = " + jobId + " userId = " + userId);

    }

    public void update(Job job) throws DatabaseException, IllegalArgumentException {
        logger.debug("BEGIN update");
        if (job == null) {
            logger.error("job mustn't be null!");
            throw new IllegalArgumentException("job mustn't be null!");
        }
        jobDBManager.updateJob(job);
        logger.debug("END update");
    }

    public int updateAllUnterminatedJobCommand() throws DatabaseException {
        logger.debug("BEGIN updateAllUnterminatedJobCommand");
        int result = jobDBManager.executeUpdateAllUnterminatedJobCommand();
        logger.debug("END updateAllUnterminatedJobCommand");
        
        return result;
    }

    public void updateJobCommand(JobCommand jobCommand) throws DatabaseException, IllegalArgumentException {
        logger.debug("BEGIN updateJobCommand");
        if (jobCommand == null) {
            logger.error("jobCommand mustn't be null!");
            throw new IllegalArgumentException("jobCommand mustn't be null!");
        }
        jobDBManager.updateJobCommand(jobCommand);
        logger.debug("END updateJobCommand");

    }

    public void updateJobLease(Lease jobLease) throws DatabaseException, IllegalArgumentException {
        logger.debug("BEGIN updateJobLease");
        if (jobLease == null) {
            logger.error("jobLease mustn't be null!");
            throw new IllegalArgumentException("jobLease mustn't be null!");
        }
        if (jobLease.getUserId() == null) {
            logger.error("userId mustn't be null!");
            throw new IllegalArgumentException("userId mustn't be null!");
        }
        if ((jobLease.getLeaseId() == null) || ("".equals(jobLease.getLeaseId()))) {
            logger.error("leaseId mustn't be null or empty!");
            throw new IllegalArgumentException("leaseId mustn't be null or empty!");
        }
        if (jobLease.getLeaseTime() == null) {
            logger.error("leaseTime mustn't be null!");
            throw new IllegalArgumentException("leaseTime mustn't be null!");
        }
        jobDBManager.executeUpdateJobLease(jobLease);
        logger.debug("END updateJobLease");
    }
    
    public void updateDelegationProxyInfo(String delegationId, String delegationProxyInfo, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("BEGIN updateDelegationProxyInfo");

        if (delegationId == null || "".equals(delegationId)) {
            logger.error("delegationId not specified!");
            throw new IllegalArgumentException("delegationId not specified!");
        }
        
        if (delegationProxyInfo == null || "".equals(delegationProxyInfo)) {
            logger.error("delegationProxyInfonot specified!");
            throw new IllegalArgumentException("delegationProxyInfo not specified!");
        }
        
        if (userId == null || "".equals(userId)) {
            logger.error("userId not specified!");
            throw new IllegalArgumentException("userId not specified!");
        }
                
        jobDBManager.executeUpdateDelegationProxyInfo(delegationId, delegationProxyInfo, userId);
        logger.debug("END updateDelegationProxyInfo");
    }
    
    public void updateStatus(JobStatus status, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("BEGIN updateStatus");
        if (status == null) {
            logger.error("status mustn't be null!");
            throw new IllegalArgumentException("status mustn't be null!");
        }
        jobDBManager.updateStatus(status, userId);
        logger.debug("END updateStatus");
    }

    public List<JobStatus> retrieveJobStatus(String fromJobStatusId, String toJobStatusId, Calendar fromDate, Calendar toDate, int maxElements, String userId) throws DatabaseException, IllegalArgumentException {
    	String iceId = null;
    	//if iceId exists, then userId must be equals "userId@iceId".
    	int index = userId.indexOf("@");
    	if ((userId != null) && (index != -1)){
    	  iceId  = userId.substring(index+1);
    	  userId = userId.substring(0, index);
    	}
    	return jobDBManager.retrieveJobStatus(fromJobStatusId, toJobStatusId, fromDate, toDate, maxElements, iceId, userId);
    }

	public List<Event> retrieveJobStatusAsEvent(String fromJobStatusId, String toJobStatusId, Calendar fromDate, Calendar toDate, int[] jobStatusType, int maxElements, String userId) throws DatabaseException,	IllegalArgumentException {
    	String iceId = null;
    	//if iceId exists, then userId must be equals "userId@iceId".
    	int index = userId.indexOf("@");
    	if ((userId != null) && (index != -1)){
    	  iceId  = userId.substring(index+1);
    	  userId = userId.substring(0, index);
    	}
    	return jobDBManager.retrieveJobStatusAsEvent(fromJobStatusId, toJobStatusId, fromDate, toDate, jobStatusType, maxElements, iceId, userId);
    }
}
