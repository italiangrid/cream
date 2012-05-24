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

package org.glite.ce.cream.jobmanagement.db;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;

import org.glite.ce.commonj.db.DatabaseException;
import org.glite.ce.creamapi.eventmanagement.Event;
import org.glite.ce.creamapi.jobmanagement.Job;
import org.glite.ce.creamapi.jobmanagement.JobStatus;
import org.glite.ce.creamapi.jobmanagement.Lease;
import org.glite.ce.creamapi.jobmanagement.command.JobCommand;
import org.glite.ce.creamapi.jobmanagement.db.JobDBInterface;

public class JobDBInMemory implements JobDBInterface {    
    private Hashtable jobTable;

    public JobDBInMemory() {
        jobTable = new Hashtable(0);
    }

    public void delete(String jobId, String userId) throws DatabaseException, IllegalArgumentException {
        if (jobId == null) {
            throw new IllegalArgumentException("jobId not specified!");
        }

        if (userId != null) {
            Hashtable userTable = getUserTable(userId);

            if (!userTable.containsKey(jobId)) {
                throw new DatabaseException("job " + jobId + " doesn't exist!");
            }
            userTable.remove(jobId);
        } else {
            ArrayList userTable = new ArrayList(jobTable.values());
            for (int i = 0; i < userTable.size(); i++) {
                Hashtable table = (Hashtable) userTable.get(i);
                if (table.containsKey(jobId)) {
                    table.remove(jobId);
                }
            }
        }

        throw new DatabaseException("job " + jobId + " doesn't exist!");
    }

    public void init(String datasourceName) throws IllegalArgumentException {
        // TODO Auto-generated method stub

    }

    private Hashtable getUserTable(String userId) throws IllegalArgumentException {
        if (userId == null) {
            throw new IllegalArgumentException("userId not specified!");
        }

        if (jobTable.containsKey(userId)) {
            return (Hashtable) jobTable.get(userId);
        }

        Hashtable userTable = new Hashtable(0);
        jobTable.put(userId, userTable);

        return userTable;
    }

    public void insert(Job job) throws DatabaseException, IllegalArgumentException {
        if (job == null) {
            throw new IllegalArgumentException("job not specified!");
        }

        if (jobTable.containsKey(job.getId())) {
            throw new DatabaseException("job " + job.getId() + " already exist!");
        }

        if (job.getUserId() == null) {
            throw new DatabaseException("userId not defined!");
        }

        Hashtable userTable = getUserTable(job.getUserId());
        userTable.put(job.getId(), job);
    }

    public void insertStatus(JobStatus status, String userId) throws DatabaseException, IllegalArgumentException {
        if (status == null) {
            throw new IllegalArgumentException("status not specified!");
        }

        if (status.getJobId() == null) {
            throw new IllegalArgumentException("jobId not defined!");
        }

        Job job = retrieveJob(status.getJobId(), userId);
        job.addStatus(status);
    }

    public boolean isAlive() {
        return true;
    }

    public List<JobCommand> retrieveCommandHistory(String jobId, String userId) throws DatabaseException, IllegalArgumentException {
        Job job = retrieveJob(jobId, userId);

        return job.getCommandHistory();
    }

    public Job retrieveJob(String jobId, String userId) throws DatabaseException, IllegalArgumentException {
        if (jobId == null) {
            throw new IllegalArgumentException("jobId not specified!");
        }

        if (userId != null) {
            Hashtable userTable = getUserTable(userId);

            if (!userTable.containsKey(jobId)) {
                throw new DatabaseException("job " + jobId + " doesn't exist!");
            }
            return (Job) userTable.get(jobId);
        } else {
            ArrayList userTable = new ArrayList(jobTable.values());
            for (int i = 0; i < userTable.size(); i++) {
                Hashtable table = (Hashtable) userTable.get(i);
                if (table.containsKey(jobId)) {
                    return (Job) table.get(jobId);
                }
            }
        }

        throw new DatabaseException("job " + jobId + " doesn't exist!");
    }

    public List<String> retrieveJobId(String userId) throws DatabaseException, IllegalArgumentException {
        Hashtable userTable = getUserTable(userId);

        ArrayList result = new ArrayList(0);
        ArrayList jobList = new ArrayList(userTable.values());
        Job job = null;

        for (int x = 0; x < jobList.size(); x++) {
            job = (Job) jobList.get(x);
            result.add(job.getId());
        }
       
        return result;
    }

    public List<String> retrieveJobId(List<String> jobId, String userId) throws DatabaseException, IllegalArgumentException {
        List<String> result = new ArrayList(0);

        if (userId != null) {
            Hashtable userTable = getUserTable(userId);

            if (jobId != null) {
                for (int i = 0; i < jobId.size(); i++) {
                    if (userTable.containsKey(jobId.get(i))) {
                        result.add(jobId.get(i));
                    }
                }
            } else {
                ArrayList jobList = new ArrayList(userTable.values());
                Job job = null;

                for (int i = 0; i < jobList.size(); i++) {
                    job = (Job) jobList.get(i);
                    result.add(job.getId());
                }
            }
        } else {
            ArrayList userTable = new ArrayList(jobTable.values());
            for (int i = 0; i < userTable.size(); i++) {
                Hashtable table = (Hashtable) userTable.get(i);

                if (jobId != null) {
                    for (int x = 0; x < jobId.size(); x++) {
                        if (table.containsKey(jobId.get(x))) {
                            result.add(jobId.get(x));
                        }
                    }
                } else {
                    ArrayList jobList = new ArrayList(table.values());
                    Job job = null;

                    for (int x = 0; x < jobList.size(); x++) {
                        job = (Job) jobList.get(x);
                        result.add(job.getId());
                    }
                }
            }
        }

        return result;
    }

    public List<String> retrieveJobId(List<String> jobId, String userId, int[] jobStatusId) throws DatabaseException, IllegalArgumentException {
        List<String> jobList = retrieveJobId(jobId, userId);
        
        if (jobStatusId == null) {
            return jobList;
        }

        Job job = null;
        for (int i = 0; i < jobList.size(); i++) {
            job = retrieveJob(jobList.get(i), userId);
            boolean found = false;
            for (int x = 0; x < jobStatusId.length && !found; x++) {
                if (job.getLastStatus() != null && job.getLastStatus().getType() == jobStatusId[x]) {
                    found = true;
                }
            }
            if (!found) {
                jobList.remove(i);
            }
        }

        return jobList;
    }

    public List<String> retrieveJobId(String delegId, String userID) throws DatabaseException, IllegalArgumentException {
        // TODO Auto-generated method stub
        return null;
    }

    public List<String> retrieveJobId(int[] jobStatusType, String userId) throws DatabaseException, IllegalArgumentException {
        List<String> result = new ArrayList(0);
        ArrayList userTable = null;

        if (userId != null) {
            userTable = new ArrayList(0);
            userTable.add(getUserTable(userId));
        } else {
            userTable = new ArrayList(jobTable.values());
        }

        for (int x = 0; x < userTable.size(); x++) {
            Hashtable table = (Hashtable) userTable.get(x);

            Job job = null;

            for (int i = 0; i < table.size(); i++) {
                job = (Job) table.get(i);
                if (jobStatusType == null || jobStatusType.length == 0) {
                    result.add(job.getId());
                } else {
                    for (int y = 0; y < jobStatusType.length; y++) {
                        if (jobStatusType[y] == job.getLastStatus().getType()) {
                            result.add(job.getId());
                            continue;
                        }
                    }
                }
            }
        }

        return result;
    }

    public List<String> retrieveJobId(String delegId, int[] jobStatusId, String userId) throws DatabaseException, IllegalArgumentException {
        // TODO Auto-generated method stub
        return null;
    }

    public List<String> retrieveJobIdByGridJobId(List<String> gridJobId, String userId) throws DatabaseException, IllegalArgumentException {
        // TODO Auto-generated method stub
        return null;
    }

    public List<JobStatus> retrieveJobStatusHistory(String jobId, String userId) throws DatabaseException, IllegalArgumentException {
        Job job = retrieveJob(jobId, userId);

        return job.getStatusHistory();
    }

    public JobCommand retrieveLastCommand(String jobId, String userId) throws DatabaseException, IllegalArgumentException {
        Job job = retrieveJob(jobId, userId);
        if (job.getStatusCount() > 0) {
            return job.getCommandHistoryAt(job.getCommandHistoryCount() - 1);
        }

        return null;
    }

    public JobStatus retrieveLastJobStatus(String jobId, String userId) throws DatabaseException, IllegalArgumentException {
        Job job = retrieveJob(jobId, userId);
        if (job.getStatusCount() > 0) {
            return job.getStatusAt(job.getStatusCount() - 1);
        }

        return null;
    }

    public List<JobStatus> retrieveLastJobStatus(List<String> jobId, String userId) throws DatabaseException, IllegalArgumentException {
        // TODO Auto-generated method stub
        return null;
    }

    public void update(Job job) throws DatabaseException, IllegalArgumentException {
        if (job == null) {
            throw new IllegalArgumentException("job not specified!");
        }

        if (job.getUserId() == null) {
            throw new IllegalArgumentException("userId not defined!");
        }

        Hashtable userTable = getUserTable(job.getUserId());
        if (userTable.containsKey(job.getId())) {
            userTable.remove(job.getId());
            userTable.put(job.getId(), job);
        }
    }

    public void updateStatus(JobStatus status, String userId) throws DatabaseException, IllegalArgumentException {
        if (status == null) {
            throw new IllegalArgumentException("status not specified!");
        }

        Job job = retrieveJob(status.getJobId(), userId);
        List<JobStatus> list = job.getStatusHistory();
        if (list.size() == 0) {
            return;
        }

        list.remove(list.size() - 1);
        list.add(status);
    }

    public void updateLastStatus(JobStatus status, String userId) throws DatabaseException, IllegalArgumentException {
        if (status == null) {
            throw new IllegalArgumentException("status not specified!");
        }
        
    }

    public void insertJobCommand(JobCommand jobCommand) throws DatabaseException, IllegalArgumentException {
        if (jobCommand == null) {
            throw new IllegalArgumentException("jobCommand not specified!");
        }
        
        if (jobCommand.getJobId() == null) {
            throw new IllegalArgumentException("jobId not defined!");
        }
        JobCommand clone = new JobCommand(jobCommand.getType(), jobCommand.getJobId());
        clone.setCommandExecutorName(jobCommand.getCommandExecutorName());
        clone.setCreationTime(jobCommand.getCreationTime());
        clone.setDescription(jobCommand.getDescription());
        clone.setExecutionCompletedTime(jobCommand.getExecutionCompletedTime());
        clone.setFailureReason(jobCommand.getFailureReason());
        clone.setStartProcessingTime(jobCommand.getStartProcessingTime());
        clone.setStartSchedulingTime(jobCommand.getStartSchedulingTime());
        clone.setStatus(jobCommand.getStatus());
        clone.setUserId(jobCommand.getUserId());
        
        Job job = retrieveJob(jobCommand.getJobId(), jobCommand.getUserId());
        job.addCommandHistory(clone);
    }

    public void updateJobCommand(JobCommand jobCommand) throws DatabaseException, IllegalArgumentException {
        if (jobCommand == null) {
            throw new IllegalArgumentException("jobCommand not specified!");
        }
        
        if (jobCommand.getJobId() == null) {
            throw new IllegalArgumentException("jobId not defined!");
        }
        
        Job job = retrieveJob(jobCommand.getJobId(), jobCommand.getUserId());
        List<JobCommand> history = job.getCommandHistory();
        history.set(history.size()-1, jobCommand);        
    }

    public List<String> retrieveJobId(int[] jobStatusType, Calendar leaseTime, String userId) throws DatabaseException, IllegalArgumentException {
        if (leaseTime == null) {
            throw new IllegalArgumentException("jobCommand not specified!");
        }
        List<String> result = new ArrayList(0);
        ArrayList userTable = null;

        if (userId != null) {
            userTable = new ArrayList(0);
            userTable.add(getUserTable(userId));
        } else {
            userTable = new ArrayList(jobTable.values());
        }

        for (int x = 0; x < userTable.size(); x++) {
            Hashtable table = (Hashtable) userTable.get(x);

            Job job = null;

            Enumeration enumeration = table.elements();
            while(enumeration.hasMoreElements()) {
                job = (Job)enumeration.nextElement();
            
                if (jobStatusType == null || jobStatusType.length == 0) {
                    if(job.getLease() != null && job.getLease().getLeaseTime().before(leaseTime)) {
                        result.add(job.getId());
                    }
                } else {
                    for (int y = 0; y < jobStatusType.length; y++) {
                        if (job.getLastStatus() != null && jobStatusType[y] == job.getLastStatus().getType()) {
                            if(job.getLease() != null && job.getLease().getLeaseTime().before(leaseTime)) {
                                result.add(job.getId());
                            }
                            continue;                        
                        }
                    }
                }
            }
        }

        return result;
    }

    public void deleteJobLease(String leaseId, String userId) throws DatabaseException, IllegalArgumentException {
        // TODO Auto-generated method stub
        
    }

    public void insertJobLease(Lease jobLease) throws DatabaseException, IllegalArgumentException {
        // TODO Auto-generated method stub
        
    }

    public long jobCountByStatus(int[] jobStatusType, String userId) throws DatabaseException, IllegalArgumentException {
        // TODO Auto-generated method stub
        return 0;
    }

    public List<String> retrieveByDate(List<String> jobId, String userId, Calendar startDate, Calendar endDate) throws DatabaseException, IllegalArgumentException {
        // TODO Auto-generated method stub
        return null;
    }

    public List<String> retrieveJobId(List<String> jobId, String delegId, String leaseId, String userId) throws DatabaseException, IllegalArgumentException {
        // TODO Auto-generated method stub
        return null;
    }

    public List<String> retrieveJobIdByLease(int[] jobStatusType, String leaseId, String userId) throws DatabaseException, IllegalArgumentException {
        // TODO Auto-generated method stub
        return null;
    }

    public List<String> retrieveJobIdByLease(int[] jobStatusType, Calendar maxLeaseTime, String userId) throws DatabaseException, IllegalArgumentException {
        // TODO Auto-generated method stub
        return null;
    }

    public Lease retrieveJobLease(String leaseId, String userId) throws DatabaseException, IllegalArgumentException {
        // TODO Auto-generated method stub
        return null;
    }

    public List<Lease> retrieveJobLease(String userId) throws DatabaseException, IllegalArgumentException {
        // TODO Auto-generated method stub
        return null;
    }

    public List<Lease> retrieveJobLease(Calendar maxLeaseTime, String userId) throws DatabaseException, IllegalArgumentException {
        // TODO Auto-generated method stub
        return null;
    }

    public void setLeaseExpired(Lease lease) throws DatabaseException, IllegalArgumentException {
        // TODO Auto-generated method stub
        
    }

    public void setLeaseExpired(String jobId, Lease lease) throws DatabaseException, IllegalArgumentException {
        // TODO Auto-generated method stub
        
    }

    public void setLeaseId(String leaseId, String jobId, String userId) throws DatabaseException, IllegalArgumentException {
        // TODO Auto-generated method stub
        
    }

    public void updateJobLease(Lease jobLease) throws DatabaseException, IllegalArgumentException {
        // TODO Auto-generated method stub
        
    }

	public List<String> retrieveJobIdLeaseTimeExpired(int[] jobStatusType, String delegationId, String userId) throws DatabaseException, IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

    public String retrieveOlderJobId(int[] jobStatusType, String batchSystem, String userId) throws DatabaseException, IllegalArgumentException {
        // TODO Auto-generated method stub
        return null;
    }

    public int updateAllUnterminatedJobCommand() throws DatabaseException {
       return 0;        
    }

	public List<String> retrieveJobId(List<String> jobId, String userId, int[] jobStatusType, Calendar startStatusDate, Calendar endStatusDate) throws DatabaseException, IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

	public List<String> retrieveJobId(int[] jobStatusType, String queueName,
			String batchSystem, String userId) throws DatabaseException,
			IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

	public List<String> retrieveJobId(String userId, String delegationId,
			int[] jobStatusType, Calendar registerCommandStartDate,
			Calendar registerCommandEndDate, String leaseId,
			Calendar startStatusDate, Calendar endStatusDate)
			throws DatabaseException, IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

	public List<JobStatus> retrieveJobStatus(String fromJobStatusId,
			String toJobStatusId, Calendar fromDate, Calendar toDate,
			int maxElements, String userId) throws DatabaseException,
			IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

    public List<Event> retrieveJobStatusAsEvent(String fromJobStatusId, String toJobStatusId, Calendar fromDate, Calendar toDate, int[] jobStatusType, int maxElements, String userId) throws DatabaseException,
            IllegalArgumentException {
        // TODO Auto-generated method stub
        return null;
    }

    public void updateDelegationProxyInfo(String delegationId, String delegationProxyInfo, String userId) throws DatabaseException, IllegalArgumentException {
        // TODO Auto-generated method stub
    }

    public void insertJobCommand(JobCommand jobCommand, String delegationId, int[] jobStatusType) throws DatabaseException, IllegalArgumentException {
        // TODO Auto-generated method stub        
    }
}
