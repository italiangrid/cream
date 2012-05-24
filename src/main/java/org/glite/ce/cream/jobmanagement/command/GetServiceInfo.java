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

package org.glite.ce.cream.jobmanagement.command;

import org.glite.ce.creamapi.jobmanagement.JobStatus;
import org.glite.ce.creamapi.jobmanagement.cmdexecutor.JobCommandConstant;

public class GetServiceInfo extends JobCmd {

    public GetServiceInfo() {
        super(JobCommandConstant.GET_SERVICE_INFO);
        setAsynchronous(false);

    }

    public boolean doesAcceptNewJobs() {
        String accept = getResult().getParameterAsString("ACCEPT_NEW_JOBS");
        if (accept == null) {
            return true;
        }

        return Boolean.parseBoolean(accept);
    }

    public long getTotalAbortedJobs() {
        Long result = (Long) getResult().getParameter(JobStatus.getNameByType(JobStatus.ABORTED));
        if (result == null) {
            return 0;
        }

        return result.longValue();
    }

    public long getTotalCancelledJobs() {
        Long result = (Long) getResult().getParameter(JobStatus.getNameByType(JobStatus.CANCELLED));
        if (result == null) {
            return 0;
        }

        return result.longValue();
    }

    public long getTotalDoneFailedJobs() {
        Long result = (Long) getResult().getParameter(JobStatus.getNameByType(JobStatus.DONE_FAILED));
        if (result == null) {
            return 0;
        }

        return result.longValue();
    }

    public long getTotalDoneOkJobs() {
        Long result = (Long) getResult().getParameter(JobStatus.getNameByType(JobStatus.DONE_OK));
        if (result == null) {
            return 0;
        }

        return result.longValue();
    }

    public long getTotalHeldJobs() {
        Long result = (Long) getResult().getParameter(JobStatus.getNameByType(JobStatus.HELD));
        if (result == null) {
            return 0;
        }

        return result.longValue();
    }

    public long getTotalIdleJobs() {
        Long result = (Long) getResult().getParameter(JobStatus.getNameByType(JobStatus.IDLE));
        if (result == null) {
            return 0;
        }

        return result.longValue();
    }

    public long getTotalJobs() {
        long result = 0L;
        result += getTotalAbortedJobs();
        result += getTotalCancelledJobs();
        result += getTotalDoneFailedJobs();
        result += getTotalDoneOkJobs();
        result += getTotalHeldJobs();
        result += getTotalIdleJobs();
        result += getTotalPendingJobs();
        result += getTotalRegisteredJobs();
        result += getTotalRunningJobs();

        return result;
    }

    public long getTotalPendingJobs() {
        Long result = (Long) getResult().getParameter(JobStatus.getNameByType(JobStatus.PENDING));
        if (result == null) {
            return 0;
        }

        return result.longValue();
    }

    public long getTotalRegisteredJobs() {
        Long result = (Long) getResult().getParameter(JobStatus.getNameByType(JobStatus.REGISTERED));
        if (result == null) {
            return 0;
        }

        return result.longValue();
    }

    public long getTotalRunningJobs() {
        Long running = (Long) getResult().getParameter(JobStatus.getNameByType(JobStatus.RUNNING));
        Long reallyRunning = (Long) getResult().getParameter(JobStatus.getNameByType(JobStatus.REALLY_RUNNING));
        long result = 0L;

        if (running != null) {
            result = running.longValue();
        }

        if (reallyRunning != null) {
            result += reallyRunning.longValue();
        }

        return result;
    }
}
