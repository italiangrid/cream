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

import java.util.Calendar;

import org.glite.ce.creamapi.jobmanagement.cmdexecutor.JobCommandConstant;

public class SetLeaseCmd extends JobCmd {
    private static final String LEASE_TIME = "LEASE_TIME";
    
    public SetLeaseCmd() {
        super(JobCommandConstant.SET_LEASE);
        setAsynchronous(false);
    }

    public SetLeaseCmd(String leaseId, Calendar leaseTime) {
        super(JobCommandConstant.SET_LEASE);
        setAsynchronous(false);
        setLeaseId(leaseId);
        setLeaseTime(leaseTime);
    }

    public Calendar getLeaseTime() {
        if (containsParameterKey(LEASE_TIME)) {
            Long timestamp = Long.parseLong(getParameterAsString(LEASE_TIME));
            Calendar date = Calendar.getInstance();
            date.setTimeInMillis(timestamp);

            return date;
        }
        return null;
    }

    public void setLeaseTime(Calendar date) {
        if (date != null) {
            addParameter(LEASE_TIME, "" + date.getTimeInMillis());
        }
    }
    
    public Calendar getLeaseTimeResult() {
        return (Calendar)getResult().getParameter(LEASE_TIME);
    }
}
