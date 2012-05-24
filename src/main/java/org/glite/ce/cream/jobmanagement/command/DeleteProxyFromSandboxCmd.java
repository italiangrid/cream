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

package org.glite.ce.cream.jobmanagement.command;

import org.glite.ce.creamapi.cmdmanagement.Command;
import org.glite.ce.creamapi.jobmanagement.cmdexecutor.JobCommandConstant;

public final class DeleteProxyFromSandboxCmd extends JobCmd {

    public DeleteProxyFromSandboxCmd() {
        super(JobCommandConstant.DELETE_PROXY_FROM_SANDBOX);
        setAsynchronous(true);
        setExecutionMode(Command.ExecutionModeValues.PARALLEL);
        setPriorityLevel(LOW_PRIORITY);
    }
    
    public String getLocalUser() {
        return getParameterAsString("LOCAL_USER");
    }

    public String getLocalUserGroup() {
        return getParameterAsString("LOCAL_USER_GROUP");
    }

    public void setLocalUser(String user) {
        addParameter("LOCAL_USER", user);
    }
    
    public void setLocalUserGroup(String group) {
        addParameter("LOCAL_USER_GROUP", group);
    }
}
