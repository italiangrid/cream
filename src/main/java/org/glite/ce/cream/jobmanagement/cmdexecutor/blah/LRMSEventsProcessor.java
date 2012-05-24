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
 * Authors: Luigi Zangrando, <luigi.zangrando@pd.infn.it>
 *
 */

package org.glite.ce.cream.jobmanagement.cmdexecutor.blah;

import java.util.Calendar;
import java.util.Hashtable;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;
import org.glite.ce.cream.cmdmanagement.CommandManager;
import org.glite.ce.creamapi.cmdmanagement.Command;
import org.glite.ce.creamapi.jobmanagement.JobStatus;
import org.glite.ce.creamapi.jobmanagement.cmdexecutor.JobCommandConstant;

public class LRMSEventsProcessor {
    private static Logger logger = Logger.getLogger(LRMSEventsProcessor.class.getName());

    private BLAHExecutor blahExec = null;

    public LRMSEventsProcessor(BLAHExecutor blahExec) {
        this.blahExec = blahExec;
    }

    public void processEvent(String line) {
        if (line == null) {
            return;
        }

        logger.debug("processing event: " + line);

        int jobStatus = -1;
        String creamJobId = null;
        String exitCode = null;
        Hashtable<String, String> attributeTable = new Hashtable<String, String>(0);

        if (line.startsWith("[")) {
            line = line.substring(1);
        }

        if (line.endsWith("]")) {
            line = line.substring(0, line.length() - 1);
        }

        StringTokenizer st = new StringTokenizer(line, ";");
        String attribute = null;
        String value = null;
        int index = -1;

        while (st.hasMoreElements()) {
            attribute = st.nextToken();

            if ((index = attribute.indexOf("=")) != -1) {
                value = attribute.substring(index + 1, attribute.length());
                value = value.trim();
                
                if (value.startsWith("\"")) {
                    value = value.substring(1);
                }

                if (value.endsWith("\"")) {
                    value = value.substring(0, value.length() - 1);
                }

                attributeTable.put(attribute.substring(0, index).trim(), value.trim());  
            }
        }

        if (attributeTable.containsKey("JobStatus")) {
            jobStatus = Integer.parseInt(attributeTable.get("JobStatus"));
        }

        if (attributeTable.containsKey("ClientJobId")) {
            creamJobId = "CREAM" + attributeTable.get("ClientJobId");
        } else {
            if (attributeTable.containsKey("BlahJobName")) {
                creamJobId = "CREAM" + attributeTable.get("BlahJobName");
            } else {
                logger.error("ClientJobId or BlahJobName not present in this line: " + line);
                return;
            }
        }

        int creamJobStatusType;
        switch (jobStatus) {
        case 1: // job new
            creamJobStatusType = JobStatus.IDLE;
            break;
        case 2: // job running
            creamJobStatusType = JobStatus.RUNNING;
            break;
        case 3: // job cancelled
            creamJobStatusType = JobStatus.CANCELLED;
            break;
        case 4: // job done
            String reason = attributeTable.get("Reason");

            if (reason == null || reason.endsWith("reason=0")) {
                creamJobStatusType = JobStatus.DONE_OK;
                reason = null;
                exitCode = "W";

                if (attributeTable.containsKey("ExitCode")) {
                    exitCode = attributeTable.get("ExitCode");
                }
            } else if (reason.endsWith("reason=999")) {
                exitCode = "999";
                reason = "job not found!";
                creamJobStatusType = JobStatus.ABORTED;
            } else {
                exitCode = "W";
                creamJobStatusType = JobStatus.DONE_FAILED;
            }

            break;
        case 5: // job held
            creamJobStatusType = JobStatus.HELD;
            break;
        case 11: // job really_running
            creamJobStatusType = JobStatus.REALLY_RUNNING;
            break;
        default:
            logger.warn("invalid status " + line);
            return;
        }

        if (logger.isDebugEnabled()) {
            logger.debug("LRMSEVENTPARSER new status " + JobStatus.getNameByType(creamJobStatusType) + " for job " + creamJobId
                    + (attributeTable.containsKey("ChangeTime") ? " at time " + attributeTable.get("ChangeTime") : ""));
        }

        Command statusCmd = new Command(JobCommandConstant.SET_JOB_STATUS, blahExec.getCategory());
        // statusCmd.setCommandExecutorName(blahExec.getName());
        statusCmd.setAsynchronous(true);
        statusCmd.setUserId("admin");
        statusCmd.addParameter("JOB_ID", creamJobId);
        statusCmd.addParameter("STATUS_TYPE", "" + creamJobStatusType);
        statusCmd.addParameter("STATUS_CHANGE_TIME", attributeTable.get("ChangeTime"));
        statusCmd.addParameter("WORKER_NODE", attributeTable.get("WorkerNode"));
        statusCmd.addParameter("LRMS_JOB_ID", attributeTable.get("BatchJobId"));
        statusCmd.addParameter("EXIT_CODE", exitCode);
        statusCmd.addParameter("FAILURE_REASON", attributeTable.get("Reason"));
        statusCmd.addParameter("IS_ADMIN", "true");
        statusCmd.setPriorityLevel(Command.MEDIUM_PRIORITY);
        statusCmd.setExecutionMode(Command.ExecutionModeValues.SERIAL);
        statusCmd.setCommandGroupId(creamJobId);

        long startTime = 0L;

        if (logger.isDebugEnabled()) {
            startTime = Calendar.getInstance().getTimeInMillis();
        }

        try {
            CommandManager.getInstance().execute(statusCmd);
        } catch (Throwable e) {
            logger.error(e.getMessage());
        }

        if (logger.isDebugEnabled()) {
            logger.info("processor time (msec): " + (Calendar.getInstance().getTimeInMillis() - startTime));
        }
    }
}
