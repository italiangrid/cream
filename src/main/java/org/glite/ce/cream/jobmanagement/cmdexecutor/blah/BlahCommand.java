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
 
package org.glite.ce.cream.jobmanagement.cmdexecutor.blah;

import java.io.Serializable;
import java.util.Calendar;

public class BlahCommand implements Serializable {
    private static final long serialVersionUID = 1L;
    public final static int QUIT = 0;
    public final static int RESULTS = 1;
    public final static int VERSION = 2;
    public final static int ASYNC_MODE_ON = 3;
    public final static int ASYNC_MODE_OFF = 4;
    public final static int BLAH_JOB_CANCEL = 5;
    public final static int BLAH_JOB_STATUS = 6;
    public final static int BLAH_JOB_SUBMIT = 7;
    public final static int BLAH_JOB_HOLD = 8;
    public final static int BLAH_JOB_RESUME = 9;
    public final static int BLAH_JOB_REFRESH_PROXY = 10;
    public final static int BLAH_JOB_SEND_PROXY_TO_WORKER_NODE = 11;
    public final static int BLAH_GET_HOSTPORT = 12;
    public final static int BLAH_SET_GLEXEC_DN = 13;
    public final static int BLAH_SET_GLEXEC_OFF = 14;
    public final static int BLAH_SET_SUDO_ID = 15;

    public final static String[] commands = { "QUIT", "RESULTS", "VERSION", "ASYNC_MODE_ON", "ASYNC_MODE_OFF", "BLAH_JOB_CANCEL", "BLAH_JOB_STATUS", "BLAH_JOB_SUBMIT", "BLAH_JOB_HOLD", "BLAH_JOB_RESUME",
            "BLAH_JOB_REFRESH_PROXY", "BLAH_JOB_SEND_PROXY_TO_WORKER_NODE", "BLAH_GET_HOSTPORT", "BLAH_SET_GLEXEC_DN", "BLAH_SET_GLEXEC_OFF", "BLAH_SET_SUDO_ID" };

    private String reqId;
    private String[] parameter, jobId;
    private BLAHCommandResult[] result;
    private int cmdType = 0;
    private Exception exception;
    private Calendar startTime, stopTime;

    public BlahCommand(int cmdType) {
        this(cmdType, null);
    }

    public BlahCommand(int cmdType, String reqId) {
        this.cmdType = cmdType;
        this.reqId = reqId;
        this.jobId = null;
        this.parameter = null;
        this.result = new BLAHCommandResult[] { new BLAHCommandResult() };
    }

    public BlahCommand(int cmdType, String reqId, String jobId) {
        this(cmdType, reqId, new String[] { jobId });
    }

    public BlahCommand(int cmdType, String reqId, String jobId[]) {
        this.cmdType = cmdType;
        this.reqId = reqId;
        this.jobId = jobId;
        this.parameter = null;

        if (jobId != null) {
            this.result = new BLAHCommandResult[jobId.length];
            for (int i = 0; i < jobId.length; i++) {
                result[i] = new BLAHCommandResult(jobId[i]);
            }
        }
    }

    public Exception getException() {
        return exception;
    }

    public String getName() {
        return commands[cmdType];
    }

    public String[] getParameter() {
        return parameter;
    }

    public String getReqId() {
        return reqId;
    }

    public BLAHCommandResult[] getResult() {
        return result;
    }

    public BLAHCommandResult getResultAt(int i) {
        if (result != null && i < result.length) {
            return result[i];
        }
        return null;
    }

    public Calendar getStartTime() {
        return startTime;
    }

    public Calendar getStopTime() {
        return stopTime;
    }

    public long getTime() {
        long diffTime = stopTime.getTimeInMillis() - startTime.getTimeInMillis();
        return diffTime;
    }

    public int getType() {
        return cmdType;
    }

    public boolean isCompleted() {
        boolean completed = true;
        for (int i = 0; i < result.length && completed; i++) {
            if (result[i] == null || result[i].getResult() == null) {
                completed = false;
            }
        }
        return completed;
    }

    public boolean isExceptionOccurred() {
        return (exception != null);
    }

    private String makeCommandParameter(String[] values) {
        String result = "";

        if (values != null) {
            for (int i = 0; i < values.length; i++) {
                result += " " + values[i];
            }
        }

        return result;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    public void setParameter(String parameter) {
        setParameter(new String[] { parameter });
    }

    public void setParameter(String[] parameter) {
        this.parameter = parameter;
    }

    public void setReqId(String reqId) {
        this.reqId = reqId;
    }

    public void setResult(BLAHCommandResult[] result) {
        this.result = result;
    }

    public void setResult(int index, BLAHCommandResult cr) {
        if (result != null && index < result.length) {
            result[index] = cr;
        }
    }

    public void setType(int cmdType) {
        this.cmdType = cmdType;
    }

    public Calendar start() {
        return startTime = Calendar.getInstance();
    }

    public Calendar stop() {
        return stopTime = Calendar.getInstance();
    }

    public String toString() {
        String cmd = commands[cmdType];

        if (reqId != null) {
            cmd += " " + reqId;
        }

        switch (cmdType) {
        case BLAH_SET_SUDO_ID:
        case BLAH_SET_GLEXEC_DN:
        case BLAH_GET_HOSTPORT:
        case BLAH_JOB_SUBMIT:
            cmd += makeCommandParameter(parameter);
            break;
        case BLAH_JOB_CANCEL:
        case BLAH_JOB_HOLD:
        case BLAH_JOB_RESUME:
        case BLAH_JOB_STATUS:
            cmd += makeCommandParameter(jobId);
            break;
        case BLAH_JOB_REFRESH_PROXY:
        case BLAH_JOB_SEND_PROXY_TO_WORKER_NODE:
            cmd += makeCommandParameter(jobId) + " " + makeCommandParameter(parameter);
            break;
        }

        return cmd + "\n";
    }
}
