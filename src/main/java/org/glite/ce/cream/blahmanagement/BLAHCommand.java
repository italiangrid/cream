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
 * Authors: L. Zangrando <zangrando@pd.infn.it>
 *
 */

package org.glite.ce.cream.blahmanagement;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class BLAHCommand implements Serializable {
    private static final long serialVersionUID = 1L;
    public static final String QUIT = "QUIT";
    public static final String RESULTS = "RESULTS";
    public static final String VERSION = "VERSION";
    public static final String ASYNC_MODE_ON = "ASYNC_MODE_ON";
    public static final String ASYNC_MODE_OFF = "ASYNC_MODE_OFF";
    public static final String BLAH_JOB_CANCEL = "BLAH_JOB_CANCEL";
    public static final String BLAH_JOB_STATUS = "BLAH_JOB_STATUS";
    public static final String BLAH_JOB_STATUS_ALL = "BLAH_JOB_STATUS_ALL";
    public static final String BLAH_JOB_STATUS_SELECT = "BLAH_JOB_STATUS_SELECT";
    public static final String BLAH_JOB_SUBMIT = "BLAH_JOB_SUBMIT";
    public static final String BLAH_JOB_HOLD = "BLAH_JOB_HOLD";
    public static final String BLAH_JOB_RESUME = "BLAH_JOB_RESUME";
    public static final String BLAH_JOB_REFRESH_PROXY = "BLAH_JOB_REFRESH_PROXY";
    public static final String BLAH_JOB_SEND_PROXY_TO_WORKER_NODE = "BLAH_JOB_SEND_PROXY_TO_WORKER_NODE";
    public static final String BLAH_GET_HOSTPORT = "BLAH_GET_HOSTPORT";
    public static final String BLAH_SET_GLEXEC_DN = "BLAH_SET_GLEXEC_DN";
    public static final String BLAH_SET_GLEXEC_OFF = "BLAH_SET_GLEXEC_OFF";
    public static final String BLAH_SET_SUDO_ID = "BLAH_SET_SUDO_ID";

    private String name = null;
    private String reqId = null;
    private String blahJobId = null;

    private BLAHException exception;
    private Calendar timeout;
    private List<String> parameterList = null;
    private List<String> resultList = null;

    public BLAHCommand(String name) {
        this.name = name;
    }

    public BLAHCommand(String name, String blahJobId) {
        this(name, blahJobId, null);
    }

    public BLAHCommand(String name, String blahJobId, String reqId) {
        this.name = name;
        this.blahJobId = blahJobId;
        this.reqId = reqId;
    }

    public void addParameter(String parameter) {
        if (parameter == null) {
            return;
        }

        if (parameterList == null) {
            parameterList = new ArrayList<String>(1);
        }

        parameterList.add(parameter);
    }

    public void addResult(String result) {
        if (result == null) {
            return;
        }

        if (resultList == null) {
            resultList = new ArrayList<String>(1);
        }

        resultList.add(result);
    }

    public String getBlahJobId() {
        return blahJobId;
    }

    public BLAHException getException() {
        return exception;
    }

    public String getName() {
        return name;
    }

    public List<String> getParameterList() {
        return parameterList;
    }

    public String getReqId() {
        return reqId;
    }

    public List<String> getResultList() {
        return resultList;
    }

    public Calendar getTimeout() {
        return timeout;
    }

    public boolean isSuccessfull() {
        return (exception == null);
    }

    public boolean isTimedOut() {
        if (timeout != null && Calendar.getInstance().after(timeout)) {
            return true;
        }
        return false;
    }

    public void setException(BLAHException exception) {
        this.exception = exception;
    }

    public void setReqId(String reqId) {
        this.reqId = reqId;
    }

    public void setTimeout(Calendar timeout) {
        this.timeout = timeout;
    }

    public String toString() {
        String cmd = name;

        if (reqId != null) {
            cmd += " " + reqId;
        }

        if (blahJobId != null) {
            cmd += " " + blahJobId;
        }

        if (parameterList != null) {
            for (String parameter : parameterList) {
                cmd += " " + parameter;
            }
        }

        return cmd + "\n";
    }
}
