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

import org.glite.ce.creamapi.jobmanagement.Job;
import org.glite.ce.creamapi.jobmanagement.cmdexecutor.JobCommandConstant;

public final class JobRegisterCmd extends JobCmd {

    public JobRegisterCmd() {
        super(JobCommandConstant.JOB_REGISTER);
        setAsynchronous(false);
    }

    public String getCEId() {
        return getParameterAsString("CE_ID");
    }

    public String getCREAMURL() {
        return getParameterAsString("CREAM_URL");
    }

    public String getDelegationProxyInfo() {
        return getParameterAsString("DELEGATION_PROXY_INFO");
    }

    public String getGSIFTPCREAMURL() {
        return getParameterAsString("GSI_FTP_CREAM_URL");
    }

    public String getICEId() {
        return getParameterAsString("ICE_ID");
    }

    public String getJDL() {
        return getParameterAsString("JDL");
    }

    public Job getJob() {
        return (Job) getResult().getParameter("JOB");
    }

    public String getLoggerDestURI() {
        return getParameterAsString("LOGGER_DEST_URI");
    }

    public String getUserDNX500() {
        return getParameterAsString("USER_DN_X500");
    }

    public String getUserVO() {
        return getParameterAsString("USER_VO");
    }

    public boolean isAutostart() {
        if (containsParameterKey("AUTOSTART")) {
            Boolean b = new Boolean(getParameterAsString("AUTOSTART"));
            return b.booleanValue();
        }
        return false;
    }

    public void setAutostart(boolean b) {
        addParameter("AUTOSTART", new Boolean(b).toString());
    }

    public void setCEId(String ceId) {
        addParameter("CE_ID", ceId);
    }

    public void setCREAMURL(String creamURL) {
        addParameter("CREAM_URL", creamURL);
    }

    public void setDelegationProxyInfo(String delegInfo) {
        addParameter("DELEGATION_PROXY_INFO", delegInfo);
    }

    public void setGSIFTPCREAMURL(String gsiFTPCREAMURL) {
        addParameter("GSI_FTP_CREAM_URL", gsiFTPCREAMURL);
    }

    public void setICEId(String iceId) {
        addParameter("ICE_ID", iceId);
    }

    public void setJDL(String jdl) {
        addParameter("JDL", jdl);
    }

    public void setLoggerDestURI(String loggerDestURI) {
        addParameter("LOGGER_DEST_URI", loggerDestURI);
    }

    public void setUserDNX500(String dn) {
        addParameter("USER_DN_X500", dn);
    }

    public void setUserVO(String userVO) {
        addParameter("USER_VO", userVO);
    }
}
