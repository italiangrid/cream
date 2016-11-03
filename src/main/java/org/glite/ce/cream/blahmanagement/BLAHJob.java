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
 * Authors: L. Zangrando, <zangrando@pd.infn.it>
 *
 */

package org.glite.ce.cream.blahmanagement;

import java.util.ArrayList;
import java.util.List;

public class BLAHJob {
    private static final long serialVersionUID = 1L;
        
    private String jobId = null;
    private String ceId = null;
    private String gridJobId = null;
    private String lrms = null;
    private String queue = null;
 //   private String sandboxPath = null;
    private String ceRequirements = null;
    private String mwVersion = null;
    private String iwd = null;
    private String userDN = null;
    private String userFQAN = null;
    private String localUser = null;
    private String virtualOrganisation = null;
    private String delegationCertPath = null;
    private String executableFile = null;
    private String standardOutputFile = null;
    private String standardErrorFile = null;
    private String transferInput = null;
    private String transferOutput = null;
    private String transferOutputRemaps = null;
    private boolean wholeNodes = false;
    private int smpGranularity = -1;
    private int nodeNumber = -1;
    private int hostNumber = -1;
    private int micNumber = 0;
    private int gpuNumber = 0;
    private String gpuMode = null;
    private String gpuModel = null;
    private List<String> arguments = null;

    public BLAHJob() {
        setArguments(new ArrayList<String>(0));
    }

    public List<String> getArguments() {
        return arguments;
    }

    public String getCeId() {
        return ceId;
    }

    public String getCeRequirements() {
        return ceRequirements;
    }

    public String getDelegationCertPath() {
        return delegationCertPath;
    }

    public String getExecutableFile() {
        return executableFile;
    }

    public String getGridJobId() {
        return gridJobId;
    }

    public int getMICNumber() {
        return micNumber;
    }
 
    public int getGPUNumber() {
        return gpuNumber;
    }

    public String getGPUMode() {
        return gpuMode;
    }

    public String getGPUModel() {
        return gpuModel;
    }

    public int getHostNumber() {
        return hostNumber;
    }

    public String getIwd() {
        return iwd;
    }

    public String getJobId() {
        return jobId;
    }

    public String getLocalUser() {
        return localUser;
    }

    public String getLRMS() {
        return lrms;
    }

    public String getMwVersion() {
        return mwVersion;
    }

    public int getNodeNumber() {
        return nodeNumber;
    }

//    public String getSandboxPath() {
//        return sandboxPath;
//    }

    public String getQueue() {
        return queue;
    }

    public int getSmpGranularity() {
        return smpGranularity;
    }

    public String getStandardErrorFile() {
        return standardErrorFile;
    }

    public String getStandardOutputFile() {
        return standardOutputFile;
    }

    public String getTransferInput() {
        return transferInput;
    }

    public String getTransferOutput() {
        return transferOutput;
    }

    public String getTransferOutputRemaps() {
        return transferOutputRemaps;
    }

    public String getUserDN() {
        return userDN;
    }

    public String getUserFQAN() {
        return userFQAN;
    }

    public String getVirtualOrganisation() {
        return virtualOrganisation;
    }

    public boolean isWholeNodes() {
        return wholeNodes;
    }

    public void setArguments(List<String> arguments) {
        this.arguments = arguments;
    }

    public void setCeId(String ceId) {
        this.ceId = ceId;
    }

    public void setCeRequirements(String ceRequirements) {
        this.ceRequirements = ceRequirements;
    }

    public void setDelegationCertPath(String delegationCertPath) {
        this.delegationCertPath = delegationCertPath;
    }

    public void setExecutableFile(String executableFile) {
        this.executableFile = executableFile;
    }

    public void setGridJobId(String gridJobId) {
        this.gridJobId = gridJobId;
    }

    public void setMICNumber(int micNumber) {
        this.micNumber = micNumber;
    }

    public void setGPUNumber(int gpuNumber) {
        this.gpuNumber = gpuNumber;
    }

    public void setGPUMode(String gpuMode) {
        this.gpuMode = gpuMode;
    }

    public void setGPUModel(String gpuModel) {
        this.gpuModel = gpuModel;
    }

    public void setHostNumber(int hostNumber) {
        this.hostNumber = hostNumber;
    }

    public void setIwd(String iwd) {
        this.iwd = iwd;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public void setLocalUser(String localUser) {
        this.localUser = localUser;
    }

    public void setLRMS(String lrms) {
        this.lrms = lrms;
    }

    public void setMwVersion(String mwVersion) {
        this.mwVersion = mwVersion;
    }

//    public void setSandboxPath(String sandboxPath) {
//        this.sandboxPath = sandboxPath;
//    }

    public void setNodeNumber(int nodeNumber) {
        this.nodeNumber = nodeNumber;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public void setSmpGranularity(int smpGranularity) {
        this.smpGranularity = smpGranularity;
    }

    public void setStandardErrorFile(String standardErrorFile) {
        this.standardErrorFile = standardErrorFile;
    }

    public void setStandardOuputFile(String standardOutputFile) {
        this.standardOutputFile = standardOutputFile;
    }

    public void setTransferInput(String transferInput) {
        this.transferInput = transferInput;
    }

    public void setTransferOutput(String transferOutput) {
        this.transferOutput = transferOutput;
    }

    public void setTransferOutputRemaps(String transferOutputRemaps) {
        this.transferOutputRemaps = transferOutputRemaps;
    }

    public void setUserDN(String userDN) {
        this.userDN = userDN;
    }

    public void setUserFQAN(String userFQAN) {
        this.userFQAN = userFQAN;
    }

    public void setVirtualOrganisation(String virtualOrganisation) {
        this.virtualOrganisation = virtualOrganisation;
    }

    public void setWholeNodes(boolean wholeNodes) {
        this.wholeNodes = wholeNodes;
    }
}
