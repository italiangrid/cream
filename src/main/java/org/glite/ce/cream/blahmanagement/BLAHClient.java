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

import java.io.File;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

public class BLAHClient {
    private static final Logger logger = Logger.getLogger(BLAHClient.class.getName());
    public static final String SANDBOX_TRANSFER_METHOD_LRMS = "LRMS";
    public static final String SANDBOX_TRANSFER_METHOD_GSIFTP = "GSIFTP";
    private BLAHProcess blahProcess = null;
    private String prefix = null;
    private String executablePath = null;
    private String scratchDirPath = null;
    private String sandboxTransfertMethod = null;
    private int hostSMPSize = 0;
    private int commandTimeout = 0;
    private int notifierRetryCount = 0;
    private int notifierRetryDelay = 0;
    private int notificationListenerPort = 0;
    private boolean asynchronousMode = false;
    private Hashtable<String, BLAHNotifierClient> blahNotifierClientTable;
    private BLAHNotificationListener blahNotificationListener = null;
    private BLAHJobStatusChangeListener jobStatusChangeListener = null;
    private static int nextId = 1;

    public BLAHClient() {
        executablePath = "/usr/bin/blahpd";
        scratchDirPath = "/tmp/";
        hostSMPSize = 1;
        commandTimeout = 300; // 300 sec
        notifierRetryCount = 100;
        notifierRetryDelay = 60000; // 1 minute
        notificationListenerPort = 9091;
        sandboxTransfertMethod = SANDBOX_TRANSFER_METHOD_LRMS;
    }

    public void cancel(String blahJobId, String localUser) throws BLAHException {
        execute(new BLAHCommand(BLAHCommand.BLAH_JOB_CANCEL, blahJobId, getReqId()), localUser);
    }

    private void execute(BLAHCommand command, String localUser) throws BLAHException {
        if (command == null) {
            throw new BLAHException("command not specified!");
        }

        List<BLAHCommand> commandList = new ArrayList<BLAHCommand>(1);
        if (localUser != null) {
            BLAHCommand setSudoCommand = new BLAHCommand(BLAHCommand.BLAH_SET_SUDO_ID);
            setSudoCommand.addParameter(localUser);

            commandList.add(setSudoCommand);
        }

        commandList.add(command);

        synchronized (blahProcess) {
            blahProcess.execute(commandList);
        }

        if (asynchronousMode) {
            blahProcess.waitForResult(command);
        }
        
        if (!command.isSuccessfull()) {
            throw command.getException();
        }
    }
    
    protected BLAHNotifierInfo getBlahHostPort(String batchSystem) {
        if (batchSystem == null) {
            return null;
        }

        try {
            List<BLAHNotifierInfo> list = getBLAHNotifierInfo();
            for (BLAHNotifierInfo blahNotifierInfo : list) {
                if (batchSystem.equalsIgnoreCase(blahNotifierInfo.getLRMS())) {
                    return blahNotifierInfo;
                }
            }
        } catch (Exception e) {
        }

        return null;
    }

    public List<BLAHNotifierInfo> getBLAHNotifierInfo() throws BLAHException {
        BLAHCommand getHostPortCommand = new BLAHCommand(BLAHCommand.BLAH_GET_HOSTPORT);
        getHostPortCommand.setReqId(getReqId());
        execute(getHostPortCommand, null);

        List<String> resultList = getHostPortCommand.getResultList();

        if (resultList == null) {
            throw new BLAHException("result not available!");
        }

        List<BLAHNotifierInfo> list = new ArrayList<BLAHNotifierInfo>(0);
        int port = -1;
        String lrms = null;
        String host = null;
        StringTokenizer st = null;

        for (String result : resultList) {
            port = -1;
            lrms = null;
            host = null;
            st = new StringTokenizer(result);

            while (st.hasMoreTokens()) {
                String token = st.nextToken().trim();
                int i = token.indexOf("/");
                try {
                    if (i > 0) {
                        lrms = token.substring(0, i).toLowerCase();

                        int j = token.indexOf(":");
                        if (j > 0) {
                            host = token.substring(i + 1, j);
                            port = Integer.parseInt(token.substring(j + 1, token.length()));
                        }

                        list.add(new BLAHNotifierInfo(lrms, host, port));
                    }
                } catch (IndexOutOfBoundsException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
        return list;
    }

    public int getCommandTimeout() {
        return commandTimeout;
    }

    public String getExecutablePath() {
        return executablePath;
    }

    public int getHostSMPSize() {
        return hostSMPSize;
    }

    public String getPrefix() {
        return prefix;
    }

    public int getNotificationListenerPort() {
        return notificationListenerPort;
    }

    public int getNotifierRetryCount() {
        return notifierRetryCount;
    }

    public int getNotifierRetryDelay() {
        return notifierRetryDelay;
    }

    public synchronized String getReqId() {
        return "" + nextId++;
    }

    public String getSandboxTransferMethod() {
        return sandboxTransfertMethod;
    }

    public String getScratchDirPath() {
        return scratchDirPath;
    }

    public BLAHJobStatus getStatus(String blahJobId, String localUser) throws BLAHException {
        BLAHCommand jobStatusCommand = new BLAHCommand(BLAHCommand.BLAH_JOB_STATUS, blahJobId, getReqId());
        execute(jobStatusCommand, localUser);

        List<String> result = jobStatusCommand.getResultList();

        if (result == null || result.size() == 0) {
            throw new BLAHException("result not available!");
        }

        return new BLAHJobStatus(result.get(0));
    }

    public BLAHJobStatus getStatusAll(String localUser) throws BLAHException {
        BLAHCommand jobStatusCommand = new BLAHCommand(BLAHCommand.BLAH_JOB_STATUS_ALL, null, getReqId());
        execute(jobStatusCommand, localUser);

        List<String> result = jobStatusCommand.getResultList();

        if (result == null || result.size() == 0) {
            throw new BLAHException("result not available!");
        }

        return new BLAHJobStatus(result.get(0));
    }

    public BLAHJobStatus getStatusSelect(String classadExpr, String localUser) throws BLAHException {
        BLAHCommand jobStatusCommand = new BLAHCommand(BLAHCommand.BLAH_JOB_STATUS_SELECT, classadExpr, getReqId());
        execute(jobStatusCommand, localUser);

        List<String> result = jobStatusCommand.getResultList();

        if (result == null || result.size() == 0) {
            throw new BLAHException("result not available!");
        }

        return new BLAHJobStatus(result.get(0));
    }
    
    public void init() throws BLAHException {
        try {
            blahProcess = new BLAHProcess(executablePath, scratchDirPath, commandTimeout);
            
            List<BLAHNotifierInfo> list = getBLAHNotifierInfo();
            blahNotifierClientTable = new Hashtable<String, BLAHNotifierClient>(list.size());

            if (list.size() == 0) {
                logger.error("Unable to retrieve info about Notifier from BLAH => CREAM will not be able to submit jobs anymore! Please check if the blParser process is up and running and restart CREAM");
                return;
            }

            BLAHNotifierClient blahNotifierClient = null;
            
            for (BLAHNotifierInfo blahNotifierInfo : list) {
                blahNotifierClient = new BLAHNotifierClient();
                blahNotifierClient.setPrefix(prefix);
                blahNotifierClient.setRetryCount(notifierRetryCount);
                blahNotifierClient.setRetryDelay(notifierRetryDelay);
                blahNotifierClient.setLRMS(blahNotifierInfo.getLRMS());
                blahNotifierClient.setHost(blahNotifierInfo.getHost());
                blahNotifierClient.setPort(blahNotifierInfo.getPort());
                blahNotifierClient.setJobStatusChangeListener(jobStatusChangeListener);

                blahNotifierClientTable.put(blahNotifierClient.getLRMS(), blahNotifierClient);
            }
        } catch (BLAHException e) {
            logger.error(e.getMessage());
            throw new BLAHException(e.getMessage());
        }

        for (BLAHNotifierClient client : blahNotifierClientTable.values()) {
            client.start();
        }

        blahNotificationListener = new BLAHNotificationListener(notificationListenerPort);
        blahNotificationListener.setJobStatusChangeListener(jobStatusChangeListener);
        blahNotificationListener.startListener();
    }

    public boolean isBatchSystemSupported(String batchSystem) {
        if (batchSystem == null) {
            return false;
        }
        return blahNotifierClientTable.containsKey(batchSystem.toLowerCase());
    }

    public void quit() throws BLAHException {
        execute(new BLAHCommand(BLAHCommand.QUIT), null);
    }
            
    public void renewProxy(String blahJobId, String delegationCertPath, String localUser) throws BLAHException {
        renewProxy(blahJobId, localUser, delegationCertPath, null, false);
    }

    public void renewProxy(String blahJobId, String localUser, String delegationCertPath, String workerNode, boolean sendToWN) throws BLAHException {
        if (delegationCertPath == null) {
            throw new BLAHException("delegationCertPath not specified!");
        }

        BLAHCommand command = null;
        if (sendToWN) {
            if (workerNode == null) {
                throw new BLAHException("workerNode not specified!");
            }
            
            command = new BLAHCommand(BLAHCommand.BLAH_JOB_SEND_PROXY_TO_WORKER_NODE, blahJobId, getReqId());
            command.addParameter(delegationCertPath);
            command.addParameter(workerNode);
        } else {
            command = new BLAHCommand(BLAHCommand.BLAH_JOB_REFRESH_PROXY, blahJobId, getReqId());
            command.addParameter(delegationCertPath);
        }

        execute(command, localUser);
    }

    public void resume(String blahJobId, String localUser) throws BLAHException {
        execute(new BLAHCommand(BLAHCommand.BLAH_JOB_RESUME, blahJobId, getReqId()), localUser);
    }
    
    public void setAsynchronousMode(boolean on) throws BLAHException {
        execute(new BLAHCommand(on ? BLAHCommand.ASYNC_MODE_ON : BLAHCommand.ASYNC_MODE_OFF), null);
        asynchronousMode = on;
    }

    public boolean setCommandTimeout(int timeout) {
        if (commandTimeout < 0) {
            logger.warn("wrong command timeout value found, using the default value: " + commandTimeout + " sec");
            return false;
        }

        commandTimeout = timeout;
        return true;
    }

    public boolean setExecutablePath(String path) {
        if (path == null) {
            logger.warn("blahpd path not specified, usign default value: " + executablePath);
            return false;
        }

        executablePath = path;
        return true;
    }

    public void setHostSMPSize(int hostSMPSize) {
        if (hostSMPSize <= 0) {
            logger.warn("wrong value for the \"HOST_SMP_SIZE\" parameter, using the default value: " + hostSMPSize);
        }

        this.hostSMPSize = hostSMPSize;
    }

    public void setPrefix(String prefix) {
        String defaultPrefix = "crxxx_";

        if (prefix != null) {
            if (prefix.length() != 6) {
                logger.warn("the prefix must be 6 chars long, usign the default value: " + defaultPrefix);
                prefix = defaultPrefix;
            } else if (!prefix.startsWith("cr")) {
                logger.warn("the prefix must start with \"cr\", using the default value: " + defaultPrefix + ")");
                prefix = defaultPrefix;
            } else if (!prefix.endsWith("_")) {
                logger.warn("the prefix must end with \'_\', using the default value: " + defaultPrefix + ")");
                prefix = defaultPrefix;
            } else {
                this.prefix = prefix;
            }
        } else {
            logger.warn("prefix not specified, using the default value: " + defaultPrefix + ")");
            prefix = defaultPrefix;
        }
    }

    public BLAHNotifierClient getBLAHNotifierClient(String batchSystem) {
        BLAHNotifierClient client = null;

        if (batchSystem != null) {
            client = blahNotifierClientTable.get(batchSystem.toLowerCase());
        }

        return client;
    }

    public void setNotificationListenerPort(int port) {
        this.notificationListenerPort = port;
    }

    public boolean setNotifierRetryCount(int retryCount) {
        if (retryCount < 0) {
            logger.warn("wrong retry count value found, using the default value: " + notifierRetryCount + " sec");
            return false;
        }

        notifierRetryCount = retryCount;
        return true;
    }

    public boolean setNotifierRetryDelay(int retryDelay) {
        if (retryDelay < 0) {
            logger.warn("wrong retry delay value found, using the default value: " + notifierRetryDelay + " sec");
            return false;
        }

        notifierRetryDelay = retryDelay;
        return true;
    }

    public boolean setSandboxTransferMethod(String transferMethod) {
        if (transferMethod == null || (!SANDBOX_TRANSFER_METHOD_LRMS.equalsIgnoreCase(transferMethod) && !SANDBOX_TRANSFER_METHOD_GSIFTP.equalsIgnoreCase(transferMethod))) {
            logger.warn("wrong sandbox transfer method foud, usign default value: " + SANDBOX_TRANSFER_METHOD_LRMS);
            sandboxTransfertMethod = SANDBOX_TRANSFER_METHOD_LRMS;
            return false;
        }

        sandboxTransfertMethod = transferMethod;
        return true;
    }

    public boolean setScratchDirPath(String path) {
        if (path == null) {
            logger.warn("blahpd scratch dir path not specified, usign the default value: " + scratchDirPath);
            return false;
        }

        File dir = new File(scratchDirPath);
        if (!dir.exists()) {
            return dir.mkdirs();
        }

        scratchDirPath = path;
        return true;
    }

    public String submit(BLAHJob blahJob) throws BLAHException {
        if (blahJob == null) {
            throw new BLAHException("command not specified!");
        }

        if (blahJob.getLocalUser() == null) {
            throw new BLAHException("localUser not specified!");
        }

        if (blahJob.getJobId() == null) {
            throw new BLAHException("jobId not specified!");
        }

        if (blahJob.getQueue() == null) {
            throw new BLAHException("queue not specified!");
        }

        if (blahJob.getLRMS() == null) {
            throw new BLAHException("lrms not specified!");
        }

        if (blahJob.getExecutableFile() == null) {
            throw (new BLAHException("executableFile not specified!"));
        }

/*        if (blahJob.getNodeNumber() < 0) {
            throw (new BLAHException("wrong nodeNumber value!"));
        }

        if (blahJob.getHostNumber() < 1) {
            throw new BLAHException("wrong hostNumber value: use hostNumber >= 1");
        }

        if (blahJob.getVirtualOrganisation() == null) {
            throw new BLAHException("virtualOrganisation not specified");
        }
*/
        if (blahJob.getLocalUser() == null) {
            throw new BLAHException("localUser not specified");
        }

        if (blahJob.getDelegationCertPath() != null) {
            if (blahJob.getUserDN() == null) {
                throw new BLAHException("userDN not specified");
            }

            if (blahJob.getUserFQAN() == null) {
                throw new BLAHException("userFQAN not specified");
            }
        }

        String jobId = blahJob.getJobId();
        String clientJobId = jobId.substring(jobId.length() - 9);
        String queue = blahJob.getQueue();
        int smpGranularity = blahJob.getSmpGranularity();
        int hostNumber = blahJob.getHostNumber();
        int gpuNumber = blahJob.getGPUNumber();
        int micNumber = blahJob.getMICNumber();
        String gpuMode = blahJob.getGPUMode();
        String gpuModel = blahJob.getGPUModel();

        StringBuffer blahpAD = new StringBuffer("[Cmd=\"");
        blahpAD.append(blahJob.getExecutableFile());
        blahpAD.append("\";gridType=\"").append(blahJob.getLRMS());
        blahpAD.append("\";uniquejobid=\"").append(jobId);
        blahpAD.append("\";Out=\"").append(blahJob.getStandardOutputFile());
        blahpAD.append("\";Err=\"").append(blahJob.getStandardErrorFile());
 
        if (blahJob.getVirtualOrganisation() != null) {
            blahpAD.append("\";VirtualOrganisation=\"").append(blahJob.getVirtualOrganisation());
        }
        
        if (blahJob.getDelegationCertPath() != null) {
            blahpAD.append("\";x509userproxy=\"").append(blahJob.getDelegationCertPath());
            blahpAD.append("\";x509UserProxySubject=\"").append(blahJob.getUserDN());
            blahpAD.append("\";x509UserProxyFQAN=\"").append(blahJob.getUserFQAN());
        }

        if (blahJob.getMwVersion() != null) {
            blahpAD.append("\";Env=\"EDG_MW_VERSION=").append(blahJob.getMwVersion());
        }

        if (blahJob.getCeId() != null) {
            blahpAD.append("\";ceid=\"").append(blahJob.getCeId());
        }

        if (prefix == null) {
            blahpAD.append("\";ClientJobId=\"").append(clientJobId).append("\"");
        } else {
            blahpAD.append("\";ClientJobId=\"").append(prefix).append(clientJobId).append("\"");
        }
/*        
        if (gpuNumber > 0) {        
            blahpAD.append("\";GPUNumber=\"").append(gpuNumber).append("\"");
            
            if (gpuMode != null) {        
                blahpAD.append("\";GPUMode=\"").append(gpuMode).append("\"");
            }
        }    
*/
        if (micNumber > 0) {
            blahpAD.append("\";MICNumber=\"").append(micNumber).append("\"");
        }

        if (gpuNumber > 0) {
            blahpAD.append("\";GPUNumber=\"").append(gpuNumber).append("\"");
        }

        if (gpuMode != null) {
            blahpAD.append("\";GPUMode=\"").append(gpuMode).append("\"");
        }

        if (gpuModel != null) {
            blahpAD.append("\";GPUModel=\"").append(gpuModel).append("\"");
        }
 
        if (blahJob.isWholeNodes()) {
            blahpAD.append(";WholeNodes=true;HostSMPSize=").append(hostSMPSize);
        } else {
            blahpAD.append(";WholeNodes=false");

            if (blahJob.getNodeNumber()>0) {
                blahpAD.append(";NodeNumber=").append(blahJob.getNodeNumber());
            } else {
                blahpAD.append(";NodeNumber=1");
            }

            if (smpGranularity > 0 && hostNumber > 0) {
                throw new BLAHException("the SMPGranularity and HostNumber attributes cannot be specified together when WholeNodes=false");
            }
        }

        if (smpGranularity > 0) {
            blahpAD.append(";SMPGranularity=").append(smpGranularity);
        }

        if (hostNumber > 0) {
            blahpAD.append(";HostNumber=").append(hostNumber);
        }

        List<String> arguments = blahJob.getArguments();
        if (!arguments.isEmpty()) {
            blahpAD.append(";Args=\"'");
            
            for (String arg : arguments) {
                blahpAD.append(arg).append(":");
            }
            
            blahpAD.replace(blahpAD.length()-1, blahpAD.length(), "'\"");
        }

        if (blahJob.getCeRequirements() != null) {
            String req = blahJob.getCeRequirements();
            req = req.replaceAll(" ", "\\\\ ");
            req = req.replaceAll("\"", "\\\\\"");

            blahpAD.append(";CERequirements=\"").append(req).append("\"");
        }

        if (blahJob.getGridJobId() != null) {
            blahpAD.append(";edg_jobid=\"").append(blahJob.getGridJobId()).append("\"");
        }

        if (SANDBOX_TRANSFER_METHOD_LRMS.equalsIgnoreCase(sandboxTransfertMethod)) {
            if (blahJob.getIwd() != null) {
                blahpAD.append(";iwd=\"").append(blahJob.getIwd()).append("\"");
            }

            if (blahJob.getTransferInput() != null) {
                blahpAD.append(";TransferInput=\"").append(blahJob.getTransferInput()).append("\"");
            }

            if (blahJob.getTransferOutput() != null) {
                blahpAD.append(";TransferOutput=\"").append(blahJob.getTransferOutput()).append("\"");
            }

            if (blahJob.getTransferOutputRemaps() != null) {
                blahpAD.append(";TransferOutputRemaps=\"").append(blahJob.getTransferOutputRemaps()).append("\"");
            }
        }

        blahpAD.append((queue != null && !queue.equals("")) ? ";queue=\"" + queue + "\"]" : "]");

        BLAHCommand jobSubmitCommand = new BLAHCommand(BLAHCommand.BLAH_JOB_SUBMIT, null, getReqId());
        jobSubmitCommand.addParameter(blahpAD.toString());
        
        execute(jobSubmitCommand, blahJob.getLocalUser());
        
        List<String> result = jobSubmitCommand.getResultList();

        if (result == null || result.size() == 0) {
            throw new BLAHException("blahJobId not available!");
        }

        return result.get(0).trim();
    }

    public void suspend(String blahJobId, String localUser) throws BLAHException {
        execute(new BLAHCommand(BLAHCommand.BLAH_JOB_HOLD, blahJobId, getReqId()), localUser);
    }

    public void terminate() {        
        logger.info("teminate invoked!");

        try {
            logger.info("sending the QUIT command...");
            quit();
            logger.info("sending the QUIT command... done!");
        } catch (BLAHException e) {
            logger.error("error on sending the QUIT command: " + e.getMessage());
        }
        
        if (blahNotificationListener != null) {
            blahNotificationListener.stopListener();
            logger.info("LRMSEventsListener stopped!");
        }

        if (blahNotifierClientTable != null) {
            for (BLAHNotifierClient client : blahNotifierClientTable.values()) {
                client.terminate();
            }
        }
        logger.info("teminated!");
    }
    
    public static void main(String[] args) {
        BLAHClient blahClient = new BLAHClient();
        
        List<BLAHNotifierInfo> list;
        try {
            blahClient.setAsynchronousMode(false);
            
            list = blahClient.getBLAHNotifierInfo();
            
            for (BLAHNotifierInfo blahNotifierInfo : list) {
                System.out.println("blahNotifier [lrms=" + blahNotifierInfo.getLRMS() + "; host=" + blahNotifierInfo.getHost() + "; port=" + blahNotifierInfo.getPort() + "]");   
            }

            blahClient.quit();
        } catch (BLAHException e1) {
            logger.error(e1.getMessage(), e1);
            blahClient.terminate();
        }

    }

    public void setJobStatusChangeListener(BLAHJobStatusChangeListener jobStatusChangeListener) {
        this.jobStatusChangeListener = jobStatusChangeListener;
    }

    public BLAHJobStatusChangeListener getJobStatusChangeListener() {
        return jobStatusChangeListener;
    }
}
