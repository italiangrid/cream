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
 * Authors: Luigi Zangrando <zangrando@pd.infn.it>
 *
 */

package org.glite.ce.cream.jobmanagement.cmdexecutor.blah;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.StringTokenizer;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.glite.ce.commonj.db.DatabaseException;
import org.glite.ce.commonj.db.DatasourceManager;
import org.glite.ce.commonj.utils.BooleanLock;
import org.glite.ce.cream.configuration.ServiceConfig;
import org.glite.ce.cream.jobmanagement.db.JobDBImplementation;
import org.glite.ce.creamapi.cmdmanagement.CommandException;
import org.glite.ce.creamapi.cmdmanagement.CommandExecutorException;
import org.glite.ce.creamapi.cmdmanagement.CommandResult;
import org.glite.ce.creamapi.jobmanagement.Job;
import org.glite.ce.creamapi.jobmanagement.cmdexecutor.AbstractJobExecutor;
import org.glite.ce.creamapi.jobmanagement.db.JobDBInterface;
import org.glite.jdl.Jdl;

public class BLAHExecutor extends AbstractJobExecutor {
    private static final Logger logger = Logger.getLogger(BLAHExecutor.class.getName());
    private static int blCommandTimeout = -1;
    private Hashtable<String, BLParserClient> blParserClientTable;
    private LRMSEventsProcessor lrmsEventsProcessor = null;
    private LRMSEventsListener eventsListener = null;
    private String blahJobIdPrefix = null;
    private static int nextId = 1;

    private static class MyThreadLocal extends ThreadLocal {
        private Runtime rt = Runtime.getRuntime();
        private BLAHExecutor blahExec;
        private final String cmdResString = (new BlahCommand(BlahCommand.RESULTS)).toString();

        public MyThreadLocal(BLAHExecutor blahExec) {
            this.blahExec = blahExec;
        }

        protected Object initialValue() {
            return makeBLAHProcess();
        }

        private Process makeBLAHProcess() {
            try {
                String scratchDir = blahExec.getParameterValueAsString("JOBS_SCRATCH_DIR");
                if (scratchDir != null) {
                    File dir = new File(scratchDir);
                    if (!dir.exists()) {
                        dir.mkdirs();
                    }

                    dir = null;
                }

                Map<String, String> envMap = new HashMap<String, String>(System.getenv());
                envMap.put("TERM", "vanilla");

                String[] env = new String[envMap.size()];
                int index = 0;

                for (String key : envMap.keySet()) {
                    env[index++] = key.concat("=").concat(envMap.get(key));
                }

                Process proc = rt.exec(blahExec.getParameterValueAsString("BLAHP_BIN_PATH"), env, new File(scratchDir));

                BufferedReader readIn = new BufferedReader(new InputStreamReader(proc.getInputStream()));
                readIn.readLine();

                logger.debug("makeBLAHProcess: made a new blah process");

                return proc;
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }

        private void killBLAHProcess() {
            Process proc = (Process) super.get();
            if (proc == null) {
                return;
            }

            try {
                proc.getInputStream().close();
            } catch (Exception e) {
            }

            try {
                proc.getOutputStream().close();
            } catch (Exception e) {
            }

            try {
                proc.getErrorStream().close();
            } catch (Exception e) {
            }

            try {
                proc.destroy();
            } catch (Exception e) {
            }

            logger.debug("killBLAHProcess: blah process killed");
        }

        private void checkTimeout(Calendar timeout) throws BLAHException {
            if (timeout != null && Calendar.getInstance().after(timeout)) {
                killBLAHProcess();
                super.set(makeBLAHProcess());
                throw new BLAHException("blah error: send command timeout");
            }
        }

        public String[] sendCommand(BlahCommand[] cmd) throws BLAHException {
            if (cmd == null) {
                throw new BLAHException("command not specified!");
            }

            boolean exit = true;
            String[] result = null;

            do {
                Process proc = (Process) super.get();
                exit = true;

                result = new String[cmd.length];

                BufferedReader readIn = new BufferedReader(new InputStreamReader(proc.getInputStream()));
                BufferedReader readErr = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
                BufferedWriter writeOut = new BufferedWriter(new OutputStreamWriter(proc.getOutputStream()));
                // boolean foundR = false;
                Calendar timeout = null;

                try {
                    for (int i = 0; i < cmd.length; i++) {
                        if (cmd[i] == null) {
                            throw new BLAHException("BLAHCommand not specified!");
                        }

                        logger.debug(cmd[i].toString());

                        if (blCommandTimeout != -1) {
                            timeout = Calendar.getInstance();
                            timeout.add(Calendar.SECOND, blCommandTimeout);
                        }

                        String cmdString = cmd[i].toString();
                        writeOut.write(cmdString);
                        writeOut.flush();

                        if (readErr.ready()) {
                            System.out.println(Calendar.getInstance().getTime() + " - BLAH stderr: " + getBlahOutput(readErr, timeout));
                        }

                        result[i] = getBlahOutput(readIn, timeout);

                        if (result[i].startsWith("S")) {
                            if (cmd[i].getReqId() != null) {
                                BooleanLock lock = new BooleanLock(false);
                                boolean done = false;

                                while (!done) {
                                    checkTimeout(timeout);

                                    writeOut.write(cmdResString);
                                    writeOut.flush();

                                    // STDERR
                                    if (readErr.ready()) {
                                        System.out.println(Calendar.getInstance().getTime() + " - BLAH stderr: " + getBlahOutput(readErr, timeout));
                                    }

                                    if (parseResult(cmd[i], readIn, timeout)) {
                                        done = true;
                                    } else {
                                        lock.waitUntilTrue(1000);
                                    }
                                }
                            }
                        } else if (result[i].startsWith("E")) {
                            throw new BLAHException("blah error: " + result[i].replaceAll("\\\\", ""));
                            /*
                             * } else if (result[i].equalsIgnoreCase("R")) {
                             * foundR = true;
                             */
                        } else if (result[i].startsWith("F")) {
                            throw new BLAHException(cmd[i].getName() + " error");
                        }
                    }
                } catch (BLAHException ex) {
                    logger.error(ex.getMessage(), ex);
                    throw ex;
                } catch (Exception e) {
                    if (e.getMessage().equals("Broken pipe")) {
                        logger.warn("sendCommand: Caught a \"Broken pipe\" exception (maybe the blah process is dead!) I am going to make a new one");

                        super.set(makeBLAHProcess());
                        exit = false;
                    } else {
                        logger.error(e.getMessage());
                        throw new BLAHException(e.getMessage());
                    }
                }
            } while (!exit);

            return result;
        }

        private boolean parseResult(BlahCommand pendingCmd, BufferedReader readIn, Calendar timeout) throws BLAHException {
            if (pendingCmd == null || readIn == null) {
                throw new BLAHException("invalid argument");
            }

            String result = getBlahOutput(readIn, timeout);

            int items = 0;
            StringTokenizer strtok = new StringTokenizer(result);
            if (strtok.countTokens() == 2 && strtok.nextToken().equals("S")) {
                try {
                    items = Integer.parseInt(strtok.nextToken());
                } catch (NumberFormatException e) {
                    throw (new BLAHException(e.getMessage()));
                }
            }

            if (items == 0) {
                return false;
            }

            try {
                // Fill the cache
                for (int i = 0; i < items; i++) {
                    String line = getBlahOutput(readIn, timeout);

                    if (line != null) {
                        int index = line.indexOf(" ");
                        if (index > -1) {
                            String reqId = line.substring(0, index);
                            int subId = 0;
                            line = line.substring(index + 1, line.length());

                            index = reqId.indexOf(".");
                            if (index > -1) {
                                subId = Integer.parseInt(reqId.substring(index + 1, reqId.length()));
                                reqId = reqId.substring(0, index);
                            }

                            index = line.indexOf(" ");
                            String errorCode = line.substring(0, index);
                            String value = line.substring(index + 1, line.length());

                            if (pendingCmd != null) {
                                BLAHCommandResult cr = pendingCmd.getResultAt(subId);
                                cr.setSuccessfull(errorCode.startsWith("0"));

                                if (!cr.isSuccessfull()) {
                                    cr.setResult(value.replaceAll("\\\\", ""));
                                } else {
                                    index = value.indexOf("error");
                                    if (index > 0 && value.length() >= 10) {
                                        value = value.substring(10);
                                    }
                                    cr.setResult(value);
                                }

                                pendingCmd.setResult(subId, cr);
                            }
                        }
                    }
                }
            } catch (IndexOutOfBoundsException e) {
                e.printStackTrace();
            }
            return true;
        }

        private String getBlahOutput(BufferedReader readIn, Calendar timeout) throws BLAHException {
            try {
                BooleanLock lock = new BooleanLock(false);

                while (!readIn.ready()) {
                    checkTimeout(timeout);

                    lock.waitUntilTrue(1000);
                }
                String result = readIn.readLine();
                logger.debug("getBlahOutput: " + result);
                return result;
            } catch (Exception e) {
                throw (new BLAHException("getBlahOutput error: " + e.getMessage()));
            }
        }
    }

    // Define/create thread local variable
    static MyThreadLocal blahProcess = null;

    public BLAHExecutor() throws CommandExecutorException {
        super("BLAHExecutor");
        
        dataSourceName = JobDBInterface.JOB_DATASOURCE_NAME;

        addParameter("BLAHP_BIN_PATH", "/opt/glite/bin/blahpd");
        addParameter("GLEXEC_BIN_PATH", "/usr/share/tomcat5/glexec-wrapper.sh");
        addParameter("JOBS_SCRATCH_DIR", "/tmp/jobs/blaphd");
        addParameter("LRMS_EVENT_LISTENER_PORT", "9090");
        addParameter("BLPARSER_RETRY_COUNT", "100");
        addParameter("BLPARSER_RETRY_DELAY", "60000");
        addParameter(JOB_WRAPPER_TEMPLATE_PATH, ServiceConfig.getConfiguration().getConfigurationDirectory());
 
        if (containsParameterKey("HOST_SMP_SIZE")) {
            int hostSMPsize = 0;
            
            try {
                hostSMPsize = Integer.parseInt(getParameterValueAsString("HOST_SMP_SIZE"));
            } catch (Throwable t) {
                logger.warn("wrong value for the \"HOST_SMP_SIZE\" parameter, using the default value: HOST_SMP_SIZE=1");
                addParameter("HOST_SMP_SIZE", "1");
            }
            
            if (hostSMPsize <= 0) {
                logger.warn("wrong value for the \"HOST_SMP_SIZE\" parameter, using the default value: HOST_SMP_SIZE=1");
                addParameter("HOST_SMP_SIZE", "1");                
            }
        } else {
            logger.info("parameter \"HOST_SMP_SIZE\" not defined, using the default value: HOST_SMP_SIZE=1");
            addParameter("HOST_SMP_SIZE", "1");
        }

        blahProcess = new MyThreadLocal(this);

        blParserClientTable = new Hashtable<String, BLParserClient>(1);

        lrmsEventsProcessor = new LRMSEventsProcessor(this);
    }

    public void destroy() {
        logger.info("destroy invoked!");
        if (eventsListener != null) {
            eventsListener.stopListener();
            // eventsListener.interrupt();
            logger.info("LRMSEventsListener stopped!");
        }

        for (Enumeration<BLParserClient> e = blParserClientTable.elements(); e.hasMoreElements();) {
            BLParserClient client = (BLParserClient) e.nextElement();
            client.terminate();
            //logger.info("BLParserClient " + client.getBlParserHost() + ":" + client.getBlParserPort() + " terminated!");
        }

        super.destroy();
        logger.info("destroyed!");
    }

    public void initExecutor() throws CommandExecutorException {
        super.initExecutor();

        ServiceConfig serviceConfig = ServiceConfig.getConfiguration();
        if (serviceConfig == null) {
            throw new CommandExecutorException("Configuration error: cannot initialize the ServiceConfig");
        }
        
        HashMap<String, DataSource> dataSources = serviceConfig.getDataSources();
        boolean found = false;
        
        if (dataSources != null) {
            for (String name : dataSources.keySet()) {
                if (name.equals(dataSourceName)) {
                    if (DatasourceManager.addDataSource(name, dataSources.get(name))) {
                        logger.info("new dataSource \"" + name + "\" added to the DatasourceManager");
                    } else {
                        logger.info("the dataSource \"" + name + "\" already exist!");
                    }
                    found = true;
                    break;                     
                }
            }
        }

        if (dataSources == null || !found) {
            throw new CommandExecutorException("Datasource \"" + dataSourceName + "\" not found!");
        }    

        JobDBInterface jobDB = null;

        try {
            jobDB = JobDBImplementation.getInstance();
        } catch (DatabaseException e) {
            throw new CommandExecutorException(e.getMessage());
        }

        setJobDB(jobDB);

        int listenerPort = 9000;

        String port = getParameterValueAsString("LRMS_EVENT_LISTENER_PORT");
        if (port != null) {
            listenerPort = Integer.parseInt(port);
        }
        eventsListener = new LRMSEventsListener(lrmsEventsProcessor, listenerPort);

        String defaultBlahJobIdPrefix = "cream_";
        blahJobIdPrefix = getParameterValueAsString("BLAH_JOBID_PREFIX");

        if (blahJobIdPrefix != null) {
            if (blahJobIdPrefix.length() != 6) {
                logger.warn("initExecutor: wrong BLAH_JOBID_PREFIX value length found (" + blahJobIdPrefix
                        + "): the prefix must be 6 chars long! Going on with the default value (" + defaultBlahJobIdPrefix + ")");
                blahJobIdPrefix = defaultBlahJobIdPrefix;
            } else if (!blahJobIdPrefix.startsWith("cr")) {
                logger.warn("initExecutor: wrong BLAH_JOBID_PREFIX value found (" + blahJobIdPrefix + "): the prefix must start with \"cr\". Going on with the default value"
                        + defaultBlahJobIdPrefix + ")");
                blahJobIdPrefix = defaultBlahJobIdPrefix;
            } else if (!blahJobIdPrefix.endsWith("_")) {
                logger.warn("initExecutor: wrong BLAH_JOBID_PREFIX value found (" + blahJobIdPrefix + "): the prefix must end with \'_\'. Going on with the default value"
                        + defaultBlahJobIdPrefix + ")");
                blahJobIdPrefix = defaultBlahJobIdPrefix;
            }
        }

        String value = null;

        if (containsParameterKey("BLAH_COMMAND_TIMEOUT")) {
            value = getParameterValueAsString("BLAH_COMMAND_TIMEOUT");
            try {
                blCommandTimeout = Integer.parseInt(value);
            } catch (NumberFormatException nfe) {
                blCommandTimeout = 300; // 300 sec = 5 minutes
                logger.warn("initExecutor: wrong BLAH_COMMAND_TIMEOUT value (" + value + ") found. Going on with the default value: 300 seconds");
            } finally {
                value = null;
            }
        }

        int blParserRetryCount = 100;
        if (containsParameterKey("BLPARSER_RETRY_COUNT")) {
            value = getParameterValueAsString("BLPARSER_RETRY_COUNT");

            try {
                blParserRetryCount = Integer.parseInt(value);
            } catch (NumberFormatException nfe) {
                logger.warn("initExecutor: wrong BLPARSER_RETRY_COUNT value (" + value + ") found. Going on with the default value: " + blParserRetryCount);
            } finally {
                value = null;
            }
        }

        int blParserRetryDelay = 60000; // 60000 milliseconds = 1 minute
        if (containsParameterKey("BLPARSER_RETRY_DELAY")) {
            value = getParameterValueAsString("BLPARSER_RETRY_DELAY");

            try {
                blParserRetryDelay = Integer.parseInt(value);
            } catch (NumberFormatException nfe) {
                logger.warn("initExecutor: wrong BLPARSER_RETRY_DELAY value (" + value + ") found. Going on with the default value: 1 minute");
            } finally {
                value = null;
            }
        }

        // blParserRetryDelay = blParserRetryDelay >= 5000 ? blParserRetryDelay
        // : 5000;

        try {
            ArrayList<String[]> list = getBlahLRMS();

            if (list.size() == 0) {
                logger.error("Unable to retrieve info about blParser from BLAH => CREAM will not be able to submit jobs anymore! Please check if the blParser process is up and running and restart CREAM");

                return;
            }

            for (int i = 0; i < list.size(); i++) {
                String[] blParserInfo = (String[]) list.get(i);
                BLParserClient blParserClient = new BLParserClient(this, lrmsEventsProcessor, blahJobIdPrefix, blParserInfo[0], blParserInfo[1], blParserInfo[2],
                        blParserRetryCount, blParserRetryDelay, jobDB);
                blParserClientTable.put(blParserInfo[0].toLowerCase(), blParserClient);
            }
        } catch (BLAHException e) {
            logger.error(e.getMessage());
            throw new CommandExecutorException(e.getMessage());
        }

        // start executor
        for (Enumeration<BLParserClient> e = blParserClientTable.elements(); e.hasMoreElements();) {
            BLParserClient client = (BLParserClient) e.nextElement();
            try {
                client.initBLParserClient();
            } catch (Exception e1) {
                logger.error("startConnector", e1);
            }
        }

        eventsListener.startListener();
    }

    protected ArrayList<String[]> getBlahLRMS() throws BLAHException {
        BlahCommand cmd = new BlahCommand(BlahCommand.BLAH_GET_HOSTPORT, "0");
        blahProcess.sendCommand(new BlahCommand[] { cmd });

        BLAHCommandResult commandResult = cmd.getResultAt(0);
        ArrayList<String[]> list = new ArrayList<String[]>();

        if (commandResult != null) {
            StringTokenizer st = new StringTokenizer(commandResult.getResult());

            while (st.hasMoreTokens()) {
                String token = st.nextToken().trim();
                int i = token.indexOf("/");
                try {
                    if (i > 0) {
                        String[] blParserInfo = new String[3];
                        blParserInfo[0] = token.substring(0, i).toLowerCase();

                        int j = token.indexOf(":");
                        if (j > 0) {
                            blParserInfo[1] = token.substring(i + 1, j);
                            blParserInfo[2] = token.substring(j + 1, token.length());
                        }

                        list.add(blParserInfo);
                    }
                } catch (IndexOutOfBoundsException e) {
                    e.printStackTrace();
                }
            }
        }

        return list;
    }

    protected String[] getBlahHostPort(String batchSystem) {
        try {
            ArrayList<String[]> list = getBlahLRMS();
            for (int i = 0; i < list.size(); i++) {
                String[] blParserInfo = (String[]) list.get(i);
                if (blParserInfo[0].equalsIgnoreCase(batchSystem)) {
                    return blParserInfo;
                }
            }
        } catch (Exception e) {
        }

        return null;
    }

    public synchronized String getReqId() {
        return "" + nextId++;
    }

    public CommandResult getStatus(Job job) throws CommandException {
        BLAHCommandResult[] blahResult = command(BlahCommand.BLAH_JOB_STATUS, job, null, false);

        if (blahResult == null || blahResult.length == 0) {
            throw new CommandException("BLAH error: no results found!");
        }
        if (blahResult[0].isSuccessfull()) {
            CommandResult result = new CommandResult();

            result.addParameter("JOB_STATUS", blahResult[0].getResult());
            return result;
        } else {
            throw new CommandException("BLAH error: " + blahResult[0].getResult());
        }
    }

    public void cancel(Job job) throws CommandException {
        command(BlahCommand.BLAH_JOB_CANCEL, job, null, false);
    }

    public void resume(Job job) throws CommandException {
        command(BlahCommand.BLAH_JOB_RESUME, job, null, false);
    }

    public CommandResult submit(Job job) throws CommandException {
        CommandResult result = new CommandResult();

        if (job == null) {
            throw new CommandException("job not defined!");
        }

        if (job.getNodeNumber() < 0) {
            throw (new CommandException("wrong NodeNumber value!"));
        }

        String batchSystem = job.getBatchSystem();
        if (batchSystem == null) {
            throw new CommandException("BatchSystem not defined!");
        }

        if (!isBatchSystemSupported(batchSystem)) {
            throw new CommandException("BatchSystem \"" + batchSystem + "\" not supported!");
        }

        BLParserClient blParserClient = (BLParserClient) blParserClientTable.get(batchSystem.toLowerCase());

        if (!blParserClient.isConnected()) {
            throw new CommandException("The job cannot be submitted because the blparser service is not alive");
        }

        if (job.containsExtraAttribute(Jdl.HOST_NUMBER)) {
            try {
                if (Integer.parseInt(job.getExtraAttribute(Jdl.HOST_NUMBER)) < 1) {
                    throw new CommandException("wrong value for the HostNumber attribute: use HostNumber >= 1");
                }
            } catch (Exception ex) {
                throw new CommandException("wrong value for the HostNumber attribute: use HostNumber >= 1");                        
            }
        }

        String workingDirPath = job.getWorkingDirectory();
        if (workingDirPath == null) {
            throw (new CommandException("workingDirPath not set!"));
        }

        String queue = job.getQueue();
        String jobId = job.getId();
        String clientJobId = jobId.substring(jobId.length() - 9);
        String jobWrapperFileName = "/" + jobId + "_jobWrapper.sh";

        StringBuffer blahpAD = new StringBuffer("[Cmd=\"");
        blahpAD.append(workingDirPath);
        blahpAD.append(jobWrapperFileName);
        blahpAD.append("\";gridType=\"");
        blahpAD.append(batchSystem);
        blahpAD.append("\";uniquejobid=\"");
        blahpAD.append(jobId);
        blahpAD.append("\";Out=\"");
        blahpAD.append(workingDirPath);
        blahpAD.append("/StandardOutput\";Err=\"");
        blahpAD.append(workingDirPath);
        blahpAD.append("/StandardError\";x509userproxy=\"").append(job.getDelegationProxyCertPath()).append("\"");
        blahpAD.append(";VirtualOrganisation=\"").append(job.getVirtualOrganization()).append("\"");

        String userDN = job.getExtraAttribute("USER_DN_X500");
        if (userDN != null) {
            blahpAD.append(";x509UserProxySubject=\"").append(userDN).append("\"");
        }

        if (job.containsExtraAttribute("USER_FQAN")) {
            blahpAD.append(";x509UserProxyFQAN=\"").append(job.getExtraAttribute("USER_FQAN")).append("\"");
        }

        if (job.containsExtraAttribute(Jdl.MW_VERSION)) {
            blahpAD.append(";Env=\"EDG_MW_VERSION=").append(job.getExtraAttribute(Jdl.MW_VERSION)).append("\"");
        }

        if (job.getCeId() != null) {
            blahpAD.append(";ceid=\"").append(job.getCeId()).append("\"");
        }

        if (blahJobIdPrefix == null) {
            blahpAD.append(";ClientJobId=\"").append(clientJobId).append("\"");
        } else {
            blahpAD.append(";ClientJobId=\"").append(blahJobIdPrefix).append(clientJobId).append("\"");
        }

        if (job.containsExtraAttribute(Jdl.WHOLE_NODES)) {
            if ("TRUE".equalsIgnoreCase(job.getExtraAttribute(Jdl.WHOLE_NODES))) {
                if (!containsParameterKey("HOST_SMP_SIZE")) {
                    throw new CommandException("HOST_SMP_SIZE parameter not found!");
                }

                blahpAD.append(";WholeNodes=true;HostSMPSize=").append(getParameterValueAsString("HOST_SMP_SIZE"));
            } else {
                blahpAD.append(";NodeNumber=").append(job.getNodeNumber());
                
                if (job.containsExtraAttribute(Jdl.SMP_GRANULARITY) && job.containsExtraAttribute(Jdl.HOST_NUMBER)) {
                    throw new CommandException("the SMPGranularity and HostNumber attributes cannot be specified together when WholeNodes=false");
                }
            }

            if (job.containsExtraAttribute(Jdl.SMP_GRANULARITY)) {
                blahpAD.append(";SMPGranularity=").append(job.getExtraAttribute(Jdl.SMP_GRANULARITY));
            }

            if (job.containsExtraAttribute(Jdl.HOST_NUMBER)) {
                blahpAD.append(";HostNumber=").append(job.getExtraAttribute(Jdl.HOST_NUMBER));
            }
        }

        if (job.getSequenceCode() != null) {
            blahpAD.append(";Args=\"").append(job.getSequenceCode()).append("\"");
        }

        if (job.getCeRequirements() != null) {
            String req = job.getCeRequirements();
            req = req.replaceAll(" ", "\\\\ ");
            req = req.replaceAll("\"", "\\\\\"");

            blahpAD.append(";CERequirements=\"").append(req).append("\"");
        }

        if (job.getICEId() != null && job.getICEId().length() > 0) {
            blahpAD.append(";edg_jobid=\"").append(job.getGridJobId()).append("\"");
        }

        if (LRMS_SANDBOX_TRANSFER_METHOD.equalsIgnoreCase(getParameterValueAsString(SANDBOX_TRANSFER_METHOD))) {
            if (job.containsExtraAttribute("iwd")) {
                blahpAD.append(";").append(job.getExtraAttribute("iwd"));
            }
            if (job.containsExtraAttribute("TransferInput")) {
                blahpAD.append(";").append(job.getExtraAttribute("TransferInput"));
            }
            if (job.containsExtraAttribute("TransferOutput")) {
                blahpAD.append(";").append(job.getExtraAttribute("TransferOutput"));
            }
            if (job.containsExtraAttribute("TransferOutputRemaps")) {
                blahpAD.append(";").append(job.getExtraAttribute("TransferOutputRemaps"));
            }
        }

        blahpAD.append((queue != null && !queue.equals("")) ? ";queue=\"" + queue + "\"]" : "]");

        BlahCommand[] cmd = null;
        if (job.getDelegationProxyCertPath() != null) {
            cmd = new BlahCommand[2];
            cmd[0] = new BlahCommand(BlahCommand.BLAH_SET_SUDO_ID);
            cmd[0].setParameter(job.getLocalUser());
        } else {
            cmd = new BlahCommand[1];
        }

        cmd[cmd.length - 1] = new BlahCommand(BlahCommand.BLAH_JOB_SUBMIT, getReqId(), job.getId());
        cmd[cmd.length - 1].setParameter(blahpAD.toString());
        cmd[cmd.length - 1].start();

        try {
            blahProcess.sendCommand(cmd);
        } catch (BLAHException e) {
            throw new CommandException(e.getMessage());
        }

        for (int i = 0; i < cmd.length; i++) {
            if (cmd[i].isExceptionOccurred()) {
                logger.error("BLAH error: " + cmd[i].getException() + " (jobId = " + jobId + ")");
                throw new CommandException("BLAH error: " + cmd[i].getException() + " (jobId = " + jobId + ")");
            }
        }

        BLAHCommandResult cr = cmd[cmd.length - 1].getResultAt(0);
        cr.setJobId(job.getId());

        if (cr.isSuccessfull()) {
            String resultMsg = cr.getResult();

            if (resultMsg == null) {
                throw new CommandException("BLAH id not found!");
            }
            // result.addParameter("LRMS_JOB_ID", "" + now.getTime());
            result.addParameter("LRMS_ABS_JOB_ID", resultMsg.trim());
        } else {
            throw new CommandException("BLAH error: " + cr.getResult() + " (jobId = " + jobId + ")");
        }

        return result;
    }

    public void suspend(Job job) throws CommandException {
        command(BlahCommand.BLAH_JOB_HOLD, job, null, false);
    }

    public boolean isBatchSystemSupported(String batchSystem) {
        if (batchSystem == null) {
            return false;
        }
        return blParserClientTable.containsKey(batchSystem.toLowerCase());
    }

    public void renewProxy(Job job) throws CommandException {
        renewProxy(job, false);
    }

    public void renewProxy(Job job, boolean sendToWN) throws CommandException {
        if (job == null) {
            throw new CommandException("job not defined!");
        }

        if (job.getDelegationProxyCertPath() == null) {
            throw new CommandException("delegation proxy path not defined!");
        }

        BLAHCommandResult[] result = null;

        if (sendToWN) {
            if (job.getWorkerNode() == null) {
                throw new CommandException("worker node name not specified!");
            }

            result = command(BlahCommand.BLAH_JOB_SEND_PROXY_TO_WORKER_NODE, job, new String[] { job.getDelegationProxyCertPath(), job.getWorkerNode() }, true);
        } else {
            result = command(BlahCommand.BLAH_JOB_REFRESH_PROXY, job, new String[] { job.getDelegationProxyCertPath() }, true);
        }

        if (result == null || result.length == 0 && !result[0].isSuccessfull()) {
            throw new CommandException("BLAH_JOB_REFRESH_PROXY failed!");
        }
    }

    private BLAHCommandResult[] command(int blahCommand, Job job, String[] parameter, boolean limitedProxy) throws CommandException {
        if (job == null) {
            throw new CommandException("job not defined!");
        }

        if (job.getLRMSAbsLayerJobId() == null || job.getLRMSAbsLayerJobId().equalsIgnoreCase("N/A")) {
            throw new CommandException("LRMSAbsLayerJobId not defined!");
        }

        if (job.getDelegationProxyCertPath() == null) {
            throw new CommandException("DelegationProxyCertPath not defined!");
        }

        BlahCommand[] cmd = new BlahCommand[2];
        cmd[0] = new BlahCommand(BlahCommand.BLAH_SET_SUDO_ID);
        cmd[0].setParameter(job.getLocalUser());
        cmd[1] = new BlahCommand(blahCommand, getReqId(), job.getLRMSAbsLayerJobId());
        cmd[1].setParameter(parameter);

        try {
            blahProcess.sendCommand(cmd);
        } catch (BLAHException e) {
            throw new CommandException(e.getMessage());
        }

        if (cmd[1].getResult().length > 0 && !cmd[1].getResultAt(0).isSuccessfull()) {
            throw new CommandException(cmd[1].getResultAt(0).getResult());
        }

        if (cmd[1].isExceptionOccurred()) {
            throw new CommandException(cmd[1].getException().getMessage());
        }

        return cmd[1].getResult();
    }
}
