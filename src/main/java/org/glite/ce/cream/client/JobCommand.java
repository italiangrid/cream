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

package org.glite.ce.cream.client;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.axis2.AxisFault;
import org.apache.axis2.databinding.utils.ConverterUtil;
import org.apache.commons.httpclient.protocol.Protocol;
import org.glite.ce.creamapi.ws.cream2.CREAMStub;
import org.glite.ce.creamapi.ws.cream2.types.BaseFaultType;
import org.glite.ce.creamapi.ws.cream2.types.Command;
import org.glite.ce.creamapi.ws.cream2.types.JobFilter;
import org.glite.ce.creamapi.ws.cream2.types.JobId;
import org.glite.ce.creamapi.ws.cream2.types.JobInfo;
import org.glite.ce.creamapi.ws.cream2.types.Lease;
import org.glite.ce.creamapi.ws.cream2.types.Result;
import org.glite.ce.creamapi.ws.cream2.types.ResultChoice_type0;
import org.glite.ce.creamapi.ws.cream2.types.Status;
import org.glite.ce.security.delegation.DelegationException;
import org.glite.ce.security.delegation.DelegationServiceStub;
import org.glite.security.delegation.GrDPX509Util;
import org.glite.security.delegation.GrDProxyDlgorOptions;
import org.glite.security.delegation.GrDProxyGenerator;
import org.glite.security.trustmanager.ContextWrapper;
import org.glite.security.trustmanager.axis2.AXIS2SocketFactory;

public abstract class JobCommand {
    public static final String EPR = "epr";
    public static final String STATUS = "status";
    public static final String DELEGATION_ID = "delegationId";
    public static final String LEASE_ID = "leaseId";
    public static final String LEASE_EXP_TIME = "leaseExpTime";
    public static final String FROM_DATE = "fromDate";
    public static final String TO_DATE = "toDate";
    public static final String ALL_JOBS = "allJobs";
    public static final String AUTOSTART = "autostart";
    public static final String DELEGATE = "delegate";
    public static final String PROXY = "proxy";
    public static final String JDL_FILE = "jdlFile";
    public static final String ACCEPT_NEW_JOBS = "acceptNewJobs";
    public static final String NEW_LEASE_ID = "newLeaseId";
    public static final String RENEW = "renew";

    private String proxy = null;
    private String epr = null;
    private String leaseId = null;
    private String newLeaseId = null;
    private String delegationId = null;
    private String[] jdlFileArray = null;
    private String[] status = null;
    private Calendar leaseExpTime = null;
    private Calendar fromDate = null;
    private Calendar toDate = null;
    private boolean allJobs = false;
    private boolean autostart = false;
    private boolean acceptNewJobs = true;
    private boolean delegate = false;
    private boolean renew = false;
    private JobId[] jobIdList = null;
    private List<String> options = null;

    public JobCommand(String[] args, List<String> options) throws IllegalArgumentException {
        if (args == null) {
            throw new IllegalArgumentException("arguments not specified!");
        }
        if (options == null) {
            throw new IllegalArgumentException("options not specified!");
        }

        this.options = options;

        parseArguments(args, this.options);

        execute();
    }

    protected abstract void execute();

    public CREAMStub getCREAMStub() throws AxisFault {
        if (epr == null) {
            throw new AxisFault("epr not specified!");
        }

        if (epr.startsWith("https")) {
            setSSLProperties();
        }

        return new CREAMStub(epr + "/ce-cream/services/CREAM2");
    }

    public DelegationServiceStub getDelegationServiceStub() throws AxisFault {
        if (epr == null) {
            throw new AxisFault("epr not specified!");
        }

        if (epr.startsWith("https")) {
            setSSLProperties();
        }

        return new DelegationServiceStub(epr + "/ce-cream/services/gridsite-delegation");
    }

    public String getDelegationId() {
        return delegationId;
    }

    public String getEpr() {
        return epr;
    }

    public Calendar getFromDate() {
        return fromDate;
    }

    public String[] getJdlFileArray() {
        return jdlFileArray;
    }

    public JobFilter getJobFilter() {
        JobFilter filter = new JobFilter();
        filter.setDelegationId(delegationId);
        filter.setLeaseId(leaseId);
        filter.setToDate(toDate);
        filter.setFromDate(fromDate);
        filter.setJobId(jobIdList);
        filter.setStatus(status);

        return filter;
    }

    public JobId[] getJobIdList() {
        return jobIdList;
    }

    public Calendar getLeaseExpTime() {
        return leaseExpTime;
    }

    public String getLeaseId() {
        return leaseId;
    }

    public String getNewLeaseId() {
        return newLeaseId;
    }

    public String[] getStatus() {
        return status;
    }

    public Calendar getToDate() {
        return toDate;
    }

    public boolean isAllJobs() {
        return allJobs;
    }

    public boolean isAutostart() {
        return autostart;
    }

    public boolean isDelegate() {
        return delegate;
    }

    public boolean isRenew() {
        return renew;
    }

    private void parseArguments(String[] args, List<String> options) {
        CmdLineParser parser = new CmdLineParser();
        CmdLineParser.Option getHelpOpt = parser.addBooleanOption('h', "help");
        CmdLineParser.Option eprOpt = null;
        CmdLineParser.Option allOpt = null;
        CmdLineParser.Option delegationIdOpt = null;
        CmdLineParser.Option statusOpt = null;
        CmdLineParser.Option leaseIdOpt = null;
        CmdLineParser.Option newLeaseIdOpt = null;
        CmdLineParser.Option leaseExpTimeOpt = null;
        CmdLineParser.Option fromOpt = null;
        CmdLineParser.Option toOpt = null;
        CmdLineParser.Option autostartOpt = null;
        CmdLineParser.Option delegateOpt = null;
        CmdLineParser.Option renewOpt = null;
        CmdLineParser.Option acceptNewJobsOpt = null;
        CmdLineParser.Option proxyOpt = null;
        
        if (options.contains(PROXY)) {
            proxyOpt = parser.addStringOption('p', "proxy");
        }
        
        if (options.contains(EPR)) {
            eprOpt = parser.addStringOption('e', "epr");
        }

        if (options.contains(ACCEPT_NEW_JOBS)) {
            acceptNewJobsOpt = parser.addBooleanOption('a', "acceptNewJobs");
        }

        if (options.contains(ALL_JOBS)) {
            allOpt = parser.addBooleanOption('a', "all");
        }

        if (options.contains(DELEGATION_ID)) {
            delegationIdOpt = parser.addStringOption('d', "delegId");
        }

        if (options.contains(STATUS)) {
            statusOpt = parser.addStringOption('s', "status");
        }

        if (options.contains(LEASE_ID)) {
            leaseIdOpt = parser.addStringOption('l', "leaseId");
        }

        if (options.contains(NEW_LEASE_ID)) {
            newLeaseIdOpt = parser.addStringOption('n', "newLeaseId");
        }

        if (options.contains(LEASE_EXP_TIME)) {
            leaseExpTimeOpt = parser.addStringOption('x', "leaseExpTime");
        }

        if (options.contains(FROM_DATE)) {
            fromOpt = parser.addStringOption('f', "fromDate");
        }

        if (options.contains(TO_DATE)) {
            toOpt = parser.addStringOption('t', "toDate");
        }

        if (options.contains(AUTOSTART)) {
            autostartOpt = parser.addBooleanOption("autostart");
        }

        if (options.contains(DELEGATE)) {
            delegateOpt = parser.addBooleanOption("delegate");
        }

        if (options.contains(RENEW)) {
            renewOpt = parser.addBooleanOption("renew");
        }

        try {
            parser.parse(args);
        } catch (CmdLineParser.OptionException e) {
            System.err.println(e.getMessage());
            printUsage();
        }

        Boolean getHelp = (Boolean) parser.getOptionValue(getHelpOpt, Boolean.FALSE);

        if (getHelp.booleanValue()) {
            printUsage();
        }

        if (proxyOpt != null) {
            proxy = (String) parser.getOptionValue(proxyOpt);
        }
            
        if (eprOpt != null) {
            epr = (String) parser.getOptionValue(eprOpt);

            if (epr == null) {
                printUsage();
            }
        }

        if (allOpt != null) {
            allJobs = ((Boolean) parser.getOptionValue(allOpt, Boolean.FALSE)).booleanValue();
        }

        if (delegationIdOpt != null) {
            delegationId = (String) parser.getOptionValue(delegationIdOpt, null);
        }

        if (leaseIdOpt != null) {
            leaseId = (String) parser.getOptionValue(leaseIdOpt, null);
        }

        if (newLeaseIdOpt != null) {
            newLeaseId = (String) parser.getOptionValue(newLeaseIdOpt, null);
        }

        if (statusOpt != null) {
            String statuses = (String) parser.getOptionValue(statusOpt, null);
            if (statuses != null) {
                StringTokenizer st = new StringTokenizer(statuses, ":");

                if (st.countTokens() > 0) {
                    status = new String[st.countTokens()];
                    int index = 0;

                    while (st.hasMoreTokens()) {
                        status[index++] = st.nextToken();
                    }
                }
            }
        }

        if (leaseExpTimeOpt != null) {
            String leaseExpTimeStr = (String) parser.getOptionValue(leaseExpTimeOpt, null);
            SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd.HH:mm:ss");

            if (leaseExpTimeStr != null) {
                try {
                    leaseExpTime = new GregorianCalendar();
                    leaseExpTime.setTime(df.parse(leaseExpTimeStr));
                } catch (ParseException e) {
                    fromDate = null;
                    System.err.println("Invalid --from date " + leaseExpTimeStr + "; ignored");
                }
            }
        }

        if (fromOpt != null) {
            String fromDateStr = (String) parser.getOptionValue(fromOpt, null);
            SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

            if (fromDateStr != null) {
                try {
                    fromDate = new GregorianCalendar();
                    fromDate.setTime(df.parse(fromDateStr));
                } catch (ParseException e) {
                    fromDate = null;
                    System.err.println("Invalid --from date " + fromDateStr + "; ignored");
                }
            }
        }

        if (toOpt != null) {
            String toDateStr = (String) parser.getOptionValue(toOpt, null);
            SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

            if (toDateStr != null) {
                try {
                    toDate = new GregorianCalendar();
                    toDate.setTime(df.parse(toDateStr));
                } catch (ParseException e) {
                    toDate = null;
                    System.err.println("Invalid --end date " + toDateStr + "; ignored");
                }
            }
        }

        if (acceptNewJobsOpt != null) {
            acceptNewJobs = ((Boolean) parser.getOptionValue(acceptNewJobsOpt, Boolean.FALSE)).booleanValue();
        }

        if (autostartOpt != null) {
            autostart = ((Boolean) parser.getOptionValue(autostartOpt, Boolean.FALSE)).booleanValue();
        }

        if (delegateOpt != null) {
            delegate = ((Boolean) parser.getOptionValue(delegateOpt, Boolean.FALSE)).booleanValue();
        }

        if (renewOpt != null) {
            renew = ((Boolean) parser.getOptionValue(renewOpt, Boolean.FALSE)).booleanValue();
        }

        String[] opt = parser.getRemainingArgs();

        // if ((!allJobs && (opt.length == 0 && (status == null || status.length
        // == 0)))) {
        // printUsage();
        // return;
        // }

        if (opt.length > 0) {
            if (autostartOpt == null) {
                jobIdList = new JobId[opt.length];
                for (int i = 0; i < opt.length; i++) {
                    jobIdList[i] = new JobId();
                    jobIdList[i].setCreamURL(ConverterUtil.convertToAnyURI(epr));
                    jobIdList[i].setId(opt[i]);
                }
            } else {
                jdlFileArray = opt;
            }
        }
    }

    protected final boolean printFault(AxisFault fault) {
        if (fault == null) {
            return false;
        }

        System.out.println("error: " + fault.getMessage());

        return true;
    }

    protected final boolean printFault(DelegationException fault) {
        if (fault == null) {
            return false;
        }

        System.out.println("error: " + fault.getMsg());

        return true;
    }

    /**
     * Prints information about the given fault.
     * 
     * @param fault
     *            the BaseFaultType object to dump.
     */
    protected final boolean printFault(BaseFaultType fault) {
        if (fault == null) {
            return false;
        }

        System.out.println("class name:   " + fault.getClass().getName());
        System.out.println("method name:  " + fault.getMethodName());
        System.out.println("description:  " + fault.getDescription());
        System.out.println("fault cause:  " + fault.getFaultCause());
        System.out.println("error code:   " + fault.getErrorCode());

        if (fault.getTimestamp() != null) {
            System.out.println("timestamp:    " + fault.getTimestamp().getTime());
        }

        return true;
    }

    /**
     * Prints informations from the given JobInfo structure.
     * 
     * @param jobInfo
     *            the JobInfo structure to print. If null, this method does
     *            nothing.
     */
    protected final void printJobInfo(JobInfo jobInfo) {
        if (jobInfo == null) {
            return;
        }

        System.out.println("CREAM job ID          " + jobInfo.getJobId().getId());
        System.out.println("CREAM url             " + jobInfo.getJobId().getCreamURL().toString());
        System.out.println("GRID job ID           " + jobInfo.getGridJobId());
        System.out.println("LRMSAbsLayer job ID   " + jobInfo.getLRMSAbsLayerJobId());
        System.out.println("LRMS job ID           " + jobInfo.getLRMSJobId());
        System.out.println("delegation ID         " + jobInfo.getDelegationProxyId());
        System.out.println("job working directory " + jobInfo.getWorkingDirectory());
        System.out.println("worker node           " + jobInfo.getWorkerNode());
        System.out.println("ISB URI               " + jobInfo.getCREAMInputSandboxURI());
        System.out.println("OSB URI               " + jobInfo.getCREAMOutputSandboxURI());
        System.out.println("user proxy info       " + jobInfo.getDelegationProxyInfo() + "\n");

        System.out.println("job type              " + jobInfo.getType());
        if (jobInfo.getFatherJobId() != null) {
            System.out.println("father job id         " + jobInfo.getFatherJobId());
        }

        JobId[] childJobId = jobInfo.getChildJobId();
        if (childJobId != null) {
            System.out.println("child job id list:");

            for (int i = 0; i < childJobId.length; i++) {
                System.out.println("\t " + i + ") " + childJobId[i]);
            }
        }
        System.out.println("JDL                   " + jobInfo.getJDL() + "\n");
        if (jobInfo.getLease() != null) {
            Lease lease = jobInfo.getLease();
            System.out.println("lease id            " + lease.getLeaseId());
            System.out.println("lease time          " + lease.getLeaseTime().getTime());
        } else
            System.out.println("job lease             N/A");

        Status[] status = jobInfo.getStatus();
        for (int i = 0; i < status.length; i++) {
            System.out.println("\njob status");
            System.out.println("\tjobId = " + status[i].getJobId().getId());
            System.out.println("\tname = " + status[i].getName());
            System.out.println("\ttimestamp = " + status[i].getTimestamp().getTime());

            if (status[i].getFailureReason() != null) {
                System.out.println("\tfailure reason = " + status[i].getFailureReason());
            }

            if (status[i].getDescription() != null) {
                System.out.println("\tdescription = " + status[i].getDescription());
            }

            if (status[i].getExitCode() != null) {
                System.out.println("\texit code = " + status[i].getExitCode());
            }
        }

        Command[] cmd = jobInfo.getLastCommand();
        if (cmd == null) {
            return;
        }
        for (int i = 0; i < cmd.length; i++) {
            System.out.println("command " + i);
            if (cmd[i].getId() != null) {
                System.out.println("\tid                       " + cmd[i].getId());
            }

            System.out.println("\tname                     " + cmd[i].getName());
            System.out.println("\tstatus                   " + cmd[i].getStatus());
            if (cmd[i].getDescription() != null) {
                System.out.println("\tdescription              " + cmd[i].getDescription());
            }
            if (cmd[i].getFailureReason() != null) {
                System.out.println("\tfailure reason           " + cmd[i].getFailureReason());
            }
            if (cmd[i].getCreationTime() != null) {
                System.out.println("\tcreation time            " + cmd[i].getCreationTime().getTime());
            }
            if (cmd[i].getStartSchedulingTime() != null) {
                System.out.println("\tstart scheduling time    " + cmd[i].getStartSchedulingTime().getTime());
            }
            if (cmd[i].getStartProcessingTime() != null) {
                System.out.println("\tstart processing time    " + cmd[i].getStartProcessingTime().getTime());
            }
            if (cmd[i].getExecutionCompletedTime() != null) {
                System.out.println("\texecution completed time " + cmd[i].getExecutionCompletedTime().getTime());
            }
        }
    }

    protected void printResult(Result[] result, String msgOnSuccess) {
        if (result == null) {
            return;
        }

        System.out.println();

        for (int i = 0; i < result.length; i++) {
            System.out.println("" + i + ") " + result[i].getJobId().getId());
            ResultChoice_type0 resultChoice = result[i].getResultChoice_type0();

            if (resultChoice == null) {
                System.out.println(msgOnSuccess);
            } else if (resultChoice.isDateMismatchFaultSpecified()) {
                printFault(resultChoice.getDateMismatchFault());
            } else if (resultChoice.isDelegationIdMismatchFaultSpecified()) {
                printFault(resultChoice.getDelegationIdMismatchFault());
            } else if (resultChoice.isGenericFaultSpecified()) {
                printFault(resultChoice.getGenericFault());
            } else if (resultChoice.isJobStatusInvalidFaultSpecified()) {
                printFault(resultChoice.getJobStatusInvalidFault());
            } else if (resultChoice.isJobUnknownFaultSpecified()) {
                printFault(resultChoice.getJobUnknownFault());
            } else if (resultChoice.isLeaseIdMismatchFaultSpecified()) {
                printFault(resultChoice.getLeaseIdMismatchFault());
            }

            System.out.println("-----------------------------------------------------------------------------");
        }
    }

    protected void printUsage() {
        System.err.println("CREAM Client\n\n");
        System.err.print("Usage: " + getClass().getName() + " -e|--epr <endpoint> [-h|--help]");

        if (options == null) {
            System.err.println();
        } else {
            for (int i = 0; i < options.size(); i++) {                
                if (options.get(i).equals(PROXY)) {
                    System.err.print(" [-p|--proxy]");
                } else if (options.get(i).equals(STATUS)) {
                    System.err.print(" [-s|--status]");
                } else if (options.get(i).equals(LEASE_ID)) {
                    System.err.print(" [-l|--leaseId]");
                } else if (options.get(i).equals(NEW_LEASE_ID)) {
                    System.err.print(" [-n|--newLeaseId]");
                } else if (options.get(i).equals(LEASE_EXP_TIME)) {
                    System.err.print(" [-x|--leaseExpTime]");
                } else if (options.get(i).equals(DELEGATION_ID)) {
                    System.err.print(" [-d|--delegId]");
                } else if (options.get(i).equals(FROM_DATE)) {
                    System.err.print(" [-f|--fromDate]");
                } else if (options.get(i).equals(TO_DATE)) {
                    System.err.print(" [-t|--toDate]");
                } else if (options.get(i).equals(ALL_JOBS)) {
                    System.err.print(" [-a|--all]");
                } else if (options.get(i).equals(AUTOSTART)) {
                    System.err.print(" [--autostart]");
                } else if (options.get(i).equals(DELEGATE)) {
                    System.err.print(" [--delegate]");
                } else if (options.get(i).equals(RENEW)) {
                    System.err.print(" [--renew]");
                }
            }
            System.err.println();
        }

        // System.err.println("-r|--resource <endpoint>");
        // System.err.println("\tThe endpoint to use (in the form https://<host>:<port>)");
        // System.err.println("\tThe CREAM Service is assumed to be at:");
        // System.err.println("\thttps://<host>:<port>/ce-cream/services/CREAM");
        // System.err.println("[--from|--to] <date>\tSelects all jobs submitted after/before <date>");
        // System.err.println("\t\t<date> must be given as YYYY/MM/DD HH:MM:SS");
        // System.err.println("-a|--all\tDisplays informations for ALL jobs (may take a while)");
        // System.err.println("-s|--status <s>\tDisplays informations for jobs in status <s>");
        // System.err.println("\t\t<s>:=RUNNING|PENDING|DONE-OK|DONE-FAILED|CANCALLED|IDLE|ABORTED|UNKNOWN|REGISTERED|HELD");
        // System.err.println("\t\tMultiple -s options may be used to list all jobs in one of the specified states");
        // System.err.println("-h|--help\tGet this help");

        System.exit(0);
    }

    public void setAllJobs(boolean allJobs) {
        this.allJobs = allJobs;
    }

    public void setAutostart(boolean autostart) {
        this.autostart = autostart;
    }

    public void setDelegate(boolean delegate) {
        this.delegate = delegate;
    }

    public void setDelegationId(String delegationId) {
        this.delegationId = delegationId;
    }

    public void setEpr(String epr) {
        this.epr = epr;
    }

    public void setFromDate(Calendar fromDate) {
        this.fromDate = fromDate;
    }

    public void setJdlFileArray(String[] jdlFileArray) {
        this.jdlFileArray = jdlFileArray;
    }

    public void setJobIdList(JobId[] jobIdList) {
        this.jobIdList = jobIdList;
    }

    public void setLeaseExpTime(Calendar leaseExpTime) {
        this.leaseExpTime = leaseExpTime;
    }

    public void setLeaseId(String leaseId) {
        this.leaseId = leaseId;
    }

    public void setNewLeaseId(String newLeaseId) {
        this.newLeaseId = newLeaseId;
    }

    public void setRenew(boolean renew) {
        this.renew = renew;
    }

    public void setStatus(String[] status) {
        this.status = status;
    }

    public void setToDate(Calendar toDate) {
        this.toDate = toDate;
    }

    private void setSSLProperties() throws AxisFault {
        Protocol.registerProtocol("https", new Protocol("https", new AXIS2SocketFactory(), 8443));

        Properties sslConfig = new Properties();
        sslConfig.put(ContextWrapper.SSL_PROTOCOL, "SSLv3");
        sslConfig.put(ContextWrapper.CA_FILES, "/etc/grid-security/certificates/*.0");
        sslConfig.put(ContextWrapper.CRL_ENABLED, "true");
        sslConfig.put(ContextWrapper.CRL_FILES, "/etc/grid-security/certificates/*.r0");
        sslConfig.put(ContextWrapper.CRL_UPDATE_INTERVAL, "0s");

        if (proxy != null) {
            sslConfig.put(ContextWrapper.CREDENTIALS_PROXY_FILE, proxy);
        } else {
            String confFileName = System.getProperty("user.home") + "/.glite/dlgor.properties";
            GrDProxyDlgorOptions dlgorOpt;
            try {
                dlgorOpt = new GrDProxyDlgorOptions(confFileName);
            } catch (IOException e) {
                throw new AxisFault(e.getMessage());
            }

            String proxyFilename = dlgorOpt.getDlgorProxyFile();

            if (proxyFilename != null) {
                sslConfig.put(ContextWrapper.CREDENTIALS_PROXY_FILE, proxyFilename);
            } else {
                String certFilename = dlgorOpt.getDlgorCertFile();
                if (certFilename == null || "".equals(certFilename)) {
                    throw new AxisFault("Missing user credentials: issuerCertFile not found in " + confFileName);
                }

                String keyFilename = dlgorOpt.getDlgorKeyFile();
                if (certFilename == null || "".equals(certFilename)) {
                    throw new AxisFault("Missing user credentials: issuerKeyFile not found in " + confFileName);
                }

                String passwd = dlgorOpt.getDlgorPass();
                passwd = passwd == null ? "" : passwd;

                sslConfig.put(ContextWrapper.CREDENTIALS_CERT_FILE, certFilename);
                sslConfig.put(ContextWrapper.CREDENTIALS_KEY_FILE, keyFilename);
                sslConfig.put(ContextWrapper.CREDENTIALS_KEY_PASSWD, passwd);
            }
        }

        AXIS2SocketFactory.setCurrentProperties(sslConfig);
    }
    
    protected String signRequest(String certReq, String delegationID) throws IOException {
        String strX509CertChain = null;
        String confFileName = System.getProperty("user.home") + "/.glite/dlgor.properties";
        GrDProxyDlgorOptions dlgorOpt = new GrDProxyDlgorOptions(confFileName);

        try {
            GrDProxyGenerator proxyGenerator = new GrDProxyGenerator();

            byte[] x509Cert = proxyGenerator.x509MakeProxyCert(certReq.getBytes(), GrDPX509Util.getFilesBytes(new File(dlgorOpt.getDlgorCertFile())), "null");

            strX509CertChain = new String(x509Cert);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return strX509CertChain;
    }
}
