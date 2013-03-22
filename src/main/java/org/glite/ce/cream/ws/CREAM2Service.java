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

package org.glite.ce.cream.ws;

import java.net.InetAddress;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

import org.apache.axiom.soap.SOAPEnvelope;
import org.apache.axiom.soap.SOAPHeader;
import org.apache.axiom.soap.SOAPHeaderBlock;
import org.apache.axis2.AxisFault;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.context.ServiceContext;
import org.apache.axis2.databinding.types.URI;
import org.apache.axis2.databinding.types.URI.MalformedURIException;
import org.apache.axis2.description.AxisService;
import org.apache.axis2.description.Parameter;
import org.apache.axis2.engine.AxisConfiguration;
import org.apache.axis2.service.Lifecycle;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.glite.ce.commonj.utils.CEUtils;
import org.glite.ce.cream.cmdmanagement.CommandManager;
import org.glite.ce.cream.configuration.ServiceConfig;
import org.glite.ce.cream.jobmanagement.command.DeleteLeaseCmd;
import org.glite.ce.cream.jobmanagement.command.GetLeaseCmd;
import org.glite.ce.cream.jobmanagement.command.GetServiceInfo;
import org.glite.ce.cream.jobmanagement.command.JobCancelCmd;
import org.glite.ce.cream.jobmanagement.command.JobCmd;
import org.glite.ce.cream.jobmanagement.command.JobInfoCmd;
import org.glite.ce.cream.jobmanagement.command.JobListCmd;
import org.glite.ce.cream.jobmanagement.command.JobPurgeCmd;
import org.glite.ce.cream.jobmanagement.command.JobRegisterCmd;
import org.glite.ce.cream.jobmanagement.command.JobResumeCmd;
import org.glite.ce.cream.jobmanagement.command.JobSetLeaseIdCmd;
import org.glite.ce.cream.jobmanagement.command.JobStartCmd;
import org.glite.ce.cream.jobmanagement.command.JobStatusCmd;
import org.glite.ce.cream.jobmanagement.command.JobSuspendCmd;
import org.glite.ce.cream.jobmanagement.command.QueryEventCmd;
import org.glite.ce.cream.jobmanagement.command.SetAcceptNewJobsCmd;
import org.glite.ce.cream.jobmanagement.command.SetLeaseCmd;
import org.glite.ce.cream.ws.utils.FaultFactory;
import org.glite.ce.creamapi.cmdmanagement.Command.ExecutionModeValues;
import org.glite.ce.creamapi.cmdmanagement.CommandException;
import org.glite.ce.creamapi.cmdmanagement.CommandManagerException;
import org.glite.ce.creamapi.cmdmanagement.CommandManagerInterface;
import org.glite.ce.creamapi.delegationmanagement.Delegation;
import org.glite.ce.creamapi.delegationmanagement.DelegationCommand;
import org.glite.ce.creamapi.delegationmanagement.DelegationManagerInterface;
import org.glite.ce.creamapi.eventmanagement.Event;
import org.glite.ce.creamapi.jobmanagement.Job;
import org.glite.ce.creamapi.jobmanagement.JobEnumeration;
import org.glite.ce.creamapi.jobmanagement.JobManagementException;
import org.glite.ce.creamapi.jobmanagement.JobStatus;
import org.glite.ce.creamapi.jobmanagement.cmdexecutor.JobCommandConstant;
import org.glite.ce.creamapi.jobmanagement.cmdexecutor.JobSubmissionManager;
import org.glite.ce.creamapi.jobmanagement.cmdexecutor.JobSubmissionManagerInfo;
import org.glite.ce.creamapi.jobmanagement.command.JobCommand;
import org.glite.ce.creamapi.jobmanagement.command.JobIdFilterFailure;
import org.glite.ce.creamapi.jobmanagement.command.JobIdFilterResult;
import org.glite.ce.creamapi.jobmanagement.db.DBInfoManager;
import org.glite.ce.creamapi.jobmanagement.db.JobDBInterface;
import org.glite.ce.creamapi.ws.cream2.Authorization_Fault;
import org.glite.ce.creamapi.ws.cream2.CREAMSkeletonInterface;
import org.glite.ce.creamapi.ws.cream2.Generic_Fault;
import org.glite.ce.creamapi.ws.cream2.InvalidArgument_Fault;
import org.glite.ce.creamapi.ws.cream2.JobSubmissionDisabled_Fault;
import org.glite.ce.creamapi.ws.cream2.OperationNotSupported_Fault;
import org.glite.ce.creamapi.ws.cream2.types.AuthorizationFault;
import org.glite.ce.creamapi.ws.cream2.types.Command;
import org.glite.ce.creamapi.ws.cream2.types.DateMismatchFault_type0;
import org.glite.ce.creamapi.ws.cream2.types.DelegationIdMismatchFault_type0;
import org.glite.ce.creamapi.ws.cream2.types.GenericFault;
import org.glite.ce.creamapi.ws.cream2.types.JobDescription;
import org.glite.ce.creamapi.ws.cream2.types.JobFilter;
import org.glite.ce.creamapi.ws.cream2.types.JobId;
import org.glite.ce.creamapi.ws.cream2.types.JobInfo;
import org.glite.ce.creamapi.ws.cream2.types.JobInfoResult;
import org.glite.ce.creamapi.ws.cream2.types.JobRegisterResult;
import org.glite.ce.creamapi.ws.cream2.types.JobStatusInvalidFault_type0;
import org.glite.ce.creamapi.ws.cream2.types.JobStatusResult;
import org.glite.ce.creamapi.ws.cream2.types.JobUnknownFault_type0;
import org.glite.ce.creamapi.ws.cream2.types.Lease;
import org.glite.ce.creamapi.ws.cream2.types.LeaseIdMismatchFault_type0;
import org.glite.ce.creamapi.ws.cream2.types.Property;
import org.glite.ce.creamapi.ws.cream2.types.QueryEventResult;
import org.glite.ce.creamapi.ws.cream2.types.Result;
import org.glite.ce.creamapi.ws.cream2.types.ResultChoice_type0;
import org.glite.ce.creamapi.ws.cream2.types.ServiceInfo;
import org.glite.ce.creamapi.ws.cream2.types.ServiceMessage;
import org.glite.ce.creamapi.ws.cream2.types.Status;

public class CREAM2Service implements CREAMSkeletonInterface, Lifecycle {
    private static long dbID;
    private static final Logger logger = Logger.getLogger(CREAM2Service.class.getName());
    private static final int INITIALIZATION_TBD = 0;
    private static final int INITIALIZATION_OK = 1;
    private static final int INITIALIZATION_ERROR = 2;    
    private static int initialization = INITIALIZATION_TBD;

    protected static URI creamURL;
    protected static String cemonURL = null;

    protected static ServiceInfo serviceInfo = null;

    protected String getUserDN_FQAN(String vo) throws Generic_Fault {
        String fqan = "";
        List<String> fqanlist = null;

        if (vo != null) {
            fqanlist = CEUtils.getFQAN(vo);
        } else {
            String userVO = CEUtils.getUserDefaultVO();

            if (userVO != null) {
                fqanlist = CEUtils.getFQAN(userVO);
            }
        }

        if (fqanlist != null && fqanlist.size() > 0) {
            fqan = normalize(fqanlist.get(0).toString());
        }

        return normalize(CEUtils.getUserDN_RFC2253()) + fqan;
    }

    private String getHTTPTomcatPort() {
        logger.debug("Begin getHTTPTomcatPorts");
        // StringBuffer httpTomcatPorts = new StringBuffer();
        String serverPort = null;

        try {
            // Get MBean server reference
            ArrayList<MBeanServer> servers = MBeanServerFactory.findMBeanServer(null);
            if (servers == null) {
                throw new Exception("No MBeanServer found.");
            }

            MBeanServer server = (MBeanServer) servers.get(0);
            Set<ObjectInstance> mbeanSet = server.queryMBeans(null, null);

            ObjectName objnamestr = null;
            Iterator<ObjectInstance> i = mbeanSet.iterator();

            while (i.hasNext()) {
                objnamestr = ((ObjectInstance) i.next()).getObjectName();
                if (("Catalina".equals(objnamestr.getDomain())) && (objnamestr.getCanonicalName().indexOf("type=Connector") != -1)) {
                    logger.debug("Found Tomcat connector");
                    String protocol = (String) server.getAttribute(objnamestr, "protocol");

                    if (protocol.toUpperCase().indexOf("HTTP") != -1) {
                        serverPort = ((Integer) server.getAttribute(objnamestr, "port")).toString();
                        logger.info("Found HTTP Tomcat port = " + serverPort);
                        // httpTomcatPorts = httpTomcatPorts.append(serverPort);
                        break;
                    }
                }
            } // end while
        } catch (Exception e) {
            logger.warn("It's not possible to find http tomcat port: " + e.getMessage());
            logger.warn("The default Tomcat port (8443) will be used.");
            serverPort = "8443";
            // httpTomcatPorts = new StringBuffer("8443");
        }

        logger.debug("End getHTTPTomcatPorts");

        return serverPort;
    }

    private void checkDBVersion(String dbName, String datasource, String databaseVersion) throws AxisFault {
        String databaseVersionFromDatabase = DBInfoManager.getDBVersion(datasource);

        if (databaseVersionFromDatabase == null) {
            String errorMsg = "Error: cannot retrieve the " + dbName + " database version from the database " + "(Requested version is " + databaseVersion
                    + ") because either the database isn't reachable " + "or the database version isn't correct";
            throw new AxisFault(errorMsg);
        }

        if (!databaseVersionFromDatabase.equals(databaseVersion)) {
            String errorMsg = "The " + dbName + " database version (db version = " + databaseVersionFromDatabase
                    + ") is not compliant with the one requested by cream service (db version = " + databaseVersion + ")";
            throw new AxisFault(errorMsg);
        }
        logger.info(dbName + " database version is correct");
    }

    private void checkInitialization() throws Generic_Fault {
        if (initialization == INITIALIZATION_ERROR) {
            throw new Generic_Fault("CREAM service not available: configuration failed!");
        }
    }
    
    public void init(ServiceContext serviceContext) throws AxisFault {
        if (initialization == INITIALIZATION_ERROR) {
            throw new AxisFault("CREAM service not available: configuration failed!");
        }

        if (initialization == INITIALIZATION_OK) {
            return;
        }

        ConfigurationContext configurationContext = serviceContext.getConfigurationContext();

        if (configurationContext == null) {
            initialization = INITIALIZATION_ERROR;
            logger.error("Configuration error: ConfigurationContext not found!");
            return;
        }

        AxisConfiguration axisConfiguration = configurationContext.getAxisConfiguration();
        if (axisConfiguration == null) {
            initialization = INITIALIZATION_ERROR;
            logger.error("Configuration error: AxisConfiguration not found!");
            return;
        }

        AxisService axisService = axisConfiguration.getService("CREAM2");
        if (axisService == null) {
            initialization = INITIALIZATION_ERROR;
            logger.error("Configuration error: AxisService 'CREAM2' not found!");
            return;
        }

        Parameter parameter = axisService.getParameter("serviceLogConfigurationFile");
        if (parameter == null) {
            initialization = INITIALIZATION_ERROR;
            logger.error("Configuration error: parameter 'serviceLogConfigurationFile' not found!");
            return;
        }

        LogManager.resetConfiguration();
        PropertyConfigurator.configure((String)parameter.getValue());

        ServiceConfig serviceConfig = ServiceConfig.getConfiguration();
        if (serviceConfig == null) {
            initialization = INITIALIZATION_ERROR;
            logger.error("Configuration error: cannot initialize the ServiceConfig");
            return;
        }

        CommandManagerInterface commandManager = null;
        try {
            commandManager = CommandManager.getInstance();
        } catch (Throwable t) {
            initialization = INITIALIZATION_ERROR;
            logger.error("Configuration error: cannot get instance of CommandManager: " + t.getMessage());
            return;
        }

        if (!commandManager.checkCommandExecutor("BLAHExecutor", JobCommandConstant.JOB_MANAGEMENT)) {
            initialization = INITIALIZATION_ERROR;
            logger.error("Configuration error: BLAHExecutor not loaded!");
            return;
        }

        if (serviceInfo == null) {
            serviceInfo = new ServiceInfo();
        }

        // set StartUpTime to null value.
        try {
            DBInfoManager.updateStartUpTime(JobDBInterface.JOB_DATASOURCE_NAME, null);
        } catch (Throwable t) {
            initialization = INITIALIZATION_ERROR;
            logger.error("Configuration error: " + t.getMessage());
            return;
        }

        Calendar startUpTime = (Calendar.getInstance());
        serviceInfo.setStartupTime(startUpTime);
        serviceInfo.setDoesAcceptNewJobSubmissions(false);
        serviceInfo.setStatus("INIT");

        parameter = axisService.getParameter("serviceDescription");            
        serviceInfo.setDescription(parameter == null? "N/A": (String)parameter.getValue());

        parameter = axisService.getParameter("creamInterfaceVersion");   
        serviceInfo.setInterfaceVersion(parameter == null? "N/A": (String)parameter.getValue());

        parameter = axisService.getParameter("creamVersion");
        String serviceVersion = parameter == null? "N/A": (String)parameter.getValue();

        String distributionInfo = serviceConfig.getDistributionInfo();
        if (distributionInfo != null && !"".equals(distributionInfo)) {
            serviceVersion += " - " + distributionInfo;
        }
        serviceInfo.setServiceVersion(serviceVersion);
      
        // check creamdb version
        parameter = axisService.getParameter("creamdbDatabaseVersion");
        if (parameter == null) {
            initialization = INITIALIZATION_ERROR;
            logger.error("Configuration error: Cannot retrieve the creamdb_database_version property from configuration");
            return;
        } else {
            String creamdbVersion = (String)parameter.getValue();
            try {
                checkDBVersion("creamdb", JobDBInterface.JOB_DATASOURCE_NAME, creamdbVersion);
            } catch (Throwable t) {
                initialization = INITIALIZATION_ERROR;
                logger.error("Configuration error: " + t.getMessage());
                return;
            }
        }

        // check delegationdb version
        parameter = axisService.getParameter("delegationdbDatabaseVersion");
        if (parameter == null) {
            initialization = INITIALIZATION_ERROR;
            logger.error("Configuration error: Cannot retrieve the delegationdb_database_version property from configuration");         
            return;
        } else {
            String delegationdbVersion = (String)parameter.getValue();
            try {
                checkDBVersion("delegationdb", DelegationManagerInterface.DELEGATION_DATASOURCE_NAME, delegationdbVersion);
            } catch (Throwable t) {
                initialization = INITIALIZATION_ERROR;
                logger.error("Configuration error: " + t.getMessage());
                return;
            }
        }

        // retrieve DBID, that is the creationTime of the cream database.
        dbID = DBInfoManager.getCreationTime(JobDBInterface.JOB_DATASOURCE_NAME);
        logger.info("DB identifier = " + dbID);
        if (dbID == -1) {
            initialization = INITIALIZATION_ERROR;
            logger.error("Configuration error: cannot retrieve the the DB identifier from database");
            return;
        }

        // retrieve SubmissionEnabled, are submission enabled?
        int submissionEnableFromDB = DBInfoManager.getSubmissionEnabled(JobDBInterface.JOB_DATASOURCE_NAME);
        serviceInfo.setDoesAcceptNewJobSubmissions(JobSubmissionManager.SUBMISSION_ENABLED == submissionEnableFromDB);
        logger.info("submissionEnableFromDB = " + submissionEnableFromDB);

        try {
            JobSubmissionManager.getInstance().enableAcceptNewJobs(submissionEnableFromDB);                
        } catch (Throwable t) {
            initialization = INITIALIZATION_ERROR;
            logger.error("Configuration error: cannot get instance of JobSubmissionManager: " + t.getMessage());
            return;
        }

        try {
            creamURL = new URI("https://" + InetAddress.getLocalHost().getCanonicalHostName() + ":" + getHTTPTomcatPort() + "/ce-cream/services/CREAM2");
            logger.info("CREAM URL: " + creamURL);
        } catch (Throwable t) {
            initialization = INITIALIZATION_ERROR;
            logger.error("Configuration error: cannot get CREAM url");
            return;
        }
        
        // set StartUpTime value.
        try {
            DBInfoManager.updateStartUpTime(JobDBInterface.JOB_DATASOURCE_NAME, startUpTime);
        } catch (Throwable t) {
            initialization = INITIALIZATION_ERROR;
            logger.error("Configuration error: " + t.getMessage());
            return;
        }

        serviceInfo.setStatus("RUNNING");
        initialization = INITIALIZATION_OK;
        logger.info("CREAM initialization done!");
        logger.info("CREAM started!");
    }

    public Result[] insertCommand(JobId[] jobIds, String[] compatibleStatus, Calendar fromDate, Calendar toDate, String delegationId, String leaseId, JobCmd cmd)
            throws Generic_Fault, InvalidArgument_Fault {
        logger.debug("Begin insertCommand");

        if (cmd == null) {
            throw FaultFactory.makeInvalidArgumentFault("insertCommand", "0", "cmd not specified!", null);
        }

        String userId = getUserDN_FQAN(null);

        if (logger.isDebugEnabled()) {
            // code for log information
            logger.debug("insertCommand: filter for command " + cmd.getName());

            if (jobIds != null) {
                for (int index = 0; index < jobIds.length; index++) {
                    logger.debug("" + index + ") jobId = " + jobIds[index].getCreamURL() + "/" + jobIds[index].getId());
                }
            }
            if (leaseId != null) {
                logger.debug("LeaseId = " + leaseId);
            }
            if (delegationId != null) {
                logger.debug("DelegationId = " + delegationId);
            }
            if (fromDate != null) {
                logger.debug("FromDate = " + fromDate.getTime());
            }
            if (toDate != null) {
                logger.debug("ToDate = " + toDate.getTime());
            }
            if (compatibleStatus != null) {
                logger.debug("Status size = " + compatibleStatus.length);
            }
        }

        cmd.setUserId(userId);
        cmd.setIsAdmin(CEUtils.isAdmin());
        cmd.setUserDN(CEUtils.getUserDN_RFC2253());
        cmd.setUserFQAN(CEUtils.getFQAN(null));
        cmd.setLocalUser(CEUtils.getLocalUser());
        cmd.setLocalUserGroup(CEUtils.getLocalUserGroup());
        cmd.setRemoteRequestAddress(CEUtils.getRemoteRequestAddress());
        cmd.setJobStatus(compatibleStatus);
        cmd.setFromDate(fromDate);
        cmd.setToDate(toDate);
        cmd.setDelegationProxyId(delegationId);
        cmd.setLeaseId(leaseId);

        if (jobIds != null) {
            List<String> jobIdList = new ArrayList<String>(0);
            for (int i = 0; i < jobIds.length; i++) {
                jobIdList.add(jobIds[i].getId());
            }
            cmd.setJobIdList(jobIdList);
        }

        List<Result> resultList = new ArrayList<Result>(0);
        List<String> jobIdFound = null;

        if (cmd.isAsynchronous()) {
            if (jobIds != null && jobIds.length > 0) {
                if (compatibleStatus == null) {
                    if (JobCommandConstant.JOB_CANCEL.equals(cmd.getName())) {
                        compatibleStatus = new String[] { JobStatus.getNameByType(JobStatus.REGISTERED), JobStatus.getNameByType(JobStatus.IDLE),
                                JobStatus.getNameByType(JobStatus.HELD), JobStatus.getNameByType(JobStatus.REALLY_RUNNING), JobStatus.getNameByType(JobStatus.RUNNING) };

                    } else if (JobCommandConstant.JOB_PURGE.equals(cmd.getName())) {
                        compatibleStatus = new String[] { JobStatus.getNameByType(JobStatus.ABORTED), JobStatus.getNameByType(JobStatus.REGISTERED),
                                JobStatus.getNameByType(JobStatus.CANCELLED), JobStatus.getNameByType(JobStatus.DONE_OK), JobStatus.getNameByType(JobStatus.DONE_FAILED) };

                    } else if (JobCommandConstant.JOB_RESUME.equals(cmd.getName())) {
                        compatibleStatus = new String[] { JobStatus.getNameByType(JobStatus.HELD) };

                    } else if (JobCommandConstant.JOB_SET_LEASEID.equals(cmd.getName())) {
                        compatibleStatus = new String[] { JobStatus.getNameByType(JobStatus.PENDING), JobStatus.getNameByType(JobStatus.IDLE),
                                JobStatus.getNameByType(JobStatus.HELD), JobStatus.getNameByType(JobStatus.REALLY_RUNNING), JobStatus.getNameByType(JobStatus.RUNNING) };

                    } else if (JobCommandConstant.JOB_SUSPEND.equals(cmd.getName())) {
                        compatibleStatus = new String[] { JobStatus.getNameByType(JobStatus.IDLE), JobStatus.getNameByType(JobStatus.REALLY_RUNNING),
                                JobStatus.getNameByType(JobStatus.RUNNING) };

                    } else if (JobCommandConstant.JOB_START.equals(cmd.getName())) {
                        compatibleStatus = new String[] { JobStatus.getNameByType(JobStatus.REGISTERED) };
                    }
                }

                JobInfoCmd jfCmd = new JobInfoCmd();
                jfCmd.setUserId(cmd.getUserId());
                jfCmd.setJobStatus(compatibleStatus);
                jfCmd.setIsAdmin(cmd.isAdmin());
                jfCmd.setFromDate(fromDate);
                jfCmd.setToDate(toDate);
                jfCmd.setDelegationProxyId(delegationId);
                jfCmd.setLeaseId(leaseId);
                jfCmd.setJobIdList(cmd.getJobIdList());

                try {
                    logger.debug("Calling commandManager.insertCommand for synchronous command.");
                    CommandManager.getInstance().execute(jfCmd);
                    logger.debug("CommandManager.getInstance().execute for synchronous command executed.");

                    jobIdFound = jfCmd.getJobIdFound();

                    fillupResultList(resultList, jfCmd.getJobIdFilterResult(), cmd.getName());
                } catch (IllegalArgumentException e) {
                    logger.error("IllegalArgumentException: " + e.getMessage());
                    throw FaultFactory.makeGenericFault("execute", "0", e.getMessage(), e.getMessage());
                } catch (CommandException e) {
                    logger.error("CommandException: " + e.getMessage());
                    throw FaultFactory.makeGenericFault("execute", "0", e.getMessage(), e.getMessage());
                } catch (CommandManagerException e) {
                    logger.error("CommandManagementException: " + e.getMessage());
                    throw FaultFactory.makeGenericFault("execute", "0", e.getMessage(), e.getMessage());
                }
            }

            if (jobIdFound == null || jobIdFound.size() > 1) {
                cmd.addParameter("EXECUTION_MODE", cmd.getExecutionMode().getStringValue());
                cmd.addParameter("PRIORITY_LEVEL", "" + cmd.getPriorityLevel());
                cmd.setCommandGroupId("COMPOUND");
                cmd.setExecutionMode(ExecutionModeValues.PARALLEL);
                cmd.setPriorityLevel(JobCmd.MEDIUM_PRIORITY);
            } else if (jobIdFound.size() == 1) {
                cmd.setCommandGroupId(jobIdFound.get(0));
            }
        }

        if (!cmd.isAsynchronous() || (jobIdFound == null || jobIdFound.size() > 0)) {
            try {
                logger.debug("Calling CommandManager.getInstance().execute for synchronous command.");
                CommandManager.getInstance().execute(cmd);
                logger.debug("CommandManager.getInstance().execute for synchronous command executed.");
            } catch (Throwable e) {
                logger.error("execute error: " + e.getMessage());
                throw FaultFactory.makeGenericFault("execute", "1", e.getMessage(), e.getMessage());
            }
        }

        if (!cmd.isAsynchronous()) {
            fillupResultList(resultList, cmd.getJobIdFilterResult(), cmd.getName());
        }

        Result[] result = new Result[resultList.size()];
        result = resultList.toArray(result);

        logger.debug("End execute");

        return result;
    }

    private void fillupResultList(List<Result> resultList, List<JobIdFilterResult> filterResultList, String commandName) {
        if (resultList == null || filterResultList == null) {
            return;
        }

        JobUnknownFault_type0 jobNotFoundFault = FaultFactory.makeJobUnknownFault(commandName, "1", "job not found!", "N/A");
        JobStatusInvalidFault_type0 jobStatusInvalidFault = FaultFactory.makeJobStatusInvalidFault(commandName, "2", "the job has a status not compatible with the " + commandName
                + " command!", "N/A");
        DateMismatchFault_type0 dateMismatchFault = FaultFactory.makeDateMismatchFault(commandName, "3", "fromDate/toDate mismatch", "N/A");
        DelegationIdMismatchFault_type0 delegationMismatchFault = FaultFactory.makeDelegationIdMismatchFault(commandName, "3", "delegationId mismatch", "N/A");
        LeaseIdMismatchFault_type0 leaseIdMismatchFault = FaultFactory.makeLeaseIdMismatchFault(commandName, "3", "leaseId mismatch", "N/A");

        JobId jobId = null;
        Result result = null;
        ResultChoice_type0 resultChoice = null;

        for (JobIdFilterResult filterResult : filterResultList) {
            jobId = new JobId();
            jobId.setId(filterResult.getJobId());
            jobId.setCreamURL(creamURL);

            resultChoice = new ResultChoice_type0();

            result = new Result();
            result.setJobId(jobId);
            result.setResultChoice_type0(resultChoice);

            resultList.add(result);

            switch (filterResult.getErrorCode()) {
                case JobIdFilterFailure.OK_ERRORCODE:
                    break;
                case JobIdFilterFailure.JOBID_ERRORCODE:
                    resultChoice.setJobUnknownFault(jobNotFoundFault);
                    break;
                case JobIdFilterFailure.STATUS_ERRORCODE:
                    resultChoice.setJobStatusInvalidFault(jobStatusInvalidFault);
                    break;
                case JobIdFilterFailure.LEASEID_ERRORCODE:
                    resultChoice.setLeaseIdMismatchFault(leaseIdMismatchFault);
                    break;
                case JobIdFilterFailure.DATE_ERRORCODE:
                    resultChoice.setDateMismatchFault(dateMismatchFault);
                    break;
                case JobIdFilterFailure.DELEGATIONID_ERRORCODE:
                    resultChoice.setDelegationIdMismatchFault(delegationMismatchFault);
                    break;
            }
        }
    }

    public Lease[] getLeaseList() throws Authorization_Fault, Generic_Fault {
        checkInitialization();

        GetLeaseCmd cmd = new GetLeaseCmd();
        cmd.setUserId(getUserDN_FQAN(null));
        cmd.setIsAdmin(CEUtils.isAdmin());

        try {
            CommandManager.getInstance().execute(cmd);
            List<org.glite.ce.creamapi.jobmanagement.Lease> leaseList = cmd.getLease();
            if (leaseList != null) {
                Lease[] leaseArray = new Lease[leaseList.size()];
                for (int i = 0; i < leaseList.size(); i++) {
                    leaseArray[i] = new Lease();
                    leaseArray[i].setLeaseId(leaseList.get(i).getLeaseId());
                    leaseArray[i].setLeaseTime(leaseList.get(i).getLeaseTime());
                }
                return leaseArray;
            } else {
                return new Lease[0];
            }
        } catch (IllegalArgumentException e) {
            throw FaultFactory.makeGenericFault("getLeaseList", "0", "IllegalArgumentException occured", e.getMessage());
        } catch (CommandException e) {
            throw FaultFactory.makeGenericFault("getLeaseList", "1", "CommandException occured", e.getMessage());
        } catch (CommandManagerException e) {
            throw FaultFactory.makeGenericFault("getLeaseList", "2", "CommandManagementException occured", e.getMessage());
        }
    }

    public Lease getLease(String leaseId) throws Authorization_Fault, Generic_Fault, InvalidArgument_Fault {
        checkInitialization();
        
        if (leaseId == null) {
            throw FaultFactory.makeInvalidArgumentFault("getLease", "0", "leaseId not specified!", null);
        }

        GetLeaseCmd cmd = new GetLeaseCmd(leaseId);
        cmd.setUserId(getUserDN_FQAN(null));
        cmd.setIsAdmin(CEUtils.isAdmin());

        try {
            CommandManager.getInstance().execute(cmd);
            List<org.glite.ce.creamapi.jobmanagement.Lease> leaseList = cmd.getLease();
            if (leaseList != null && leaseList.size() > 0) {
                Lease lease = new Lease();
                lease.setLeaseId(leaseList.get(0).getLeaseId());
                lease.setLeaseTime(leaseList.get(0).getLeaseTime());
                return lease;
            } else {
                throw FaultFactory.makeGenericFault("getLease", "1", "lease " + leaseId + " not found!", "N/A");
            }
        } catch (IllegalArgumentException e) {
            throw FaultFactory.makeGenericFault("getLease", "0", "IllegalArgumentException occured", e.getMessage());
        } catch (CommandException e) {
            throw FaultFactory.makeGenericFault("getLease", "1", "CommandException occured", e.getMessage());
        } catch (CommandManagerException e) {
            throw FaultFactory.makeGenericFault("getLease", "2", "CommandManagementException occured", e.getMessage());
        }
    }

    public void deleteLease(String leaseId) throws Authorization_Fault, Generic_Fault, InvalidArgument_Fault {
        checkInitialization();

        if (leaseId == null) {
            throw FaultFactory.makeInvalidArgumentFault("deleteLease", "0", "leaseId not specified!", null);
        }

        DeleteLeaseCmd cmd = new DeleteLeaseCmd(leaseId);
        cmd.setUserId(getUserDN_FQAN(null));
        cmd.setIsAdmin(CEUtils.isAdmin());

        try {
            CommandManager.getInstance().execute(cmd);
        } catch (IllegalArgumentException e) {
            throw FaultFactory.makeGenericFault("deleteLease", "0", "IllegalArgumentException occured", e.getMessage());
        } catch (CommandException e) {
            throw FaultFactory.makeGenericFault("deleteLease", "1", "CommandException occured", e.getMessage());
        } catch (CommandManagerException e) {
            throw FaultFactory.makeGenericFault("deleteLease", "2", "CommandManagementException occured", e.getMessage());
        }
    }

    public JobId[] jobList() throws Generic_Fault, Authorization_Fault {
        checkInitialization();

        try {
            JobListCmd cmd = new JobListCmd();
            cmd.setUserId(getUserDN_FQAN(null));
            cmd.setIsAdmin(CEUtils.isAdmin());

            CommandManager.getInstance().execute(cmd);

            List<String> list = cmd.getJobIdFound();
            if (list != null) {
                JobId[] idList = new JobId[list.size()];

                for (int i = 0; i < list.size(); i++) {
                    idList[i] = new JobId();
                    idList[i].setCreamURL(creamURL);
                    idList[i].setId((String) list.get(i));
                }

                return idList;
            }
        } catch (CommandException e) {
            throw FaultFactory.makeGenericFault("jobList", "0", "system error", e.getMessage());
        } catch (CommandManagerException e) {
            throw FaultFactory.makeGenericFault("jobList", "1", "CommandManagementException occured", e.getMessage());
        }

        return new JobId[0];
    }

    public JobRegisterResult[] jobRegister(JobDescription[] descr) throws Authorization_Fault, Generic_Fault, InvalidArgument_Fault, JobSubmissionDisabled_Fault {
        checkInitialization();
        
        if (descr == null) {
            throw FaultFactory.makeInvalidArgumentFault("jobRegister", "0", "filter not specified!", null);
        }

        JobSubmissionManagerInfo jobSubmissionManagerInfo = JobSubmissionManager.getInstance().getJobSubmissionManagerInfo();

        if (!jobSubmissionManagerInfo.isAcceptNewJobs()) {
            String acceptNewJobSubmissionErrorMessage = "Submissions are disabled!";
            if ((serviceInfo.getMessage() != null) && (serviceInfo.getMessage().length > 0)) {
                acceptNewJobSubmissionErrorMessage = serviceInfo.getMessage()[0].getMessage();
            }

            throw FaultFactory.makeJobSubmissionDisabledFault("jobRegister", "0", "The CREAM service cannot accept jobs at the moment", acceptNewJobSubmissionErrorMessage);
        }

        ServiceConfig service = ServiceConfig.getConfiguration();
        if (service == null) {
            throw FaultFactory.makeGenericFault("jobRegister", "0", "system error", "Cannot access to the configuration");
        }

        // System.out.println(org.apache.axis.utils.XMLUtils.ElementToString(messageContext.getRequestMessage().getSOAPEnvelope()));
        List<String> fqanlist = null;

        String userVO = CEUtils.getUserDefaultVO();
        if (userVO != null) {
            fqanlist = CEUtils.getFQAN(userVO);
        }

        String fqan = "";
        if (fqanlist != null && fqanlist.size() > 0) {
            fqan = fqanlist.get(0).toString();
        }

        String userId = normalize(CEUtils.getUserDN_RFC2253() + fqan);
        String iceId = null;

        SOAPEnvelope envelope = MessageContext.getCurrentMessageContext().getEnvelope();

        if (envelope != null) {
            SOAPHeader header = envelope.getHeader();

            if (header != null) {
                for (SOAPHeaderBlock headerBlock : (List<SOAPHeaderBlock>)header.getHeaderBlocksWithNSURI("http://glite.org/2007/11/ce/cream/types")) {
                    if ("iceId".equalsIgnoreCase(headerBlock.getLocalName())) {
                        iceId = headerBlock.getText();
                        logger.debug("jobRegister: found iceId = " + iceId);
                        headerBlock.setProcessed();
                        break;
                    }
                }
            } else {
                logger.debug("jobRegister: SOAP header not found!");
            }
        } else {
            logger.debug("jobRegister: SOAP envelope not found!");
        }
 
        // ByName("http://glite.org/2007/11/ce/cream/types", "iceId");

        // if (headerElement == null) {
        // headerElement =
        // MessageContext.getCurrentMessageContext().getEnvelope().getHeaderByName("http://glite.org/ce/creamapij/types",
        // "iceId");
        // }
        //
        // if (headerElement != null && headerElement.getValue() != null &&
        // headerElement.getValue().length() > 0) {
        // iceId = headerElement.getValue();
        // headerElement.setProcessed(true);
        // }

        JobRegisterResult[] result = new JobRegisterResult[descr.length];
        Hashtable<String, Delegation> delegationIdtable = new Hashtable<String, Delegation>(0);

        for (int i = 0; i < descr.length; i++) {
            result[i] = new JobRegisterResult();
            result[i].setJobDescriptionId(descr[i].getJobDescriptionId());
            try {
                String delegationId = descr[i].getDelegationId();
                String delegationProxy = descr[i].getDelegationProxy();

                if (delegationId == null) {
                    result[i].setDelegationIdMismatchFault(FaultFactory.makeDelegationIdMismatchFault("jobRegister", "0", "delegation id not specified!", "N/A"));
                    continue;
                }

                Delegation delegation = null;

                if (!delegationIdtable.containsKey(delegationId)) {
                    if (delegationProxy != null && delegationProxy.length() > 0) {
                        try {
                            DelegationCommand command = new DelegationCommand(DelegationCommand.PUT_DELEGATION);
                            command.addParameter(DelegationCommand.DELEGATION_ID, delegationId);
                            command.addParameter(DelegationCommand.DELEGATION, delegationProxy);
                            command.addParameter(DelegationCommand.USER_DN_RFC2253, CEUtils.getUserDN_RFC2253());
                            command.addParameter(DelegationCommand.USER_CERTIFICATE, CEUtils.getUserCert());
                            command.addParameter(DelegationCommand.LOCAL_USER, CEUtils.getLocalUser());
                            command.addParameter(DelegationCommand.LOCAL_USER_GROUP, CEUtils.getLocalUserGroup());

                            CommandManager.getInstance().execute(command);

                            delegation = (Delegation)command.getResult().getParameter(DelegationCommand.DELEGATION);
                        } catch (Throwable e) {
                            result[i].setDelegationProxyFault(FaultFactory.makeDelegationProxyFault("jobRegister", "0", "delegation error: put proxy failed!", e.getMessage()));
                            continue;
                        }
                    }

                    if (delegation == null) {
                        try {
                            DelegationCommand command = new DelegationCommand(DelegationCommand.GET_DELEGATION);
                            command.addParameter(DelegationCommand.DELEGATION_ID, delegationId);
                            command.addParameter(DelegationCommand.USER_DN_RFC2253, CEUtils.getUserDN_RFC2253());
                            command.addParameter(DelegationCommand.LOCAL_USER, CEUtils.getLocalUser());

                            CommandManager.getInstance().execute(command);

                            delegation = (Delegation)command.getResult().getParameter(DelegationCommand.DELEGATION);
                        } catch (Throwable e) {
                            result[i].setDelegationProxyFault(FaultFactory.makeDelegationProxyFault("jobRegister", "0", "delegation error: put proxy failed!", e.getMessage()));
                            continue;
                        }
                    }

                    if (delegation == null) {
                        String message = "delegation [id=" + delegationId + "] not found!;";
                        result[i].setDelegationIdMismatchFault(FaultFactory.makeDelegationIdMismatchFault("jobRegister", "0", message, "delegation not found!"));
                        continue;
                    }

                    delegationIdtable.put(delegationId, delegation);
                } else {
                    delegation = delegationIdtable.get(delegationId);
                }

                if (!delegation.isValid()) {
                    String message = "the delegation [id=" + delegationId + "] is not more valid!;";
                    result[i].setDelegationProxyFault(FaultFactory.makeDelegationProxyFault("jobRegister", "0", message, "delegation proxy expired!"));
                    continue;
                }

                JobRegisterCmd cmd = new JobRegisterCmd();
                cmd.setLocalUser(CEUtils.getLocalUser());
                cmd.setLocalUserGroup(CEUtils.getLocalUserGroup());
                cmd.setUserId(userId);
                cmd.setUserDN(CEUtils.getUserDN_RFC2253());
                cmd.setUserDNX500(CEUtils.getUserDN_X500());
                cmd.setUserFQAN(fqanlist);
                cmd.setUserVO(CEUtils.getUserDefaultVO());
                cmd.setJDL(descr[i].getJDL());
                cmd.setAutostart(false);
                cmd.setDelegationProxyId(delegationId);
                cmd.setDelegationProxyPath(delegation.getFullPath());
                cmd.setDelegationProxyInfo(delegation.getInfo());
                cmd.setLeaseId(descr[i].getLeaseId());
                cmd.setCREAMURL(creamURL.toString());
                // cmd.setCEId(ceId);
                cmd.setICEId(iceId);
                cmd.setAsynchronous(false);
                cmd.setRemoteRequestAddress(CEUtils.getRemoteRequestAddress());

                String url = creamURL.toString();
                String gsiFTPcreamURL = url.substring(url.indexOf(":"), url.lastIndexOf(":"));
                cmd.setGSIFTPCREAMURL("gsiftp" + gsiFTPcreamURL);

                CommandManager.getInstance().execute(cmd);

                Job job = cmd.getJob();

                if (job == null) {
                    result[i].setGenericFault(FaultFactory.makeGenericFault_type0("jobRegister", "0", "job not created!", null));
                    continue;
                }

                if (descr[i].getAutoStart() && job.getStatusCount() == 1 && job.getLastStatus().getType() == JobStatus.REGISTERED) {
                    JobStartCmd jobStartCmd = new JobStartCmd();
                    jobStartCmd.setUserId(userId);
                    jobStartCmd.setUserDN(cmd.getUserDN());
                    jobStartCmd.setUserFQAN(cmd.getUserFQAN());
                    jobStartCmd.setLocalUser(CEUtils.getLocalUser());
                    jobStartCmd.setLocalUserGroup(CEUtils.getLocalUserGroup());
                    jobStartCmd.setJobId(job.getId());
                    jobStartCmd.setCommandGroupId(job.getId());
                    jobStartCmd.setRemoteRequestAddress(cmd.getRemoteRequestAddress());

                    CommandManager.getInstance().execute(jobStartCmd);
                }

                JobId jobId = new JobId();
                jobId.setCreamURL(creamURL);
                jobId.setId(job.getId());

                if (job.getCREAMInputSandboxURI() != null) {
                    org.glite.ce.creamapi.ws.cream2.types.Property[] property = new org.glite.ce.creamapi.ws.cream2.types.Property[((iceId == null) ? 2 : 3)];
                    property[0] = new org.glite.ce.creamapi.ws.cream2.types.Property();
                    property[0].setName("CREAMInputSandboxURI");
                    property[0].setValue(job.getCREAMInputSandboxURI());
                    property[1] = new org.glite.ce.creamapi.ws.cream2.types.Property();
                    property[1].setName("CREAMOutputSandboxURI");
                    property[1].setValue(job.getCREAMOutputSandboxURI());

                    if (iceId != null) {
                        property[2] = new org.glite.ce.creamapi.ws.cream2.types.Property();
                        property[2].setName("DB_ID");
                        property[2].setValue("" + dbID);
                    }

                    jobId.setProperty(property);
                }

                result[i].setJobId(jobId);
            } catch (Throwable e) {
                logger.error(e.getMessage(), e);

                if (e.getMessage().startsWith("lease id")) {
                    result[i].setLeaseIdMismatchFault(FaultFactory.makeLeaseIdMismatchFault("jobRegister", "0", "lease id not found!", e.getMessage()));
                } else {
                    result[i].setGenericFault(FaultFactory.makeGenericFault_type0("jobRegister", "0", "system error", e.getMessage()));
                }
            }
        }

        return result;
    }

    private JobInfo makeJob(Job job, boolean isAdmin) {
        if (job == null) {
            return null;
        }

        JobId id = new JobId();
        id.setId(job.getId());

        try {
            id.setCreamURL(new URI(job.getCreamURL()));
        } catch (MalformedURIException e) {
        }

        JobInfo jobInfo = new JobInfo();
        jobInfo.setJobId(id);

        if (job.getCREAMInputSandboxURI() != null) {
            jobInfo.setCREAMInputSandboxURI(job.getCREAMInputSandboxURI());
        } else {
            jobInfo.setCREAMInputSandboxURI(Job.NOT_AVAILABLE_VALUE);
        }

        if (job.getCREAMOutputSandboxURI() != null) {
            jobInfo.setCREAMOutputSandboxURI(job.getCREAMOutputSandboxURI());
        } else {
            jobInfo.setCREAMOutputSandboxURI(Job.NOT_AVAILABLE_VALUE);
        }

        if (job.getDelegationProxyInfo() != null) {
            jobInfo.setDelegationProxyInfo(job.getDelegationProxyInfo());
        } else {
            jobInfo.setDelegationProxyInfo(Job.NOT_AVAILABLE_VALUE);
        }

        if (job.getGridJobId() != null) {
            jobInfo.setGridJobId(job.getGridJobId());
        } else {
            jobInfo.setGridJobId(Job.NOT_AVAILABLE_VALUE);
        }

        if (job.getLocalUser() != null) {
            jobInfo.setLocalUser(job.getLocalUser());
        } else {
            jobInfo.setLocalUser(Job.NOT_AVAILABLE_VALUE);
        }

        if (isAdmin) {
            if (job.getLRMSAbsLayerJobId() != null) {
                jobInfo.setLRMSAbsLayerJobId(job.getLRMSAbsLayerJobId());
            } else {
                jobInfo.setLRMSAbsLayerJobId(Job.NOT_AVAILABLE_VALUE);
            }
        } else {
            jobInfo.setLRMSAbsLayerJobId("[reserved]");
        }

        if (isAdmin) {
            if (job.getLRMSJobId() != null) {
                jobInfo.setLRMSJobId(job.getLRMSJobId());
            } else {
                jobInfo.setLRMSJobId(Job.NOT_AVAILABLE_VALUE);
            }
        } else {
            jobInfo.setLRMSJobId("[reserved]");
        }

        if (isAdmin) {
            if (job.getWorkingDirectory() != null) {
                jobInfo.setWorkingDirectory(job.getWorkingDirectory());
            } else {
                jobInfo.setWorkingDirectory(Job.NOT_AVAILABLE_VALUE);
            }
        } else {
            jobInfo.setWorkingDirectory("[reserved]");
        }

        if (job.getWorkerNode() != null) {
            jobInfo.setWorkerNode(job.getWorkerNode());
        } else {
            jobInfo.setWorkerNode(Job.NOT_AVAILABLE_VALUE);
        }

        if (job.getType() != null) {
            jobInfo.setType(job.getType());
        } else {
            jobInfo.setType(Job.NOT_AVAILABLE_VALUE);
        }

        jobInfo.setDelegationProxyId(job.getDelegationProxyId());
        jobInfo.setJDL(job.getJDL());

        List<JobCommand> commandHistory = job.getCommandHistory();
        if (commandHistory != null) {
            Command[] cmd = new Command[commandHistory.size()];

            for (int i = 0; i < commandHistory.size(); i++) {
                JobCommand jobCmd = commandHistory.get(i);

                cmd[i] = new Command();
                cmd[i].setCategory("JOB_MANAGEMENT");
                cmd[i].setCreationTime(jobCmd.getCreationTime());
                cmd[i].setExecutionCompletedTime(jobCmd.getExecutionCompletedTime());
                cmd[i].setStartProcessingTime(jobCmd.getStartProcessingTime());
                cmd[i].setStartSchedulingTime(jobCmd.getStartSchedulingTime());
                cmd[i].setDescription(jobCmd.getDescription());
                cmd[i].setFailureReason(jobCmd.getFailureReason());
                cmd[i].setName(jobCmd.getName());
                cmd[i].setStatus(jobCmd.getStatusName());
                
                if (jobCmd.getName().equalsIgnoreCase(JobCommandConstant.PROXY_RENEW)) {
                    jobInfo.setDelegationProxyInfo(jobCmd.getDescription());
                }
            }

            jobInfo.setLastCommand(cmd);
        }

        List<JobStatus> statusHistory = job.getStatusHistory();

        if (statusHistory != null) {
            Status[] status = new Status[statusHistory.size()];

            for (int i = 0; i < statusHistory.size(); i++) {
                JobStatus jobStatus = statusHistory.get(i);

                status[i] = new Status();
                status[i].setDescription(jobStatus.getDescription());
                status[i].setExitCode(jobStatus.getExitCode());
                status[i].setFailureReason(jobStatus.getFailureReason());
                status[i].setJobId(id);
                status[i].setName(jobStatus.getName());
                status[i].setTimestamp(jobStatus.getTimestamp());
            }

            jobInfo.setStatus(status);
        }

        if (job.getLease() != null) {
            Lease lease = new Lease();
            lease.setLeaseId(job.getLease().getLeaseId());
            lease.setLeaseTime(job.getLease().getLeaseTime());

            jobInfo.setLease(lease);
        }

        return jobInfo;
    }

    protected String normalize(String s) {
        if (s != null) {
            return s.replaceAll("\\W", "_");
        }
        return null;
    }

    public ServiceInfo getServiceInfo(int verbosityLevel) throws Generic_Fault {
        checkInitialization();

        List<org.glite.ce.creamapi.ws.cream2.types.Property> propertyList = new ArrayList<org.glite.ce.creamapi.ws.cream2.types.Property>(0);

        JobSubmissionManagerInfo jobSubmissionManagerInfo = JobSubmissionManager.getInstance().getJobSubmissionManagerInfo();
        serviceInfo.setDoesAcceptNewJobSubmissions(jobSubmissionManagerInfo.isAcceptNewJobs());

        if (verbosityLevel >= 2 || cemonURL == null) {
            GetServiceInfo getServiceInfoCmd = null;

            try {
                getServiceInfoCmd = new GetServiceInfo();
                getServiceInfoCmd.setUserId(getUserDN_FQAN(null));
                
                CommandManager.getInstance().execute(getServiceInfoCmd);
            } catch (Throwable e) {
                throw FaultFactory.makeGenericFault("getServiceInfo", "0", e.getMessage(), "N/A");
            }

            if (cemonURL == null) {
                cemonURL = getServiceInfoCmd.getResult().getParameterAsString("CEMON_URL");                    
                cemonURL = cemonURL == null? "N/A": cemonURL;
                    
                logger.info("CEMON URL: " + cemonURL);
            }

            if (verbosityLevel >= 2) {
                serviceInfo.setDoesAcceptNewJobSubmissions(getServiceInfoCmd.doesAcceptNewJobs());

                String submissionThresholdMessage = getServiceInfoCmd.getResult().getParameterAsString("SUBMISSION_THRESHOLD_MESSAGE");
                if (submissionThresholdMessage != null) {
                    Property property = new Property();
                    property.setName("SUBMISSION_THRESHOLD_MESSAGE");
                    property.setValue(submissionThresholdMessage);

                    propertyList.add(property);
                }

                String submissionErrorMessage = getServiceInfoCmd.getResult().getParameterAsString("SUBMISSION_ERROR_MESSAGE");
                ServiceMessage[] serviceMessages = null;

                if (submissionErrorMessage != null) {
                    serviceMessages = new ServiceMessage[1];
                    serviceMessages[0] = new ServiceMessage();
                    serviceMessages[0].setMessage(submissionErrorMessage);
                    serviceMessages[0].setTimastamp((Calendar) getServiceInfoCmd.getResult().getParameter("SUBMISSION_EXECUTION_TIMESTAMP"));
                    serviceMessages[0].setType("SUBMISSION_ERROR_MESSAGE");
                }

                serviceInfo.setMessage(serviceMessages);
            }
        }

        if (!"N/A".equals(cemonURL)) {
            Property property = new Property();
            property.setName("cemon_url");
            property.setValue(cemonURL);

            propertyList.add(property);
        }
        
        if (propertyList.size() > 0) {
            org.glite.ce.creamapi.ws.cream2.types.Property[] properties = new org.glite.ce.creamapi.ws.cream2.types.Property[propertyList.size()];
            properties = propertyList.toArray(properties);
            serviceInfo.setProperty(properties);
        } else {
            serviceInfo.setProperty(null);
        }

        return serviceInfo;
    }

    public long getDbID() {
        return dbID;
    }

    public void destroy(ServiceContext context) {
        logger.info("destroy invoked!");
        serviceInfo = null;
        
        try {
            CommandManager.getInstance().terminate();
        } catch (Throwable t) {
            logger.error("cannot get instance of CommandManager: " + t.getMessage());
        }
        logger.info("destroyed!");
    }

    public Result[] jobCancel(JobId[] jobIds, String[] compatibleStatus, Calendar fromDate, Calendar toDate, String delegationId, String leaseId) throws Authorization_Fault, Generic_Fault, InvalidArgument_Fault {
        checkInitialization();

        return insertCommand(jobIds, compatibleStatus, fromDate, toDate, delegationId, leaseId, new JobCancelCmd());
    }

    public JobInfoResult[] jobInfo(JobId[] jobIds, String[] compatibleStatus, Calendar fromDate, Calendar toDate, String delegationId, String leaseId) throws Authorization_Fault, Generic_Fault, InvalidArgument_Fault {
        checkInitialization();
        
        JobInfoCmd cmd = new JobInfoCmd();
        insertCommand(jobIds, compatibleStatus, fromDate, toDate, delegationId, leaseId, cmd);

        JobEnumeration jobEnum = cmd.getJobEnumeration();
        List<JobIdFilterResult> jobFilterResult = cmd.getJobIdFilterResult();

        String creamBaseUrl = "";
        if (creamURL != null) {
            logger.debug("creamURL = " + creamURL.toString());
            int serviceStringIndex = creamURL.toString().lastIndexOf("ce-cream/services");
            if (serviceStringIndex != -1) {
                creamBaseUrl = creamURL.toString().substring(0, serviceStringIndex);
            }
        }

        JobInfoResult[] result = new JobInfoResult[jobFilterResult.size()];
        int index = 0;

        if (jobEnum != null) {
            boolean isAdmin = CEUtils.isAdmin();
            Job job = null;

            while (jobEnum.hasMoreJobs()) {
                try {
                    job = jobEnum.nextJob();
                    result[index] = new JobInfoResult();
                    result[index].setJobDescriptionId(creamBaseUrl + job.getId());
                    result[index++].setJobInfo(makeJob(job, isAdmin));
                } catch (JobManagementException e) {
                    result[index] = new JobInfoResult();
                    result[index].setJobUnknownFault(FaultFactory.makeJobUnknownFault("jobInfo", "0", "job " + job.getId() + " retrieving error", e.getMessage()));
                    result[index++].setJobDescriptionId(creamBaseUrl + job.getId());
                }
            }
        }

        for (int i = 0; i < jobFilterResult.size(); i++) {
            switch (jobFilterResult.get(i).getErrorCode()) {
            case JobIdFilterFailure.OK_ERRORCODE:
                break;
            case JobIdFilterFailure.DATE_ERRORCODE:
                result[index] = new JobInfoResult();
                result[index].setJobDescriptionId(creamBaseUrl + jobFilterResult.get(i).getJobId());
                result[index++].setDateMismatchFault(FaultFactory.makeDateMismatchFault("jobInfo", "0", "fromDate/toDate mismatch", "N/A"));
                break;
            case JobIdFilterFailure.DELEGATIONID_ERRORCODE:
                result[index] = new JobInfoResult();
                result[index].setJobDescriptionId(creamBaseUrl + jobFilterResult.get(i).getJobId());
                result[index++].setDelegationIdMismatchFault(FaultFactory.makeDelegationIdMismatchFault("jobInfo", "1", "delegationId mismatch", "N/A"));
                break;
            case JobIdFilterFailure.JOBID_ERRORCODE:
                result[index] = new JobInfoResult();
                result[index].setJobDescriptionId(creamBaseUrl + jobFilterResult.get(i).getJobId());
                result[index++].setJobUnknownFault(FaultFactory.makeJobUnknownFault("jobInfo", "2", "job not found", "N/A"));
                logger.debug("JobInfo method: job not found (jobId = " + jobFilterResult.get(i).getJobId() + " userId = " + cmd.getUserId() + ")");
                break;
            case JobIdFilterFailure.LEASEID_ERRORCODE:
                result[index] = new JobInfoResult();
                result[index].setJobDescriptionId(creamBaseUrl + jobFilterResult.get(i).getJobId());
                result[index++].setLeaseIdMismatchFault(FaultFactory.makeLeaseIdMismatchFault("jobInfo", "3", "leaseId mismatch", "N/A"));
                break;
            case JobIdFilterFailure.STATUS_ERRORCODE:
                result[index] = new JobInfoResult();
                result[index].setJobDescriptionId(creamBaseUrl + jobFilterResult.get(i).getJobId());
                result[index++].setJobStatusInvalidFault(FaultFactory.makeJobStatusInvalidFault("jobInfo", "4", "job status mismatch", "N/A"));
                break;
            }
        }

        return result;
    }

    public Result[] jobPurge(JobId[] jobIds, String[] compatibleStatus, Calendar fromDate, Calendar toDate, String delegationId, String leaseId) throws Authorization_Fault, Generic_Fault, InvalidArgument_Fault {
        checkInitialization();

        return insertCommand(jobIds, compatibleStatus, fromDate, toDate, delegationId, leaseId, new JobPurgeCmd());
    }

    public Result[] jobResume(JobId[] jobIds, String[] compatibleStatus, Calendar fromDate, Calendar toDate, String delegationId, String leaseId) throws OperationNotSupported_Fault, Authorization_Fault, Generic_Fault, InvalidArgument_Fault {
        checkInitialization();

        return insertCommand(jobIds, compatibleStatus, fromDate, toDate, delegationId, leaseId, new JobResumeCmd());
    }

    public Result[] jobSetLeaseId(String newLeaseId, JobFilter filter) throws Authorization_Fault, Generic_Fault, InvalidArgument_Fault {
        checkInitialization();

        return insertCommand(filter.getJobId(), filter.getStatus(), filter.getFromDate(), filter.getToDate(), filter.getDelegationId(), filter.getLeaseId(), new JobSetLeaseIdCmd(
                newLeaseId));
    }

    public Result[] jobStart(JobId[] jobIds, String[] compatibleStatus, Calendar fromDate, Calendar toDate, String delegationId, String leaseId) throws Authorization_Fault, Generic_Fault, InvalidArgument_Fault {
        checkInitialization();

        return insertCommand(jobIds, compatibleStatus, fromDate, toDate, delegationId, leaseId, new JobStartCmd());
    }

    public JobStatusResult[] jobStatus(JobId[] jobIds, String[] compatibleStatus, Calendar fromDate, Calendar toDate, String delegationId, String leaseId) throws Authorization_Fault, Generic_Fault, InvalidArgument_Fault {
        checkInitialization();

        logger.debug("Begin jobStatus");

        int index = 0;
        JobStatusCmd cmd = new JobStatusCmd();
        insertCommand(jobIds, compatibleStatus, fromDate, toDate, delegationId, leaseId, cmd);

        List<JobStatus> statusList = cmd.getJobStatusResult();
        List<JobIdFilterResult> jobIdFilterList = cmd.getJobIdFilterResult();

        String creamBaseUrl = "";
        if (creamURL != null) {
            logger.debug("creamURL = " + creamURL.toString());
            int serviceStringIndex = creamURL.toString().lastIndexOf("ce-cream/services");
            if (serviceStringIndex != -1) {
                creamBaseUrl = creamURL.toString().substring(0, serviceStringIndex);
            }
        }

        JobStatusResult[] jsResult = null;

        if (jobIds == null || jobIds.length == 0) {
            jsResult = new JobStatusResult[statusList.size()];
        } else {
            jsResult = new JobStatusResult[jobIdFilterList.size()];

            for (JobIdFilterResult filterResult : jobIdFilterList) {
                switch (filterResult.getErrorCode()) {
                case JobIdFilterFailure.OK_ERRORCODE:
                    break;
                case JobIdFilterFailure.DATE_ERRORCODE:
                    jsResult[index] = new JobStatusResult();
                    jsResult[index].setJobDescriptionId(creamBaseUrl + filterResult.getJobId());
                    jsResult[index++].setDateMismatchFault(FaultFactory.makeDateMismatchFault("jobStatus", "0", "fromDate/toDate mismatch", "N/A"));
                    break;
                case JobIdFilterFailure.DELEGATIONID_ERRORCODE:
                    jsResult[index] = new JobStatusResult();
                    jsResult[index].setJobDescriptionId(creamBaseUrl + filterResult.getJobId());
                    jsResult[index++].setDelegationIdMismatchFault(FaultFactory.makeDelegationIdMismatchFault("jobStatus", "1", "delegationId mismatch", "N/A"));
                    break;
                case JobIdFilterFailure.JOBID_ERRORCODE:
                    jsResult[index] = new JobStatusResult();
                    jsResult[index].setJobDescriptionId(creamBaseUrl + filterResult.getJobId());
                    jsResult[index++].setJobUnknownFault(FaultFactory.makeJobUnknownFault("jobStatus", "2", "job not found", "N/A"));
                    break;
                case JobIdFilterFailure.LEASEID_ERRORCODE:
                    jsResult[index] = new JobStatusResult();
                    jsResult[index].setJobDescriptionId(creamBaseUrl + filterResult.getJobId());
                    jsResult[index++].setLeaseIdMismatchFault(FaultFactory.makeLeaseIdMismatchFault("jobStatus", "3", "leaseId mismatch", "N/A"));
                    break;
                case JobIdFilterFailure.STATUS_ERRORCODE:
                    jsResult[index] = new JobStatusResult();
                    jsResult[index].setJobDescriptionId(creamBaseUrl + filterResult.getJobId());
                    jsResult[index++].setJobStatusInvalidFault(FaultFactory.makeJobStatusInvalidFault("jobStatus", "4", "job status mismatch", "N/A"));
                    break;
                }
            }
        }

        if (statusList != null && statusList.size() > 0) {
            for (JobStatus jobStatus : statusList) {
                JobId jobId = new JobId();
                jobId.setId(jobStatus.getJobId());
                jobId.setCreamURL(creamURL);

                Status status = new Status();
                status.setDescription(jobStatus.getDescription());
                status.setExitCode(jobStatus.getExitCode());
                status.setFailureReason(jobStatus.getFailureReason());
                status.setJobId(jobId);
                status.setName(jobStatus.getName());
                status.setTimestamp(jobStatus.getTimestamp());

                jsResult[index] = new JobStatusResult();
                jsResult[index].setJobDescriptionId(creamBaseUrl + jobStatus.getJobId());
                jsResult[index++].setJobStatus(status);
            }
        }

        logger.debug("End jobStatus");

        return jsResult;
    }

    public Result[] jobSuspend(JobId[] jobIds, String[] compatibleStatus, Calendar fromDate, Calendar toDate, String delegationId, String leaseId) throws OperationNotSupported_Fault, Authorization_Fault, Generic_Fault, InvalidArgument_Fault {
        checkInitialization();
        
        return insertCommand(jobIds, compatibleStatus, fromDate, toDate, delegationId, leaseId, new JobSuspendCmd());
    }

    public QueryEventResult queryEvent(String fromEventId, String toEventId, Calendar fromDate, Calendar toDate, String eventType, int maxQueryEventResultSize, int verbosityLevel, Property[] properties) throws Authorization_Fault, Generic_Fault, InvalidArgument_Fault {
        checkInitialization();

        logger.debug("Begin queryEvent");

        if (eventType == null) {
            throw FaultFactory.makeInvalidArgumentFault("queryEvent", "0", "event type not specified!", null);
        }

        if (!eventType.equalsIgnoreCase("JOB_STATUS")) {
            throw FaultFactory.makeInvalidArgumentFault("queryEvent", "0", "event type \"" + eventType + "\" not supported!", null);
        }

        // logger.info(org.apache.axis.utils.XMLUtils.ElementToString(MessageContext.getCurrentContext().getRequestMessage().getSOAPEnvelope()));

        long startTime = Calendar.getInstance().getTimeInMillis();

        QueryEventResult result = new QueryEventResult();

        QueryEventCmd queryEventCmd = new QueryEventCmd(eventType);
        queryEventCmd.setIsAdmin(false);
        queryEventCmd.setFromDate(fromDate);
        queryEventCmd.setToDate(toDate);
        queryEventCmd.setFromEventId(fromEventId);
        queryEventCmd.setToEventId(toEventId);
        queryEventCmd.setMaxQueryEventResultSize(maxQueryEventResultSize);

        boolean isIceRequest = false;
        String iceId = null;
        SOAPEnvelope envelope = MessageContext.getCurrentMessageContext().getEnvelope();

        if (envelope != null) {
            SOAPHeader header = envelope.getHeader();

            if (header != null) {
                for (SOAPHeaderBlock headerBlock : (List<SOAPHeaderBlock>)header.getHeaderBlocksWithNSURI("http://glite.org/2007/11/ce/cream/types")) {
                    if ("iceId".equalsIgnoreCase(headerBlock.getLocalName())) {
                        iceId = headerBlock.getText();
                        headerBlock.setProcessed();
                        break;
                    }
                }
            } else {
                logger.debug("queryEvent: SOAP header not found!");
            }
        } else {
            logger.debug("queryEvent: SOAP envelope not found!");
        }
        
        if (iceId != null && iceId.length() > 0) {
            logger.debug("queryEvent: found iceId = " + iceId);
            queryEventCmd.setUserId(getUserDN_FQAN(null) + "@" + iceId);
        } else {
            queryEventCmd.setUserId(getUserDN_FQAN(null));
        }
        
        if (properties != null && properties.length > 0) {
            List<String> statusList = new ArrayList<String>(0);

            for (int i = 0; i < properties.length; i++) {
                if ("status".equalsIgnoreCase(properties[i].getName())) {
                    statusList.add(properties[i].getValue());
                }
            }

            queryEventCmd.addParameter("statusList", statusList);
        } else if (isIceRequest) {
            List<String> statusList = new ArrayList<String>(9);
            statusList.add(JobStatus.getNameByType(JobStatus.ABORTED));
            statusList.add(JobStatus.getNameByType(JobStatus.CANCELLED));
            statusList.add(JobStatus.getNameByType(JobStatus.DONE_FAILED));
            statusList.add(JobStatus.getNameByType(JobStatus.DONE_OK));
            statusList.add(JobStatus.getNameByType(JobStatus.HELD));
            statusList.add(JobStatus.getNameByType(JobStatus.REALLY_RUNNING));
            statusList.add(JobStatus.getNameByType(JobStatus.RUNNING));

            queryEventCmd.addParameter("statusList", statusList);
        }

        try {
            CommandManager.getInstance().execute(queryEventCmd);

            List<Event> eventList = queryEventCmd.geEvents();

            org.glite.ce.creamapi.ws.cream2.types.Event[] eventArray = new org.glite.ce.creamapi.ws.cream2.types.Event[eventList.size()];

            Event event = null;

            for (int i = 0; i < eventList.size(); i++) {
                event = eventList.get(i);

                eventArray[i] = new org.glite.ce.creamapi.ws.cream2.types.Event();
                eventArray[i].setId(event.getId());
                eventArray[i].setTimestamp(event.getTimestamp());
                eventArray[i].setType(event.getType());

                if (logger.isDebugEnabled()) {
                    logger.debug("event [id=" + event.getId() + "; type=" + event.getType() + "; timestamp=" + event.getTimestamp().getTime() + "]");
                }

                int index = 0;
                String value = null;
                Property[] eventPropertyArray = new Property[event.getProperty().size()];

                for (org.glite.ce.creamapi.eventmanagement.Property eventProperty : event.getProperty()) {
                    value = (String) eventProperty.getValue();
                    value = value == null ? "N/A" : value;

                    eventPropertyArray[index] = new Property();
                    eventPropertyArray[index].setName(eventProperty.getName());
                    eventPropertyArray[index].setValue(value);

                    logger.debug("event property name=" + eventPropertyArray[index].getName() + " value=" + eventPropertyArray[index].getValue());

                    index++;
                }

                eventArray[i].setProperty(eventPropertyArray);
            }

            result.setEvent(eventArray);
        } catch (IllegalArgumentException e) {
            logger.error("IllegalArgumentException: " + e.getMessage());
            throw FaultFactory.makeGenericFault("execute", "0", e.getMessage(), e.getMessage());
        } catch (CommandException e) {
            logger.error("CommandException: " + e.getMessage());
            throw FaultFactory.makeGenericFault("execute", "0", e.getMessage(), e.getMessage());
        } catch (CommandManagerException e) {
            logger.error("CommandManagementException: " + e.getMessage());
            throw FaultFactory.makeGenericFault("execute", "0", e.getMessage(), e.getMessage());
        }

        result.setDbId("" + dbID);
        result.setQueryExecutionTimeInMillis(Long.valueOf(Calendar.getInstance().getTimeInMillis() - startTime));

        logger.debug("End queryEvent");

        return result;
    }

    public Lease setLease(String leaseId, Calendar leaseTime) throws Authorization_Fault, Generic_Fault, InvalidArgument_Fault {
        checkInitialization();

        if (leaseId == null) {
            throw FaultFactory.makeInvalidArgumentFault("jobLease", "0", "leaseId not specified!", null);
        }

        SetLeaseCmd cmd = new SetLeaseCmd(leaseId, leaseTime);
        cmd.setUserId(getUserDN_FQAN(null));
        cmd.setIsAdmin(CEUtils.isAdmin());

        try {
            CommandManager.getInstance().execute(cmd);
        } catch (IllegalArgumentException e) {
            throw FaultFactory.makeGenericFault("jobLease", "0", "IllegalArgumentException occured", e.getMessage());
        } catch (CommandException e) {
            throw FaultFactory.makeGenericFault("jobLease", "1", "CommandException occured", e.getMessage());
        } catch (CommandManagerException e) {
            throw FaultFactory.makeGenericFault("jobLease", "2", "CommandManagementException occured", e.getMessage());
        }

        Lease lease = new Lease();
        lease.setLeaseId(leaseId);
        lease.setLeaseTime(cmd.getLeaseTimeResult());

        return lease;
    }

    /**
     * Enables/Disables the CREAM service for accepting new job submissions.
     * Only the CREAM administrator can invoke this operation.
     * 
     * @throws RemoteException
     * @throws AuthenticationFault
     *             thrown in case of authentication problems.
     * @throws AuthorizationFault
     *             thrown in case of authorization problems.
     * @throws GenericFault
     *             thrown if any other possible error occurs.
     */
    public void acceptNewJobSubmissions(boolean b) throws Authorization_Fault, Generic_Fault {
        checkInitialization();
        
        if (!CEUtils.isAdmin()) {
            Authorization_Fault authFault = new Authorization_Fault();
            authFault.setFaultMessage(FaultFactory.makeAuthorizationFault("acceptNewJobSubmissions", "0", 
                    "Operation reserved only to administrator!", "Operation reserved only to administrator!"));
            throw authFault;
        }

        SetAcceptNewJobsCmd cmd = null;
        if (b) {
            cmd = new SetAcceptNewJobsCmd(JobSubmissionManager.SUBMISSION_ENABLED);    
        } else {
            cmd = new SetAcceptNewJobsCmd(JobSubmissionManager.SUBMISSION_DISABLED_BY_ADMIN);
        }
        
        cmd.setUserId(getUserDN_FQAN(null));
        cmd.setIsAdmin(true);

        try {
            CommandManager.getInstance().execute(cmd);
            serviceInfo.setDoesAcceptNewJobSubmissions(b);
            //DBInfoManager.updateSubmissionEnabled(JobDBInterface.JOB_DATASOURCE_NAME, b);
        } catch (Throwable e) {
            throw FaultFactory.makeGenericFault("acceptNewJobSubmissions", "0", e.getMessage(), "N/A");
        }
    }
}
