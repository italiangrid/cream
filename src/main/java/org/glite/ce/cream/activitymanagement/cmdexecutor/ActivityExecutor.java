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

package org.glite.ce.cream.activitymanagement.cmdexecutor;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.SortedSet;

import javax.sql.DataSource;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.log4j.Logger;
import org.glite.ce.commonj.db.DatabaseException;
import org.glite.ce.commonj.db.DatasourceManager;
import org.glite.ce.commonj.utils.CEUtils;
import org.glite.ce.cream.activitymanagement.ActivityCmd;
import org.glite.ce.cream.activitymanagement.ActivityCmd.ActivityCommandField;
import org.glite.ce.cream.activitymanagement.ActivityCmd.ActivityCommandName;
import org.glite.ce.cream.activitymanagement.db.ActivityDBImplementation;
import org.glite.ce.cream.blahmanagement.BLAHClient;
import org.glite.ce.cream.blahmanagement.BLAHException;
import org.glite.ce.cream.blahmanagement.BLAHJob;
import org.glite.ce.cream.blahmanagement.BLAHJobStatus;
import org.glite.ce.cream.blahmanagement.BLAHJobStatusChangeListener;
import org.glite.ce.cream.configuration.ServiceConfig;
import org.glite.ce.creamapi.activitymanagement.Activity;
import org.glite.ce.creamapi.activitymanagement.ActivityCommand;
import org.glite.ce.creamapi.activitymanagement.ActivityStatus;
import org.glite.ce.creamapi.activitymanagement.ActivityStatus.StatusAttributeName;
import org.glite.ce.creamapi.activitymanagement.ActivityStatus.StatusName;
import org.glite.ce.creamapi.activitymanagement.ListActivitiesResult;
import org.glite.ce.creamapi.activitymanagement.db.ActivityDBInterface;
import org.glite.ce.creamapi.activitymanagement.wrapper.activitymanagement.types.NotifyMessageType;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.ActivityDescription;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.InputFile;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.OutputFile;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.Resources;
import org.glite.ce.creamapi.cmdmanagement.AbstractCommandExecutor;
import org.glite.ce.creamapi.cmdmanagement.Command;
import org.glite.ce.creamapi.cmdmanagement.CommandException;
import org.glite.ce.creamapi.cmdmanagement.CommandExecutorException;
import org.glite.ce.creamapi.delegationmanagement.DelegationManagerInterface;
import org.glite.ce.creamapi.jobmanagement.db.DBInfoManager;

public class ActivityExecutor extends AbstractCommandExecutor implements BLAHJobStatusChangeListener {
    private static final Logger logger = Logger.getLogger(ActivityExecutor.class.getName());

    // labels
    public static final String ACTIVITY_STATUS_MONITOR_AGE     = "ACTIVITY_STATUS_MONITOR_AGE";
    public static final String ACTIVITY_STATUS_MONITOR_RATE    = "ACTIVITY_STATUS_MONITOR_RATE";
    public static final String ACTIVITY_WRAPPER_TEMPLATE_PATH  = "ACTIVITY_WRAPPER_TEMPLATE_PATH";
    public static final String BDII_URI                        = "BDII_URI";
    public static final String BDII_RATE                       = "BDII_RATE";
    public static final String BLAH_BIN_PATH                   = "BLAH_BIN_PATH";
    public static final String BLAH_COMMAND_TIMEOUT            = "BLAH_COMMAND_TIMEOUT";
    public static final String BLAH_PREFIX                     = "BLAH_PREFIX";
    public static final String BLAH_NOTIFICATION_LISTENER_PORT = "BLAH_NOTIFICATION_LISTENER_PORT";
    public static final String BLAH_NOTIFIER_RETRY_COUNT       = "BLAH_NOTIFIER_RETRY_COUNT";
    public static final String BLAH_NOTIFIER_RETRY_DELAY       = "BLAH_NOTIFIER_RETRY_DELAY";
    public static final String COPY_PROXY_MIN_RETRY_WAIT       = "COPY_PROXY_MIN_RETRY_WAIT";
    public static final String COPY_RETRY_COUNT_ISB            = "COPY_RETRY_COUNT_ISB";
    public static final String COPY_RETRY_COUNT_OSB            = "COPY_RETRY_COUNT_OSB";
    public static final String COPY_RETRY_FIRST_WAIT_ISB       = "COPY_RETRY_FIRST_WAIT_ISB";
    public static final String COPY_RETRY_FIRST_WAIT_OSB       = "COPY_RETRY_FIRST_WAIT_OSB";
    public static final String CREATE_SANDBOX_BIN_PATH         = "CREATE_SANDBOX_BIN_PATH";
    public static final String CREATE_WRAPPER_BIN_PATH         = "CREATE_WRAPPER_BIN_PATH";
    public static final String DEFAULT_LRMS_NAME               = "DEFAULT_LRMS_NAME";
    public static final String DEFAULT_QUEUE_NAME              = "DEFAULT_QUEUE_NAME";
    public static final String DELEGATION_TIME_SLOT            = "DELEGATION_TIME_SLOT";
    public static final String HOST_NAME                       = "HOST_NAME";
    public static final String LIMIT_FOR_LIST_ACTIVITIES       = "LIMIT_FOR_LIST_ACTIVITIES";
    public static final String NOT_AVAILABLE_VALUE             = "N/A";
    public static final String SANDBOX_DIR                     = "SANDBOX_DIR";
    public static final String SERVICE_URL                     = "SERVICE_URL";
    public static final String SERVICE_GSI_URL                 = "SERVICE_GSI_URL";
    public static final String PURGE_SANDBOX_BIN_PATH          = "PURGE_SANDBOX_BIN_PATH";

    private static String hostAddress = null;
    private static String hostName = null;
    private static String serviceUrl = null;
    private static String serviceGSIUrl = null;    
    private static String delegationSuffix = null;
    private static String activityWrapperNotificationStatusURI = null;
    private static BLAHClient blahClient = null;
    private boolean initialized = false;
    private static ActivityDBInterface activityDB = null;
    private static int limitForListActivities = 1000;
    private static int bdiiRate = 60; //sec
    private static StdHandler stdHandler = null;
    private static GLUE2Handler glue2Handler = null;
    private static ActivityStatusMonitor activityStatusMonitor = null;

    public ActivityExecutor() throws CommandExecutorException {
        super("ActivityExecutor", ActivityCmd.ACTIVITY_MANAGEMENT);

        setCommands(ActivityCommandName.toArrayList());

        addParameter(SANDBOX_DIR, "/var/cream-es_sandbox");
        addParameter(BLAH_BIN_PATH, "/usr/bin/blahpd");

        dataSourceName = ActivityDBInterface.ACTIVITY_DATASOURCE_NAME;
    }

    private void cancelActivity(Command command) throws CommandException {
        logger.debug("BEGIN cancelActivity");

        Activity activity = getActivity(command);

        SortedSet<ActivityStatus> states = activity.getStates();

        if (states.isEmpty()) {
            throw new CommandException("none status available");     
        }

        ActivityStatus status = states.last();
        if (StatusName.TERMINAL == status.getStatusName()) {
            throw new CommandException("invalid state (terminal)");     
        }

        String localUser = activity.getProperties().get(Activity.LOCAL_USER);
        if (localUser == null) {
            throw new CommandException("LOCAL_USER not speficied!");            
        }

        if (isActivityCheckOn(command)) {
            return;
        }

        Boolean isSuccess = Boolean.TRUE;

        if (StatusName.ACCEPTED == status.getStatusName() || StatusName.PREPROCESSING == status.getStatusName()) {
            ActivityStatus activityStatus = new ActivityStatus();
            activityStatus.setStatusName(StatusName.TERMINAL);
            activityStatus.setTimestamp(newXMLGregorianCalendar(Calendar.getInstance()));
            activityStatus.setDescription("cancelled by the user");

            if (StatusName.PREPROCESSING == status.getStatusName()) {
                activityStatus.getStatusAttributes().add(StatusAttributeName.PREPROCESSING_CANCEL);
            }

            try {
                activityDB.insertActivityStatus(activity.getId(), activityStatus);
                logger.info("activity " + activity.getId() + " status changed => " + activityStatus.toString());
                
                activityDB.insertActivityCommand(activity.getId(), makeActivityCommand(command.getName(), isSuccess));
            } catch (Throwable t) {
                throw new CommandException(t.getMessage());
            }
        } else {
            try {
                String lrmsId = activity.getProperties().get(Activity.LRMS_ABS_LAYER_ID);

                if (lrmsId == null) {
                    throw new CommandException("LRMS_ABS_LAYER_ID not speficied!");    
                }

                blahClient.cancel(lrmsId, localUser);
            } catch (Throwable e) {
                isSuccess = Boolean.FALSE;
                throw new CommandException("blah error: " + e.getMessage());
            } finally {
                try {
                    activityDB.insertActivityCommand(activity.getId(), makeActivityCommand(command.getName(), isSuccess));
                } catch (Throwable t) {
                    throw new CommandException(t.getMessage());
                }
            }
        }

        logger.debug("END cancelActivity");
    }

    private void createActivity(Command command) throws CommandException {
        logger.debug("BEGIN createActivity");

        if (command.getUserId() == null) {
            throw new CommandException("userId not specified!");
        }
        
        String activityId = null;
        String gsiURL = getParameterValueAsString(command, ActivityCommandField.SERVICE_GSI_URL, true);
        String userDN_X500 = getParameterValueAsString(command, ActivityCommandField.USER_DN_X500, true);
        String userDN_RFC2253 = getParameterValueAsString(command, ActivityCommandField.USER_DN_RFC2253, true);
        String userFQAN = getParameterValueAsString(command, ActivityCommandField.USER_FQAN, true);
        String localUser = getParameterValueAsString(command, ActivityCommandField.LOCAL_USER, true);
        String localUserGroup = getParameterValueAsString(command, ActivityCommandField.LOCAL_USER_GROUP, true);
        String virtualOrganisation = getParameterValueAsString(command, ActivityCommandField.VIRTUAL_ORGANISATION, true);
        String activitySandboxDir = NOT_AVAILABLE_VALUE;
        String sandboxDir = getParameterValueAsString(SANDBOX_DIR);
        String createSandboxBinPath = getParameterValueAsString(CREATE_SANDBOX_BIN_PATH);
        String delegationSandboxPath = getParameterValueAsString(command, ActivityCommandField.DELEGATION_SANDBOX_PATH, false);
        String serviceURL = getParameterValueAsString(command, ActivityCommandField.SERVICE_URL, true);
        Runtime runtime = Runtime.getRuntime();
        BufferedReader in = null;
        BufferedReader readErr = null;
        Process proc = null;
        
        ActivityDescription activityDescription = (ActivityDescription) getParameterValue(command, ActivityCommandField.ACTIVITY_DESCRIPTION, true);
        
        Activity activity = new Activity();
        activity.setUserId(command.getUserId());
        activity.setActivityIdentification(activityDescription.getActivityIdentification());
        activity.setApplication(activityDescription.getApplication());
        activity.setDataStaging(activityDescription.getDataStaging());
        activity.setResources(activityDescription.getResources());
        activity.getCommands().add(makeActivityCommand(command.getName(), Boolean.TRUE));
        activity.getStates().add(makeActivityStatus(StatusName.ACCEPTED));
        activity.getProperties().put(Activity.USER_DN_X500, userDN_X500);
        activity.getProperties().put(Activity.USER_DN_RFC2253, userDN_RFC2253);
        activity.getProperties().put(Activity.USER_FQAN, userFQAN);
        activity.getProperties().put(Activity.LOCAL_USER, localUser);
        activity.getProperties().put(Activity.LOCAL_USER_GROUP, localUserGroup);
        activity.getProperties().put(Activity.SERVICE_URL, serviceURL);
        activity.getProperties().put(Activity.VIRTUAL_ORGANISATION, virtualOrganisation);
        
        if (delegationSandboxPath != null) {
            activity.getProperties().put(Activity.DELEGATION_SANDBOX_PATH, delegationSandboxPath);
            activity.getProperties().put(Activity.DELEGATION_SANDBOX_URI, gsiURL + delegationSandboxPath);
        }
        
        String queueName = null;
        if (activity.getResources() == null) {
            queueName = getParameterValueAsString(DEFAULT_QUEUE_NAME);

            Resources resources = new Resources();
            resources.setQueueName(queueName);

            activity.setResources(resources);
        } else if (activity.getResources().getQueueName() == null) {
            queueName = getParameterValueAsString(DEFAULT_QUEUE_NAME);

            activity.getResources().setQueueName(queueName);
        } else {
            queueName = activity.getResources().getQueueName();
        }

        try {
            activityId = activityDB.insertActivity(activity);
            logger.info("new activity " + activityId + " created! " + activity.getStates().last());
        } catch (Throwable t) {
            logger.error(t.getMessage(), t);
            throw new CommandException("cannot store the activity: " + t.getMessage());
        }
        
        activity.setId(activityId);

        ActivityStatus activityStatus = null;
        try {
            logger.debug("createActivity [localUser=" + localUser + "; createSandboxBinPath=" + createSandboxBinPath + "; sandboxDir=" + sandboxDir + "; userId=" + command.getUserId() + "; activityId=" + activity.getId() + "; gsiURL=" + gsiURL + "]");

            String[] cmd = new String[] { "sudo", "-S", "-n", "-u", localUser, createSandboxBinPath, sandboxDir, command.getUserId(), activity.getId(), "true" };

            proc = runtime.exec(cmd);
            in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            readErr = new BufferedReader(new InputStreamReader(proc.getErrorStream()));

            activitySandboxDir = in.readLine();

            if (activitySandboxDir == null) {
                throw new CommandException("cannot get the activity sandbox dir path");
            }       

            activityStatus = makeActivityStatus(StatusName.PREPROCESSING);
            if (activity.getDataStaging() != null && activity.getDataStaging().isClientDataPush()) {
                activityStatus.getStatusAttributes().add(StatusAttributeName.CLIENT_STAGEIN_POSSIBLE);
            }
        } catch (Throwable t) {
            logger.error(t.getMessage(), t);
            activityStatus = makeActivityStatus(StatusName.TERMINAL);
            activityStatus.getStatusAttributes().add(StatusAttributeName.PREPROCESSING_FAILURE);
            activityStatus.setDescription(t.getMessage());

            if (proc != null) {
                proc.destroy();
            }
        } finally {
            if (proc != null) {
                try {
                    proc.waitFor();
                } catch (InterruptedException e) {}

                try {
                    proc.getInputStream().close();
                } catch (IOException ioe) {}

                try {
                    proc.getErrorStream().close();
                } catch (IOException ioe) {}

                try {
                    proc.getOutputStream().close();
                } catch (IOException ioe) {}
            }

            if (in != null) {
                try {
                    in.close();
                } catch (IOException ioe) {}
                in = null;
            }

            if (readErr != null) {
                try {
                    readErr.close();
                } catch (IOException ex) {}
                readErr = null;
            }

            try {
                activityDB.insertActivityStatus(activity.getId(), activityStatus);
                logger.info("activity " + activityId + " status changed => " + activityStatus.toString());
            } catch (Throwable t) {
                throw new CommandException(t.getMessage());
            }
        }

        String stageInDir = gsiURL + activitySandboxDir + "/ISB";
        String stageOutDir = gsiURL + activitySandboxDir + "/OSB";

        activity.getProperties().put(Activity.SANDBOX_PATH, activitySandboxDir);
        activity.getProperties().put(Activity.STAGE_IN_URI, stageInDir);
        activity.getProperties().put(Activity.STAGE_OUT_URI, stageOutDir);

        try {
            activityDB.updateActivity(activity);
        } catch (Throwable t) {
            logger.error(t.getMessage(), t);
            throw new CommandException("cannot store the activity: " + t.getMessage());
        }

        command.getResult().addParameter(ActivityCommandField.ACTIVITY_STATUS.name(), activityStatus);
        command.getResult().addParameter(ActivityCommandField.ACTIVITY_ID.name(), activityId);
        command.getResult().addParameter(ActivityCommandField.STAGE_IN_URI.name(), stageInDir);
        command.getResult().addParameter(ActivityCommandField.STAGE_OUT_URI.name(), stageOutDir);
        logger.debug("END createActivity");
    }

    public void destroy() {
        logger.info("destroy invoked!");

        blahClient.terminate();
        stdHandler.terminate();
        activityStatusMonitor.terminate();

        super.destroy();

        logger.info("destroyed!");
    }

    public void doOnJobStatusChange(BLAHJobStatus status) {
        if (status != null) {
            logger.debug("doOnJobStatusChange: new status received " + status.toString());

            ActivityStatus activityStatus = new ActivityStatus();
            activityStatus.setTimestamp(newXMLGregorianCalendar(status.getChangeTime()));
            String workerNode = null;
            Activity activity = null;

            try {
                activity = activityDB.getActivity("CR_ES" + status.getClientJobId(), null);

                if (activity == null) {
                    throw new Exception("activity CR_ES" + status.getClientJobId() + " not found!");
                }                        
            } catch (Throwable t) {
                logger.warn(t.getMessage());
                return;
            }

            switch (status.getStatus()) {
                case BLAHJobStatus.IDLE:
                    for (ActivityStatus s : activity.getStates()) {
                        if (s.getStatusName() == StatusName.PROCESSING_QUEUED) {
                            return;
                        }
                    }
                    activityStatus.setStatusName(StatusName.PROCESSING_QUEUED);
                    break;

                case BLAHJobStatus.RUNNING:
                    activityStatus.setStatusName(StatusName.PROCESSING_RUNNING);
                    workerNode = status.getWorkerNode();
                    break;

                case BLAHJobStatus.REALLY_RUNNING:
                    activityStatus.setStatusName(StatusName.PROCESSING_RUNNING);
                    activityStatus.getStatusAttributes().add(StatusAttributeName.APP_RUNNING);
                    break;

                case BLAHJobStatus.CANCELLED:                    
                    activityStatus.setStatusName(StatusName.TERMINAL);
                    activityStatus.getStatusAttributes().add(StatusAttributeName.PROCESSING_CANCEL);
                    activityStatus.setDescription("cancelled by the admin");

                    for (ActivityCommand activityCommand : activity.getCommands()) {
                        if (ActivityCommandName.CANCEL_ACTIVITY.name().equals(activityCommand.getName())) {
                            activityStatus.setDescription("cancelled by the user");
                            break;
                        }
                    }
                    
                    stdHandler.add(activity.getId());
                    break;

                case BLAHJobStatus.HELD:
                    activityStatus.setStatusName(StatusName.PROCESSING_RUNNING);

                    boolean found = false;

                    for (ActivityCommand activityCommand : activity.getCommands()) {
                        if (ActivityCommandName.PAUSE_ACTIVITY.name().equals(activityCommand.getName())) {
                            found = true;
                            break;
                        }
                    }

                    if (found) {
                        activityStatus.getStatusAttributes().add(StatusAttributeName.CLIENT_PAUSED);
                    } else {
                        activityStatus.getStatusAttributes().add(StatusAttributeName.SERVER_PAUSED);
                    }
                    break;

                case BLAHJobStatus.DONE:
                    String reason = status.getReason();
                    String exitCode = status.getExitCode();

                    activityStatus.setStatusName(StatusName.TERMINAL);
                    activityStatus.setDescription(reason);
                    activityStatus.setIsTransient(status.getSource() == BLAHJobStatus.BLAH_NOTIFICATION_LISTENER);

                    if (reason == null || reason.endsWith("reason=0")) {
                        for (OutputFile outputFile : activity.getDataStaging().getOutputFile()) {
                            if (outputFile.getTarget().size() == 0) {
                                activityStatus.getStatusAttributes().add(StatusAttributeName.CLIENT_STAGEOUT_POSSIBLE);
                                break;
                            }
                        }
                    } else {
                        if (reason.endsWith("reason=999")) {
                            exitCode = "999";
                            activityStatus.setDescription("reason=999 (job not found)");
                        } else {
                            if (reason.equals("Job has been terminated (got SIGTERM)")) {                        
                                activityStatus.getStatusAttributes().add(StatusAttributeName.PROCESSING_CANCEL);

                                for (ActivityCommand activityCommand : activity.getCommands()) {
                                    if (ActivityCommandName.CANCEL_ACTIVITY.name().equals(activityCommand.getName())) {
                                        activityStatus.setDescription("cancelled by the user");
                                        break;
                                    }
                                }
                            } else {
                                activityStatus.getStatusAttributes().add(StatusAttributeName.APP_FAILURE);
                            }

                            stdHandler.add(activity.getId());
                        }
                    }

                    if (exitCode != null) {
                        try {
                            activity.getProperties().put(Activity.EXIT_CODE, exitCode);
                            activityDB.updateActivity(activity);
                        } catch (Throwable t) {
                            logger.warn(t.getMessage(), t);
                            return;
                        }
                    }
            }

            try { 
                if (workerNode != null) {
                    activity.getProperties().put(Activity.WORKER_NODE, workerNode);
                    activityDB.updateActivity(activity);
                }

                if (!activity.getStates().contains(activityStatus)) {
                    activityDB.insertActivityStatus(activity.getId(), activityStatus);
                    StringBuffer info = new StringBuffer();

                    info.append("activity ").append(activity.getId());
                    info.append(" status changed => ").append(activityStatus.toString());

                    if (activity.getProperties().containsKey(Activity.WORKER_NODE)) {
                        info.append("; workerNode=").append(activity.getProperties().get(Activity.WORKER_NODE));
                    }

                    if (activity.getProperties().containsKey(Activity.LRMS_ABS_LAYER_ID)) {
                        info.append("; lrsmsId=").append(activity.getProperties().get(Activity.LRMS_ABS_LAYER_ID));
                    }

                    if (status.getSource() == BLAHJobStatus.BLAH_NOTIFIER) {
                        info.append("; source=BLAH_NOTIFIER");
                    } else {
                        info.append("; source=ACTIVITY_WRAPPER");                        
                    }

                    logger.info(info);
                }
            } catch (Throwable t) {
                logger.warn("activity CR_ES" + status.getClientJobId() + " not found!: " + t.getMessage(), t);
            }
        }

        //            if (status.getSource() == BLAHJobStatus.BLAH_NOTIFIER) {
        //                if (activity.getStates().contains(activityStatus)) {
        //                    activity.getStates().remove(activityStatus);
        //                } else if (activity.getStates().last().getStatusName() == activityStatus.getStatusName()) {
        //                    activity.getStates().remove(activity.getStates().last());
        //                }
        //            }

    }

    public void execute(Command command) throws CommandExecutorException, CommandException {
        logger.debug("BEGIN execute");

        if (!initialized) {
            throw new CommandExecutorException(getName() + " not initialized!");
        }

        if (command == null) {
            throw new IllegalArgumentException("command not defined!");
        }

        if (!command.getCategory().equalsIgnoreCase(getCategory())) {
            throw new CommandException("command category mismatch: found \"" + command.getCategory() + "\" required \"" + getCategory() + "\"");
        }

        if (command.containsParameterKey("USER_DN")) {
            command.addParameter("USER_DN", normalize(command.getParameterAsString("USER_DN")));
        }
        
        try {
            switch (ActivityCommandName.valueOf(command.getName())) {
                case CANCEL_ACTIVITY:
                    cancelActivity(command);
                    break;
                    
                case CREATE_ACTIVITY:
                    createActivity(command);
                    break;

                case START_ACTIVITY:
                    startActivity(command);
                    break;

                case GET_ACTIVITY_INFO:
                    getActivityInfo(command);
                    break;

                case GET_ACTIVITY_STATUS:
                    getActivityStatus(command);
                    break;
                    
                case GET_DATABASE_VERSION:
                    getDatabaseVersion(command);
                    break;
                    
                case GET_RESOURCE_INFO:
                    getResourceInfo(command);
                    break;
                    
                case LIST_ACTIVITIES:
                    listActivities(command);
                    break;
                    
                case NOTIFY_SERVICE:
                    notifyService(command);
                    break;
                    
                case PAUSE_ACTIVITY:
                    pauseActivity(command);
                    break;
                  
                case QUERY_RESOURCE_INFO:
                    queryResourceInfo(command);
                    break;
                  
                case RESTART_ACTIVITY:
                    restartActivity(command);
                    break;
                    
                case RESUME_ACTIVITY:
                    resumeActivity(command);
                    break;
                    
                case WIPE_ACTIVITY:
                    wipeActivity(command);
                    break;
                    
                default:
                    logger.error("command \"" + command.getName() + "\" not found!");
                    throw new CommandExecutorException("command \"" + command.getName() + "\" not found!");
            }
        } catch (CommandException ex) {
            logger.error("Execution of the command \"" + command.getName() + "\" failed: " + ex.getMessage());
            throw ex;
        }

        logger.debug("END execute");
    }

    public void execute(List<Command> commandList) throws CommandExecutorException, CommandException {
        if (commandList == null) {
            return;
        }

        for (Command command : commandList) {
            execute(command);
        }
    }

    private Activity getActivity(Command command) throws CommandException {
        if (command.getUserId() == null) {
            throw new CommandException("userId not specified!");
        }

        Activity activity = null;
        String activityId = getParameterValueAsString(command, ActivityCommandField.ACTIVITY_ID, true);

        try {
            activity = activityDB.getActivity(activityId, command.getUserId());
        } catch (Throwable t) {
            if (t.getMessage() == null) {
                throw new CommandException("N/A");
            } else if (t.getMessage().indexOf("is not enabled for that operation") != -1) {
                throw new CommandException("activity " + activityId + " not found!");
            } else {
                throw new CommandException(t.getMessage());
            }
        }

        return activity;
    }

    private void getActivityInfo(Command command) throws CommandException {
        logger.debug("BEGIN getActivityInfo");

        command.getResult().addParameter(ActivityCommandField.ACTIVITY_DESCRIPTION.name(), getActivity(command));

        logger.debug("END getActivityInfo");
    }

    private void getActivityStatus(Command command) throws CommandException {
        logger.debug("BEGIN getActivityStatus");

        Activity activity = getActivity(command);
//        ActivityStatus lastActivityStatus = null;
//        
//        for (ActivityStatus activityStatus : activity.getStates()) {
//            if (!activityStatus.isTransient()) {
//                lastActivityStatus = activityStatus;
//            }
//        }
//
//        if (lastActivityStatus == null) {
//            throw new CommandException("none status available");       
//        }

        
        if (activity.getStates().isEmpty()) {
            throw new CommandException("none status available");       
        }
        
        command.getResult().addParameter(ActivityCommandField.ACTIVITY_STATUS.name(), activity.getStates().last());        

        logger.debug("END getActivityStatus");
    }
    
    private String getDatabaseVersion(Command command) throws CommandException {
        logger.debug("BEGIN getDatabaseVersion");

        String version = "N/A";

        try {
            version = DBInfoManager.getDBVersion(dataSourceName);
        } catch (Exception e) {
            throw new CommandException("Failure on storage interaction: " + e.getMessage());
        }

        command.getResult().addParameter("DATABASE_VERSION", version);
        logger.debug("END getDatabaseVersion");

        return version;
    }
    
    private Object getParameterValue(Command command, ActivityCommandField field, boolean throwException) throws CommandException {
        if (command == null) {
            throw new CommandException("command not specified!");
        }

        if (field == null) {
            throw new CommandException("paramenter key not specified!");
        }

        Object value = command.getParameter(field.name());

        // Check for a null proxy
        if (value == null && throwException) {
            throw new CommandException("parameter \"" + field.name() + "\" not specified!");
        }

        return value;
    }

    private String getParameterValueAsString(Command command, ActivityCommandField field, boolean throwException) throws CommandException {
        if (command == null) {
            throw new CommandException("command not specified!");
        }

        if (field == null) {
            throw new CommandException("paramenter key not specified!");
        }

        Object value = command.getParameter(field.name());

        if (throwException) {
            if (value == null) {
                throw new CommandException("parameter \"" + field.name() + "\" not specified!");
            }

            if (!(value instanceof String)) {
                throw new CommandException("the value of the parameter \"" + field.name() + "\" is not an instance of the String type!");
            }
        }

        return (String) value;
    }

    private void getResourceInfo(Command command) throws CommandException {
        try {
            command.getResult().addParameter(ActivityCommandField.COMPUTING_SERVICE.name(), glue2Handler.getComputingService());
        } catch (Throwable t) {
            throw new CommandException(t.getMessage());
        }       
    }

    public void initExecutor() throws CommandExecutorException {
        logger.debug("BEGIN initExecutor");

        if (!initialized) {
            logger.info("initalizing the " + getName() + " executor...");
            
            ServiceConfig serviceConfig = ServiceConfig.getConfiguration();
            if (serviceConfig == null) {
                throw new CommandExecutorException("Configuration error: cannot initialize the ServiceConfig");
            }

            HashMap<String, DataSource> dataSources = serviceConfig.getDataSources();

            if (dataSources == null) {
                throw new CommandExecutorException("Datasource is empty!");
            }

            if (dataSources.containsKey(dataSourceName)) {
                if (DatasourceManager.addDataSource(dataSourceName, dataSources.get(dataSourceName))) {
                    logger.info("new dataSource \"" + dataSourceName + "\" added to the DatasourceManager");
                } else {
                    logger.info("the dataSource \"" + dataSourceName + "\" already exist!");
                }
            } else {
                throw new CommandExecutorException("Datasource \"" + dataSourceName + "\" not found!");
            }

            if (dataSources.containsKey(DelegationManagerInterface.DELEGATION_DATASOURCE_NAME)) {
                if (DatasourceManager.addDataSource(DelegationManagerInterface.DELEGATION_DATASOURCE_NAME, dataSources.get(DelegationManagerInterface.DELEGATION_DATASOURCE_NAME))) {
                    logger.info("new dataSource \"" + DelegationManagerInterface.DELEGATION_DATASOURCE_NAME + "\" added to the DatasourceManager");
                } else {
                    logger.info("the dataSource \"" + DelegationManagerInterface.DELEGATION_DATASOURCE_NAME + "\" already exist!");
                }
            } else {
                throw new CommandExecutorException("Datasource \"" + DelegationManagerInterface.DELEGATION_DATASOURCE_NAME + "\" not found!");
            }

            try {
                delegationSuffix = DBInfoManager.getDelegationSuffix(DelegationManagerInterface.DELEGATION_DATASOURCE_NAME);
            } catch (Throwable t) {
                throw new CommandExecutorException("cannot get instance of DelegationManager: " + t.getMessage());
            }

            if (delegationSuffix == null || delegationSuffix.equals("")) {
                throw new CommandExecutorException("delegationSuffix not defined!");
            }

            try {
                activityDB = new ActivityDBImplementation();
                //activityDB = ActivityDBInMemory.getInstance();
            } catch (Throwable t) {
                logger.error(t.getMessage(), t);
                throw new CommandExecutorException("ActivityDB initialization failed: " + t.getMessage());
            }

            if (!containsParameterKey(LIMIT_FOR_LIST_ACTIVITIES)) {
            	logger.info("LIMIT_FOR_LIST_ACTIVITIES parameter not defined: using the default value (" + limitForListActivities + ")");
            } else {
            	try {
            		limitForListActivities = Integer.parseInt(getParameterValueAsString(LIMIT_FOR_LIST_ACTIVITIES));
            	} catch(Throwable t) {
                    throw new CommandExecutorException("wrong value for the LIMIT_FOR_LIST_ACTIVITIES parameter found: " + t.getMessage());
            	}
            }

            if (!containsParameterKey(DEFAULT_LRMS_NAME)) {
                throw new CommandExecutorException("DEFAULT_LRMS_NAME parameter not defined!");
            }

            if (!containsParameterKey(DEFAULT_QUEUE_NAME)) {
                throw new CommandExecutorException("DEFAULT_QUEUE_NAME parameter not defined!");
            }

            if (!containsParameterKey(DELEGATION_TIME_SLOT)) {
                throw new CommandExecutorException("DELEGATION_TIME_SLOT parameter not defined!");
            }

            if (!containsParameterKey(SANDBOX_DIR)) {
                throw new CommandExecutorException("SANDBOX_DIR parameter not defined!");
            }

            if (!containsParameterKey(PURGE_SANDBOX_BIN_PATH)) {
                throw new CommandExecutorException("PURGE_SANDBOX_BIN_PATH parameter not defined!");
            }

            if (!containsParameterKey(ACTIVITY_WRAPPER_TEMPLATE_PATH)) {
                addParameter(ACTIVITY_WRAPPER_TEMPLATE_PATH, ServiceConfig.getConfiguration().getConfigurationDirectory());
            }

            if (!containsParameterKey(CREATE_SANDBOX_BIN_PATH)) {
                throw new CommandExecutorException("CREATE_SANDBOX_BIN_PATH parameter not defined!");
            }

            if (!containsParameterKey(CREATE_WRAPPER_BIN_PATH)) {
                throw new CommandExecutorException("CREATE_WRAPPER_BIN_PATH parameter not defined!");
            }

            if (!containsParameterKey(BDII_URI)) {
                throw new CommandExecutorException("BDII_URI parameter not defined!");
            }

            if (!containsParameterKey(BDII_RATE)) {
            	logger.info("BDII_RATE parameter not defined: using the default value (" + bdiiRate + "sec)");
            } else {
            	try {
            		bdiiRate = Integer.parseInt(getParameterValueAsString(BDII_RATE));
            	} catch(Throwable t) {
                    throw new CommandExecutorException("wrong value for the BDII_RATE parameter found: " + t.getMessage());
            	}
            }

            try {
                glue2Handler = new GLUE2Handler(getParameterValueAsString(BDII_URI), bdiiRate);
            } catch (Exception e) {
                throw new CommandExecutorException("cannot instantiate the GLUE2Handler: " + e.getMessage());
            }

            stdHandler = new StdHandler();
            blahClient = new BLAHClient();
            blahClient.setJobStatusChangeListener(this);

            if (containsParameterKey(BLAH_BIN_PATH)) {
                blahClient.setExecutablePath(getParameterValueAsString(BLAH_BIN_PATH));
            }

            if (containsParameterKey(BLAH_NOTIFICATION_LISTENER_PORT)) {
                try {
                    blahClient.setNotificationListenerPort(Integer.parseInt(getParameterValueAsString(BLAH_NOTIFICATION_LISTENER_PORT)));
                } catch (Throwable t) {
                    logger.warn("Found a wrong value for the BLAH_NOTIFICATION_LISTENER_PORT parameter: using the default (" + blahClient.getNotificationListenerPort() + ")");
                }
            }

            if (containsParameterKey(BLAH_COMMAND_TIMEOUT)) {
                try {
                    blahClient.setCommandTimeout(Integer.parseInt(getParameterValueAsString(BLAH_COMMAND_TIMEOUT)));
                } catch (Throwable t) {
                    logger.warn("Found a wrong value for the BLAH_COMMAND_TIMEOUT parameter: using the default (" + blahClient.getCommandTimeout() + ")");
                }
            }

            if (containsParameterKey(BLAH_NOTIFIER_RETRY_COUNT)) {
                try {
                    blahClient.setNotifierRetryCount(Integer.parseInt(getParameterValueAsString(BLAH_NOTIFIER_RETRY_COUNT)));
                } catch (Throwable t) {
                    logger.warn("Found a wrong value for the BLAH_NOTIFIER_RETRY_COUNT parameter: using the default (" + blahClient.getNotifierRetryCount() + ")");
                }
            }

            if (containsParameterKey(BLAH_NOTIFIER_RETRY_DELAY)) {
                try {
                    blahClient.setNotifierRetryDelay(Integer.parseInt(getParameterValueAsString(BLAH_NOTIFIER_RETRY_DELAY)));
                } catch (Throwable t) {
                    logger.warn("Found a wrong value for the BLAH_NOTIFIER_RETRY_DELAY parameter: using the default (" + blahClient.getNotifierRetryDelay() + ")");
                }
            }

            if (containsParameterKey(BLAH_PREFIX)) {
                blahClient.setPrefix(getParameterValueAsString(BLAH_PREFIX));
            }

            try {
                blahClient.init();
            } catch (BLAHException e) {
                throw new CommandExecutorException("blahClient initialization failed: " + e.getMessage());
            }

            try {
                if (containsParameterKey(HOST_NAME)) {
                    hostName = getParameterValueAsString(HOST_NAME);
                    //hostAddress = InetAddress.getAllByName(hostName)[0].getHostAddress();
                } else {
                    hostName = InetAddress.getLocalHost().getCanonicalHostName();                    
                }

                hostAddress = InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                throw new CommandExecutorException("cannot get the host's address: " + e.getMessage());
            }

            activityWrapperNotificationStatusURI = hostAddress + ":" + blahClient.getNotificationListenerPort();
            
            long activityStatusMonitorRate = 0;
            if (!containsParameterKey(ACTIVITY_STATUS_MONITOR_RATE)) {
                throw new CommandExecutorException("ACTIVITY_STATUS_MONITOR_RATE parameter not defined!");
            } else {
                try {
                    activityStatusMonitorRate = Long.parseLong(getParameterValueAsString(ACTIVITY_STATUS_MONITOR_RATE));
                } catch(Throwable t) {
                    throw new CommandExecutorException("wrong value for the ACTIVITY_STATUS_MONITOR_RATE parameter found: " + t.getMessage());
                }

                if (activityStatusMonitorRate < 0) {
                    throw new CommandExecutorException("wrong value for the ACTIVITY_STATUS_MONITOR_RATE parameter found: negative");
                }
            }

            int activityStatusMonitorAge = 0;
            if (!containsParameterKey(ACTIVITY_STATUS_MONITOR_AGE)) {
                throw new CommandExecutorException("ACTIVITY_STATUS_MONITOR_AGE parameter not defined!");
            } else {
                try {
                    activityStatusMonitorAge = Integer.parseInt(getParameterValueAsString(ACTIVITY_STATUS_MONITOR_AGE));
                } catch(Throwable t) {
                    throw new CommandExecutorException("wrong value for the ACTIVITY_STATUS_MONITOR_AGE parameter found: " + t.getMessage());
                }

                if (activityStatusMonitorAge < 0) {
                    throw new CommandExecutorException("wrong value for the ACTIVITY_STATUS_MONITOR_AGE parameter found: negative");
                }
            }

            activityStatusMonitor = new ActivityStatusMonitor(blahClient, getParameterValueAsString(DEFAULT_LRMS_NAME), activityStatusMonitorRate, activityStatusMonitorAge);
            initialized = true;
            logger.info(getName() + " executor initialized!");
        }

        logger.debug("END initExecutor");
    }

    private boolean isActivityCheckOn(Command command) {
    	boolean result = false;

    	if (command.containsParameterKey(ActivityCommandField.ACTIVITY_CHECK.name())) {
    		result = ActivityCommandField.ACTIVITY_CHECK_ON == (ActivityCommandField)command.getParameter(ActivityCommandField.ACTIVITY_CHECK.name());
    	}

    	return result;
    }

    private void listActivities(Command command) throws CommandException {
    	Calendar fromDate = (Calendar)getParameterValue(command, ActivityCommandField.FROM_DATE, false);
    	Calendar toDate = (Calendar)getParameterValue(command, ActivityCommandField.TO_DATE, false);
        List<ActivityStatus> activityStatusList = null;
        int limit = limitForListActivities;
        
        if (command.containsParameterKey(ActivityCommandField.ACTIVITY_STATUS_LIST.name())) {
            activityStatusList = (List<ActivityStatus>)getParameterValue(command, ActivityCommandField.ACTIVITY_STATUS_LIST, false);
        } else {
            activityStatusList = new ArrayList<ActivityStatus>(0);
        }

        if (command.containsParameterKey(ActivityCommandField.LIMIT.name())) {
            limit = ((BigInteger)getParameterValue(command, ActivityCommandField.LIMIT, false)).intValue();
            limit = Math.min(limit, limitForListActivities);
        }

        try {
            ListActivitiesResult result = activityDB.listActivities(CEUtils.getXMLGregorianCalendar(fromDate), CEUtils.getXMLGregorianCalendar(toDate), activityStatusList, limit, command.getUserId());
            command.getResult().addParameter(ActivityCommandField.ACTIVITY_ID_LIST.name(), result.getActivityIdList());
            command.getResult().addParameter(ActivityCommandField.IS_TRUNCATED.name(), result.isTruncated());  
        } catch (Throwable t) {
            throw new CommandException(t.getMessage());
        }
    }

    private ActivityCommand makeActivityCommand(String name, Boolean isSuccess) {
        ActivityCommand activityCommand = new ActivityCommand(name);
        activityCommand.setTimestamp(newXMLGregorianCalendar());
        activityCommand.setIsSuccess(isSuccess);

        return activityCommand;
    }

    private ActivityStatus makeActivityStatus(StatusName name) {
        ActivityStatus activityStatus = new ActivityStatus(name);
        activityStatus.setTimestamp(newXMLGregorianCalendar());

        return activityStatus;
    }

    private XMLGregorianCalendar newXMLGregorianCalendar() {
        return newXMLGregorianCalendar(null);
    }

    private XMLGregorianCalendar newXMLGregorianCalendar(Calendar date) {
        XMLGregorianCalendar timestamp = null;
        
        try {
            GregorianCalendar gregorianCalendar = new GregorianCalendar();
            
            if (date != null) {
                gregorianCalendar.setTimeInMillis(date.getTimeInMillis());
            }

            timestamp = DatatypeFactory.newInstance().newXMLGregorianCalendar(gregorianCalendar);
        } catch (DatatypeConfigurationException ex) {
            logger.warn(ex.getMessage());
        }

        return timestamp;
    }

    private String normalize(String s) {
        if (s != null) {
            return s.replaceAll("\\W", "_");
        }
        return null;
    }

    private void notifyService(Command command) throws CommandException {
        logger.debug("BEGIN notifyService");

        String activityId = getParameterValueAsString(command, ActivityCommandField.ACTIVITY_ID, true);
        String message = getParameterValueAsString(command, ActivityCommandField.NOTIFY_MESSAGE, true);

        NotifyMessageType notifyMessageType = NotifyMessageType.fromValue(message);
        Activity activity = getActivity(command);

        if (activity.getStates().isEmpty()) {
            throw new CommandException("none status found about the activity " + activityId);
        }

        ActivityStatus status = activity.getStates().last();

        if (NotifyMessageType.CLIENT_DATAPULL_DONE != notifyMessageType && NotifyMessageType.CLIENT_DATAPUSH_DONE != notifyMessageType) {
            throw new CommandException("message type not supported!");
        }

        if (NotifyMessageType.CLIENT_DATAPUSH_DONE == notifyMessageType) {
            if (StatusName.PREPROCESSING != status.getStatusName()) {
                throw new CommandException("invalid state (" + status.getStatusName() + ")");
            }

            List<StatusAttributeName> attributes = status.getStatusAttributes();
            if (attributes == null || !attributes.contains(StatusAttributeName.CLIENT_STAGEIN_POSSIBLE)) {
                throw new CommandException("invalid state (activity not in client-stagein-possible state)");
            }

            if (isActivityCheckOn(command)) {
                return;
            }

            String localUser = activity.getProperties().get(Activity.LOCAL_USER);
            String stageInDir = activity.getProperties().get(Activity.SANDBOX_PATH) + File.separator + "ISB";
            List<String> inputFileList = new ArrayList<String>(0);

            Runtime runtime = Runtime.getRuntime();
            BufferedReader in = null;
            BufferedReader readErr = null;
            Process proc = null;
            String[] cmd = new String[] { "sudo", "-S", "-n", "-u", localUser, "/bin/ls", stageInDir };

            try {
                proc = runtime.exec(cmd);
                in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
                readErr = new BufferedReader(new InputStreamReader(proc.getErrorStream()));

                String s = null;
                while ((s = in.readLine()) != null) {
                    inputFileList.add(s);
                }

                if ((s = readErr.readLine()) != null) {
                    throw new Exception(s);
                }
            } catch (Throwable t) {
                logger.error(t.getMessage(), t);

                if (proc != null) {
                    proc.destroy();
                }

                throw new CommandException("cannot list the stage-in directory: " + t.getMessage());
            } finally {
                if (proc != null) {
                    try {
                        proc.waitFor();
                    } catch (InterruptedException e) {}

                    try {
                        proc.getInputStream().close();
                    } catch (IOException ioe) {}

                    try {
                        proc.getErrorStream().close();
                    } catch (IOException ioe) {}

                    try {
                        proc.getOutputStream().close();
                    } catch (IOException ioe) {}
                }

                if (in != null) {
                    try {
                        in.close();
                    } catch (IOException ioe) {}
                    in = null;
                }

                if (readErr != null) {
                    try {
                        readErr.close();
                    } catch (IOException ex) {}
                    readErr = null;
                }
            }


            for (InputFile inputFile : activity.getDataStaging().getInputFile()) {
                inputFileList.remove(inputFile.getName());
            }

            InputFile inputFile = null;
            for (String file : inputFileList) {
                inputFile = new InputFile();
                inputFile.setName(file);
                inputFile.setIsExecutable(Boolean.FALSE);

                activity.getDataStaging().getInputFile().add(inputFile);                
            }

            submitActivity(activity, command.getName());
        } else {
            if (StatusName.TERMINAL != status.getStatusName()) {
                throw new CommandException("invalid state (" + status.getStatusName() + ")");
            }

            List<StatusAttributeName> attributes = status.getStatusAttributes();
            if (attributes == null || !attributes.contains(StatusAttributeName.CLIENT_STAGEOUT_POSSIBLE)) {
                throw new CommandException("invalid state (activity not in client-stageout-possible state)");
            }

            attributes.remove(StatusAttributeName.CLIENT_STAGEOUT_POSSIBLE);

            try {
                activityDB.updateActivityStatus(status);
                logger.info("activity " + activity.getId() + " status updated => " + status.toString());
            } catch (Throwable t) {
                throw new CommandException(t.getMessage());
            }
        }

        logger.debug("END notifyService");
    }

    private void submitActivity(Activity activity, String commandName) throws CommandException {
        String userDNX500 = activity.getProperties().get(Activity.USER_DN_X500);
        String userFQAN = activity.getProperties().get(Activity.USER_FQAN);
        String localUser = activity.getProperties().get(Activity.LOCAL_USER);
        String activitySandboxDir = activity.getProperties().get(Activity.SANDBOX_PATH);
        String lrmsName = getParameterValueAsString(DEFAULT_LRMS_NAME);
        String queueName = activity.getResources().getQueueName();
        String virtualOrganisation = activity.getProperties().get(Activity.VIRTUAL_ORGANISATION);
        
        activity.getVolatileProperties().put(Activity.CE_ID, hostAddress + ":8443/cream-" + lrmsName + "-" + queueName);
        activity.getVolatileProperties().put(Activity.CE_HOSTNAME, hostName);
        activity.getVolatileProperties().put(Activity.TEMPLATE_PATH, getParameterValueAsString(ACTIVITY_WRAPPER_TEMPLATE_PATH));
        activity.getVolatileProperties().put(Activity.ACTIVITY_WRAPPER_NOTIFICATION_STATUS_URI, activityWrapperNotificationStatusURI);
        activity.getVolatileProperties().put(Activity.DELEGATION_TIME_SLOT, getParameterValueAsString(DELEGATION_TIME_SLOT));
        activity.getVolatileProperties().put(Activity.DELEGATION_FILE_NAME_SUFFIX, delegationSuffix);
        
        if (containsParameterKey(Activity.COPY_PROXY_MIN_RETRY_WAIT)) {
            activity.getVolatileProperties().put(Activity.COPY_PROXY_MIN_RETRY_WAIT, getParameterValueAsString(COPY_PROXY_MIN_RETRY_WAIT));
        }

        if (containsParameterKey(Activity.COPY_RETRY_COUNT_ISB)) {
            activity.getVolatileProperties().put(Activity.COPY_RETRY_COUNT_ISB, getParameterValueAsString(COPY_RETRY_COUNT_ISB));
        }

        if (containsParameterKey(Activity.COPY_RETRY_FIRST_WAIT_ISB)) {
            activity.getVolatileProperties().put(Activity.COPY_RETRY_FIRST_WAIT_ISB, getParameterValueAsString(COPY_RETRY_FIRST_WAIT_ISB));
        }

        if (containsParameterKey(Activity.COPY_RETRY_COUNT_OSB)) {
            activity.getVolatileProperties().put(Activity.COPY_RETRY_COUNT_OSB, getParameterValueAsString(COPY_RETRY_COUNT_OSB));
        }

        if (containsParameterKey(Activity.COPY_RETRY_FIRST_WAIT_OSB)) {
            activity.getVolatileProperties().put(Activity.COPY_RETRY_FIRST_WAIT_OSB, getParameterValueAsString(COPY_RETRY_FIRST_WAIT_OSB));
        }
        
        Runtime runtime = Runtime.getRuntime();
        BufferedReader in = null;
        BufferedOutputStream os = null;
        BufferedReader readErr = null;
        Process proc = null;
        String[] cmd = new String[] { "sudo", "-S", "-n", "-u", localUser, getParameterValueAsString(CREATE_WRAPPER_BIN_PATH), activitySandboxDir + File.separator + activity.getId() + "_activityWrapper.sh" };

        try {
            proc = runtime.exec(cmd);
            in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            os = new BufferedOutputStream(proc.getOutputStream());
            readErr = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
            
            os.write(ActivityWrapper.buildWrapper(activity).getBytes());
            os.flush();
            os.close();
            os = null;
        } catch (Throwable t) {
            ActivityStatus activityStatus = makeActivityStatus(StatusName.TERMINAL);
            activityStatus.getStatusAttributes().add(StatusAttributeName.PREPROCESSING_FAILURE);
            activityStatus.setDescription(t.getMessage());

            try {
                activityDB.insertActivityStatus(activity.getId(), activityStatus);
                logger.info("activity " + activity.getId() + " status changed => " + activityStatus.toString());
            } catch (Throwable t1) {
                logger.error(t1.getMessage(), t1);
            }

            logger.error(t.getMessage(), t);

            if (proc != null) {
                proc.destroy();
            }

            throw new CommandException("cannot create the activity wrapper: " + t.getMessage());
        } finally {
            if (proc != null) {
                try {
                    proc.waitFor();
                } catch (InterruptedException e) {}

                try {
                    proc.getInputStream().close();
                } catch (IOException ioe) {}

                try {
                    proc.getErrorStream().close();
                } catch (IOException ioe) {}

                try {
                    proc.getOutputStream().close();
                } catch (IOException ioe) {}
            }

            if (in != null) {
                try {
                    in.close();
                } catch (IOException ioe) {}
                in = null;
            }

            if (os != null) {
                try {
                    os.close();
                } catch (IOException ioe) {}
                os = null;
            }

            if (readErr != null) {
                try {
                    readErr.close();
                } catch (IOException ex) {}
                readErr = null;
            }
        }
        
        BLAHJob blahJob = new BLAHJob();
        blahJob.setUserDN(userDNX500);
        blahJob.setUserFQAN(userFQAN);
        blahJob.setIwd(activitySandboxDir);
        blahJob.setJobId(activity.getId());
        blahJob.setLocalUser(localUser);
        blahJob.setExecutableFile(activitySandboxDir + File.separator + activity.getId() + "_activityWrapper.sh");
        blahJob.setStandardErrorFile(activitySandboxDir + File.separator + "StandardError");
        blahJob.setStandardOuputFile(activitySandboxDir + File.separator + "StandardOutput");
        blahJob.setLRMS(lrmsName);
        blahJob.setQueue(queueName);
        blahJob.setHostNumber(1); //TBD
        blahJob.setNodeNumber(1); //TBD
        blahJob.setVirtualOrganisation(virtualOrganisation);
        blahJob.setTransferInput(activity.getProperties().get(Activity.TRANSFER_INPUT));
        blahJob.setTransferOutput(activity.getProperties().get(Activity.TRANSFER_OUTPUT));
        blahJob.setTransferOutputRemaps(activity.getProperties().get(Activity.TRANSFER_OUTPUT_REMAPS));

        activity.getProperties().remove(Activity.TRANSFER_INPUT);
        activity.getProperties().remove(Activity.TRANSFER_OUTPUT);
        activity.getProperties().remove(Activity.TRANSFER_OUTPUT_REMAPS);
        activity.getProperties().remove(Activity.DELEGATION_SANDBOX_PATH);

        String failureReason = null;
        ActivityStatus status = null;
        
        try {
            status = makeActivityStatus(StatusName.PROCESSING_ACCEPTING);
            activityDB.insertActivityStatus(activity.getId(), status);
            logger.info("activity " + activity.getId() + " status changed => " + status.toString());
        } catch (Throwable t) {
            throw new CommandException(t.getMessage());
        }

        String blahId = null;

        for (int i=1; i<4 && blahId == null; i++) {
            failureReason = null;
            
            try {
                blahId = blahClient.submit(blahJob);
            } catch (BLAHException be) {
                logger.warn("submission to BLAH failed [activityId=" + activity.getId() + "; reason=" + failureReason + "; retry count=" + i + "/3]");
                failureReason = be.getMessage();

                synchronized(status) {
                    try {
                        logger.debug("sleeping 10 sec...");
                        status.wait(10000);
                        logger.debug("sleeping 10 sec... done");
                    } catch (InterruptedException e) {
                    }
                }
            } 
        }

        if (blahId == null) {
            try {
                failureReason = "submission to BLAH failed [retry count=3]" + (failureReason != null ? ": " + failureReason : "");
                ActivityStatus activityStatus = makeActivityStatus(StatusName.TERMINAL);
                activityStatus.setDescription(failureReason);
                activityStatus.getStatusAttributes().add(StatusAttributeName.PREPROCESSING_FAILURE);

                activityDB.insertActivityStatus(activity.getId(), activityStatus);
                logger.info("activity " + activity.getId() + " status changed => " + activityStatus.toString());
            } catch (Throwable t) {
                throw new CommandException(t.getMessage());
            }

            throw new CommandException(failureReason);
        }
        
        activity.getProperties().put(Activity.LRMS_ABS_LAYER_ID, blahId);

        try {
            status = makeActivityStatus(StatusName.PROCESSING_QUEUED);
            activityDB.insertActivityStatus(activity.getId(), status);
            logger.info("activity " + activity.getId() + " status changed => " + status.toString() + "; lrsmsId=" + blahId);

            activityDB.insertActivityCommand(activity.getId(), makeActivityCommand(commandName, Boolean.valueOf(blahId != null)));
            activityDB.updateActivity(activity);
        } catch (Throwable t) {
            throw new CommandException(t.getMessage());
        }
    }

    private void pauseActivity(Command command) throws CommandException {
        logger.debug("BEGIN pauseActivity");

        Activity activity = getActivity(command);
        
        SortedSet<ActivityStatus> states = activity.getStates();
        
        if (states.isEmpty()) {
            throw new CommandException("none status available");       
        }

        ActivityStatus status = states.last();
        
        if (StatusName.TERMINAL == status.getStatusName()) {
            throw new CommandException("invalid state (terminal)");     
        }

        if (status.getStatusAttributes().contains(StatusAttributeName.CLIENT_PAUSED)) {
            throw new CommandException("invalid state (" + status.getStatusName() + ":client-paused)");
        }
        
        if (status.getStatusAttributes().contains(StatusAttributeName.SERVER_PAUSED)) {
            throw new CommandException("invalid state (" + status.getStatusName() + ":server-paused)");
        }
        
        String localUser = activity.getProperties().get(Activity.LOCAL_USER);
        if (localUser == null) {
            throw new CommandException("LOCAL_USER not speficied!");            
        }
        
        if (isActivityCheckOn(command)) {
            return;
        }
        
        Boolean isSuccess = Boolean.TRUE;

        if (StatusName.ACCEPTED == status.getStatusName()) {
            status.setTimestamp(newXMLGregorianCalendar(Calendar.getInstance()));
            status.getStatusAttributes().add(StatusAttributeName.CLIENT_PAUSED);

            try {
                activityDB.updateActivityStatus(status);
                logger.info("activity " + activity.getId() + " status updated => " + status.toString());
                
                activityDB.insertActivityCommand(activity.getId(), makeActivityCommand(command.getName(), isSuccess));
            } catch (Throwable t) {
                throw new CommandException(t.getMessage());
            }
        } else {
            try {
                String lrmsId = activity.getProperties().get(Activity.LRMS_ABS_LAYER_ID);

                if (lrmsId == null) {
                    throw new CommandException("LRMS_ABS_LAYER_ID not speficied!");    
                }

                blahClient.suspend(lrmsId, localUser);
            } catch (Throwable e) {
                isSuccess = Boolean.FALSE;
                throw new CommandException("blah error: " + e.getMessage());
            } finally {
                try {
                    activityDB.insertActivityCommand(activity.getId(), makeActivityCommand(command.getName(), isSuccess));
                } catch (Throwable t) {
                    throw new CommandException(t.getMessage());
                }
            }
        }

        logger.debug("END pauseActivity");
    }
    
    private void queryResourceInfo(Command command) throws CommandException {
        try {
            command.getResult().addParameter(ActivityCommandField.ACTIVITY_GLUE2_ATTRIBUTE_LIST.name(), glue2Handler.executeXPathQuery(command.getParameterAsString(ActivityCommandField.XPATH_QUERY.name())));
        } catch (Throwable t) {
            throw new CommandException(t.getMessage());
        }
    }

    private void restartActivity(Command command) throws CommandException {
        throw new CommandException("operation not yet implemented!");
    }

    private void resumeActivity(Command command) throws CommandException {
        logger.debug("BEGIN resumeActivity");

        Activity activity = getActivity(command);

        SortedSet<ActivityStatus> states = activity.getStates();
        
        if (states.isEmpty()) {
            throw new CommandException("none status available");       
        }

        ActivityStatus status = states.last();

        if (StatusName.TERMINAL == status.getStatusName()) {
            throw new CommandException("invalid state (terminal)");     
        }
        
        if (!status.getStatusAttributes().contains(StatusAttributeName.CLIENT_PAUSED) &&
                !status.getStatusAttributes().contains(StatusAttributeName.SERVER_PAUSED)) {
            throw new CommandException("invalid state (activity not paused)");
        } 

        String localUser = activity.getProperties().get(Activity.LOCAL_USER);
        if (localUser == null) {
            throw new CommandException("LOCAL_USER not speficied!");
        }
        
        if (isActivityCheckOn(command)) {
            return;
        }
        
        Boolean isSuccess = Boolean.TRUE;

        if (StatusName.ACCEPTED == status.getStatusName()) {
            status.setTimestamp(newXMLGregorianCalendar(Calendar.getInstance()));
            status.getStatusAttributes().remove(StatusAttributeName.CLIENT_PAUSED);

            try {
                activityDB.updateActivityStatus(status);
                logger.info("activity " + activity.getId() + " status updated => " + status.toString());

                activityDB.insertActivityCommand(activity.getId(), makeActivityCommand(command.getName(), isSuccess));
            } catch (Throwable t) {
                throw new CommandException(t.getMessage());
            }
        } else {
            try {
                String lrmsId = activity.getProperties().get(Activity.LRMS_ABS_LAYER_ID);

                if (lrmsId == null) {
                    throw new CommandException("LRMS_ABS_LAYER_ID not speficied!");    
                }

                blahClient.resume(lrmsId, localUser);
            } catch (Throwable e) {
                isSuccess = Boolean.FALSE;
                throw new CommandException("blah error: " + e.getMessage());
            } finally {
                try {
                    activityDB.insertActivityCommand(activity.getId(), makeActivityCommand(command.getName(), isSuccess));
                } catch (Throwable t) {
                    throw new CommandException(t.getMessage());
                }
            }
        }

        logger.debug("END resumeActivity");
    }

    private void startActivity(Command command) throws CommandException {
        logger.debug("BEGIN startActivity");

        Activity activity = getActivity(command);

        if (!activity.getStates().last().getStatusAttributes().contains(StatusAttributeName.CLIENT_PAUSED) &&
                (activity.getDataStaging() == null || !activity.getDataStaging().isClientDataPush())) {
            submitActivity(activity, command.getName());
        } 

        logger.debug("END startActivity");
    }
    
    private void wipeActivity(Command command) throws CommandException {
        logger.debug("BEGIN wipeActivity");

        Activity activity = getActivity(command);

        SortedSet<ActivityStatus> states = activity.getStates();
        
        if (states.isEmpty()) {
            throw new CommandException("none status available");       
        }

        ActivityStatus status = states.last();

        if (StatusName.TERMINAL != status.getStatusName()) {
            throw new CommandException("invalid state (" + status.getStatusName() + ")");     
        }

        String localUser = activity.getProperties().get(Activity.LOCAL_USER);
        if (localUser == null) {
            throw new CommandException("LOCAL_USER not speficied!");
        }

        String sandboxPath = activity.getProperties().get(Activity.SANDBOX_PATH);
        if (sandboxPath == null) {
            throw new CommandException("SANDBOX_PATH not speficied!");
        }
        
        if (isActivityCheckOn(command)) {
            return;
        }

        Process proc = null;

        try {
            String[] cmd = new String[] { "sudo", "-S", "-n", "-u", localUser, getParameterValueAsString(PURGE_SANDBOX_BIN_PATH), sandboxPath };

            proc = Runtime.getRuntime().exec(cmd);
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (proc != null) {
                try {
                    proc.waitFor();
                } catch (InterruptedException e) {
                }

                StringBuffer errorMessage = null;

                if(proc.exitValue() != 0) {
                    BufferedReader readErr = new BufferedReader(new InputStreamReader(proc.getErrorStream()));

                    errorMessage = new StringBuffer();
                    String inputLine = null;

                    try {
                        while ((inputLine = readErr.readLine()) != null) {
                            errorMessage.append(inputLine);
                        }
                    } catch (IOException ioe) {
                        logger.error(ioe.getMessage());
                    } finally {
                        try {
                            readErr.close();
                        } catch (IOException ioe) {}
                    }

                    if (errorMessage.length() > 0) {
                        errorMessage.append("\n");
                    }
                }

                try {
                    proc.getInputStream().close();
                } catch (IOException ioe) {}
                
                try {
                    proc.getErrorStream().close();
                } catch (IOException ioe) {}
                
                try {
                    proc.getOutputStream().close();
                } catch (IOException ioe) {}

                if(errorMessage != null) {
                    throw new CommandException(errorMessage.toString());
                }
            }
        }

        try {
            activityDB.deleteActivity(activity.getId(), null);
        } catch (DatabaseException e) {
            logger.error(e.getMessage());
            throw new CommandException(e.getMessage());
        }

        logger.debug("END wipeActivity");
    }
}
