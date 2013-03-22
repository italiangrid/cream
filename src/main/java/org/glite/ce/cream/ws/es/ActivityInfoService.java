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

package org.glite.ce.cream.ws.es;

/**
 *  CreateActivitySkeleton java skeleton for the axisService
 */
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

import org.apache.axis2.AxisFault;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.context.ServiceContext;
import org.apache.axis2.databinding.types.NCName;
import org.apache.axis2.databinding.types.URI;
import org.apache.axis2.description.AxisService;
import org.apache.axis2.description.Parameter;
import org.apache.axis2.engine.AxisConfiguration;
import org.apache.axis2.service.Lifecycle;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.glite.ce.commonj.utils.CEUtils;
import org.glite.ce.cream.activitymanagement.ActivityCmd;
import org.glite.ce.cream.activitymanagement.ActivityCmd.ActivityCommandField;
import org.glite.ce.cream.activitymanagement.ActivityCmd.ActivityCommandName;
import org.glite.ce.cream.cmdmanagement.CommandManager;
import org.glite.ce.cream.configuration.ServiceConfig;
import org.glite.ce.creamapi.activitymanagement.Activity;
import org.glite.ce.creamapi.activitymanagement.ActivityCommand;
import org.glite.ce.creamapi.activitymanagement.ActivityStatus;
import org.glite.ce.creamapi.activitymanagement.ActivityStatus.StatusAttributeName;
import org.glite.ce.creamapi.activitymanagement.ActivityStatus.StatusName;
import org.glite.ce.creamapi.cmdmanagement.CommandManagerInterface;
import org.glite.ce.creamapi.ws.es.activityinfo.AccessControlFault;
import org.glite.ce.creamapi.ws.es.activityinfo.ActivityInfoServiceSkeletonInterface;
import org.glite.ce.creamapi.ws.es.activityinfo.InternalBaseFault;
import org.glite.ce.creamapi.ws.es.activityinfo.InvalidParameterFault;
import org.glite.ce.creamapi.ws.es.activityinfo.UnknownAttributeFault;
import org.glite.ce.creamapi.ws.es.activityinfo.VectorLimitExceededFault;
import org.glite.ce.creamapi.ws.es.activityinfo.types.ActivityInfoDocument_t;
import org.glite.ce.creamapi.ws.es.activityinfo.types.ActivityInfoItemChoice_type1;
import org.glite.ce.creamapi.ws.es.activityinfo.types.ActivityInfoItem_type0;
import org.glite.ce.creamapi.ws.es.activityinfo.types.ActivityNotFoundFault_type0;
import org.glite.ce.creamapi.ws.es.activityinfo.types.ActivityStatusAttribute;
import org.glite.ce.creamapi.ws.es.activityinfo.types.ActivityStatusItemChoice_type1;
import org.glite.ce.creamapi.ws.es.activityinfo.types.ActivityStatusItem_type0;
import org.glite.ce.creamapi.ws.es.activityinfo.types.ActivityStatusState;
import org.glite.ce.creamapi.ws.es.activityinfo.types.ActivityStatus_type0;
import org.glite.ce.creamapi.ws.es.activityinfo.types.ActivityStatus_type1;
import org.glite.ce.creamapi.ws.es.activityinfo.types.ComputingActivityHistory_type0;
import org.glite.ce.creamapi.ws.es.activityinfo.types.GetActivityInfo;
import org.glite.ce.creamapi.ws.es.activityinfo.types.GetActivityInfoResponse;
import org.glite.ce.creamapi.ws.es.activityinfo.types.GetActivityStatus;
import org.glite.ce.creamapi.ws.es.activityinfo.types.GetActivityStatusResponse;
import org.glite.ce.creamapi.ws.es.activityinfo.types.InternalBaseFault_Type;
import org.glite.ce.creamapi.ws.es.activityinfo.types.ListActivities;
import org.glite.ce.creamapi.ws.es.activityinfo.types.ListActivitiesResponse;
import org.glite.ce.creamapi.ws.es.activityinfo.types.OperationNotAllowedFault_type0;
import org.glite.ce.creamapi.ws.es.activityinfo.types.Operation_type0;
import org.glite.ce.creamapi.ws.es.adl.ActivityTypeEnumeration;
import org.glite.ce.creamapi.ws.es.glue.ComputingActivityState_t;
import org.glite.ce.creamapi.ws.es.glue.ComputingActivityType_t;
import org.glite.ce.creamapi.ws.es.glue.JobDescription_t;

public class ActivityInfoService implements ActivityInfoServiceSkeletonInterface, Lifecycle {
    private static final Logger logger = Logger.getLogger(ActivityInfoService.class.getName());
    private static final int INITIALIZATION_TBD = 0;
    private static final int INITIALIZATION_OK = 1;
    private static final int INITIALIZATION_ERROR = 2;
    private static int initialization = INITIALIZATION_TBD;
    private static String activityManagerURL = null;

    private void checkInitialization() throws InternalBaseFault {
        if (initialization == INITIALIZATION_ERROR) {
            throw new InternalBaseFault("ActivityInfoService not available: configuration failed!");
        }
    }

    public void destroy(ServiceContext context) {
        logger.info("destroy invoked!");

        try {
            CommandManager.getInstance().terminate();
        } catch (Throwable t) {
            logger.error("cannot get instance of CommandManager: " + t.getMessage());
        }

        logger.info("destroyed!");
    }

    public void init(ServiceContext serviceContext) throws AxisFault {
        if (initialization == INITIALIZATION_ERROR) {
            throw new AxisFault("ActivityInfoService not available: configuration failed!");
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

        AxisService axisService = axisConfiguration.getService("ActivityInfoService");
        if (axisService == null) {
            initialization = INITIALIZATION_ERROR;
            logger.error("Configuration error: AxisService 'CreationService' not found!");
            return;
        }

        Parameter parameter = axisService.getParameter("serviceLogConfigurationFile");
        if (parameter != null) {
            LogManager.resetConfiguration();
            PropertyConfigurator.configure((String) parameter.getValue());
        }

        ServiceConfig serviceConfig = ServiceConfig.getConfiguration();
        if (serviceConfig == null) {
            initialization = INITIALIZATION_ERROR;
            logger.error("Configuration error: cannot initialize the ServiceConfig");
            return;
        }

        logger.info("starting the ActivityInfoService initialization...");

        CommandManagerInterface commandManager = null;
        try {
            commandManager = CommandManager.getInstance();
        } catch (Throwable t) {
            initialization = INITIALIZATION_ERROR;
            logger.error("Configuration error: cannot get instance of CommandManager: " + t.getMessage());
            return;
        }

        if (!commandManager.checkCommandExecutor("ActivityExecutor", ActivityCmd.ACTIVITY_MANAGEMENT)) {
            initialization = INITIALIZATION_ERROR;
            logger.error("Configuration error: ActivityExecutor not loaded!");
            return;
        }
        
        try {
            String hostName = InetAddress.getLocalHost().getCanonicalHostName();
            activityManagerURL = "https://" + hostName + ":8443/ce-cream-es/services/ActivityManagementService";
        } catch (Throwable t) {
            initialization = INITIALIZATION_ERROR;
            logger.error("Configuration error: cannot get the host name: " + t.getMessage());
            return;
        }
        
        initialization = INITIALIZATION_OK;
        logger.info("ActivityInfoService initialization done!");
        logger.info("ActivityInfoService started!");
    }

    public GetActivityInfoResponse getActivityInfo(GetActivityInfo req) throws AccessControlFault, InternalBaseFault, UnknownAttributeFault, VectorLimitExceededFault {
        logger.debug("BEGIN getActivityInfo");

        checkInitialization();

        if (req == null) {
            throw new InternalBaseFault("Empty request received!");
        }

        Activity activity = null;
        ActivityStatusAttribute[] activityStatusAttributeArray = null;
        ActivityStatus_type0 activityStatus = null;
        List<ActivityStatus_type0> activityStatusList = null;
        ComputingActivityState_t computingActivityState = null;
        ComputingActivityState_t[] computingActivityStateArray = null;
        List<ComputingActivityState_t> computingActivityStateList = null;
        ActivityInfoItem_type0[] activityInfoItemArray = null;
        ActivityInfoItemChoice_type1 choice = null;
        ActivityInfoDocument_t activityInfoDocument = null;
        Operation_type0[] operationArray = null;

        ActivityCmd command = new ActivityCmd(ActivityCommandName.GET_ACTIVITY_INFO);
        command.setUserId(CEUtils.getUserId());

        activityInfoItemArray = new ActivityInfoItem_type0[req.getActivityID().length];
        int x = 0;

        for (String activityId : req.getActivityID()) {
            command.addParameter(ActivityCommandField.ACTIVITY_ID, activityId);
            choice = new ActivityInfoItemChoice_type1();

            activityInfoItemArray[x] = new ActivityInfoItem_type0();
            activityInfoItemArray[x].setActivityID(activityId);
            activityInfoItemArray[x].setActivityInfoItemChoice_type1(choice);

            try {
                CommandManager.getInstance().execute(command);

                activity = (Activity) command.getResult().getParameter(ActivityCommandField.ACTIVITY_DESCRIPTION.name());

                JobDescription_t jobDescription = new JobDescription_t();
                jobDescription.setJobDescription_t("emi:adl");

                activityInfoDocument = new ActivityInfoDocument_t();
                activityInfoDocument.setBaseType("Activity");
                activityInfoDocument.setID(new URI(activityManagerURL + "?" + activity.getId()));
                activityInfoDocument.setIDFromEndpoint(new URI(activityManagerURL + "?" + activity.getId()));
                activityInfoDocument.setLocalIDFromManager(activity.getProperties().get(Activity.LRMS_ABS_LAYER_ID));
                activityInfoDocument.setLocalOwner(activity.getProperties().get(Activity.LOCAL_USER));
                activityInfoDocument.setOwner(CEUtils.getUserId());
                activityInfoDocument.setJobDescription(jobDescription);
                activityInfoDocument.setStageInDirectory(new URI[] { new URI(activity.getProperties().get(Activity.STAGE_IN_URI)) });
                activityInfoDocument.setStageOutDirectory(new URI[] { new URI(activity.getProperties().get(Activity.STAGE_OUT_URI)) });
                // computingActivity.setProxyExpirationTime(value);

                if (activity.getActivityIdentification() != null) {
                    activityInfoDocument.setName(activity.getActivityIdentification().getName());
                }

                if (activity.getResources() != null) {
                    activityInfoDocument.setQueue(activity.getResources().getQueueName());
                }

                if (activity.getApplication() != null) {
                    activityInfoDocument.setStdErr(activity.getApplication().getError());
                    activityInfoDocument.setStdIn(activity.getApplication().getInput());
                    activityInfoDocument.setStdIn(activity.getApplication().getOutput());
                }

                // computingActivity.setSubmissionClientName(value);
                // computingActivity.getSubmissionHost();
                activityInfoDocument.setUserDomain(activity.getProperties().get(Activity.LOCAL_USER_GROUP));

                if (activity.getProperties().containsKey(Activity.WORKER_NODE)) {
                    activityInfoDocument.setExecutionNode(new String[] { activity.getProperties().get(Activity.WORKER_NODE) });
                }

                ComputingActivityType_t type = ComputingActivityType_t.single;

                if (activity.getActivityIdentification().getType() != null) {
                    String activityType = activity.getActivityIdentification().getType().name();

                    if (ActivityTypeEnumeration.single.getValue().equals(activityType)) {
                        type = ComputingActivityType_t.single;
                    } else if (ActivityTypeEnumeration.collectionelement.getValue().equals(activityType)) {
                        type = ComputingActivityType_t.collectionelement;
                    } else if (ActivityTypeEnumeration.parallelelement.getValue().equals(activityType)) {
                        type = ComputingActivityType_t.parallelelement;
                    } else if (ActivityTypeEnumeration.workflownode.getValue().equals(activityType)) {
                        type = ComputingActivityType_t.workflownode;
                    }
                }

                activityInfoDocument.setType(type);

                if (activity.getProperties().containsKey(Activity.EXIT_CODE)) {
                    try {
                        activityInfoDocument.setExitCode(Integer.valueOf(activity.getProperties().get(Activity.EXIT_CODE)));
                    } catch (NumberFormatException e) {
                        logger.warn(e.getMessage());
                    }
                }

                int index = 0;

                ComputingActivityHistory_type0 history = new ComputingActivityHistory_type0();

                activityInfoDocument.setComputingActivityHistory(history);

                activityStatusList = new ArrayList<ActivityStatus_type0>(0);
                computingActivityStateList = new ArrayList<ComputingActivityState_t>(0);

                for (org.glite.ce.creamapi.activitymanagement.ActivityStatus status : activity.getStates()) {
//                    if (!status.isTransient()) {
                        activityStatus = new ActivityStatus_type0();
                        activityStatus.setStatus(ActivityStatusState.Factory.fromValue(status.getStatusName().getName()));
                        activityStatus.setDescription(status.getDescription());
                        activityStatus.setTimestamp(status.getTimestamp().toGregorianCalendar());

                        if (status.getStatusAttributes().size() > 0) {
                            activityStatusAttributeArray = new ActivityStatusAttribute[status.getStatusAttributes().size()];
                            int i = 0;

                            for (StatusAttributeName attribute : status.getStatusAttributes()) {
                                activityStatusAttributeArray[i++] = ActivityStatusAttribute.Factory.fromValue(attribute.getName());
                            }

                            activityStatus.setAttribute(activityStatusAttributeArray);
                        }

                        activityStatusList.add(activityStatus);

                        computingActivityState = new ComputingActivityState_t();
                        computingActivityState.setComputingActivityState_t(status.getStatusName().getName());

                        computingActivityStateList.add(computingActivityState);

                        if (status.getTimestamp() != null) {
                            Calendar time = status.getTimestamp().toGregorianCalendar();

                            if (ActivityStatusState.value4.getValue().equals(activityStatus.getStatus().getValue())) {
                                activityInfoDocument.setComputingManagerSubmissionTime(time);
                            } else if (ActivityStatusState.value6.getValue().equals(activityStatus.getStatus().getValue())) {
                                activityInfoDocument.setStartTime(time);
                            } else if (ActivityStatusState.value8.getValue().equals(activityStatus.getStatus().getValue())) {
                                activityInfoDocument.setComputingManagerEndTime(time);
                                activityInfoDocument.setEndTime(time);
                            }
                        }
                    }
  //              }

                if (activityStatusList.size() > 0) {
                    ActivityStatus_type0[] activityStatusArray = new ActivityStatus_type0[activityStatusList.size()];
                    history.setActivityStatus(activityStatusList.toArray(activityStatusArray));

                    computingActivityStateArray = new ComputingActivityState_t[computingActivityStateList.size()];
                    activityInfoDocument.setState(computingActivityStateList.toArray(computingActivityStateArray));
                }

                index = 0;
                operationArray = new Operation_type0[activity.getCommands().size()];
                history.setOperation(operationArray);

                for (ActivityCommand activityCommand : activity.getCommands()) {
                    operationArray[index] = new Operation_type0();
                    operationArray[index].setRequestedOperation(new NCName(activityCommand.getName()));
                    operationArray[index].setSuccess(activityCommand.isSuccess());

                    if (activityCommand.getTimestamp() != null) {
                        Calendar time = activityCommand.getTimestamp().toGregorianCalendar();

                        if (ActivityCommandName.CREATE_ACTIVITY.name().equalsIgnoreCase(activityCommand.getName())) {
                            activityInfoDocument.setCreationTime(time);
                            activityInfoDocument.setSubmissionTime(time);
                        }

                        operationArray[index].setTimestamp(activityCommand.getTimestamp().toGregorianCalendar());
                    }

                    index++;
                }

                choice.setActivityInfoDocument(activityInfoDocument);
            } catch (Throwable t) {
                String message = t.getMessage();
                
                if (message == null) {
                    InternalBaseFault_Type fault = new InternalBaseFault_Type();
                    fault.setTimestamp(GregorianCalendar.getInstance());
                    fault.setMessage("N/A");
                    
                    choice.setInternalBaseFault(fault);
                } else if (message.indexOf("not found") != -1) {
                    ActivityNotFoundFault_type0 fault = new ActivityNotFoundFault_type0();
                    fault.setTimestamp(GregorianCalendar.getInstance());
                    fault.setMessage(message);
                    
                    choice.setActivityNotFoundFault(fault);
                } else if (message.indexOf("invalid state") != -1) {
                    OperationNotAllowedFault_type0 fault = new OperationNotAllowedFault_type0();
                    fault.setTimestamp(GregorianCalendar.getInstance());
                    fault.setMessage(message);
                    
                    choice.setOperationNotAllowedFault(fault);
                } else {
                    InternalBaseFault_Type fault = new InternalBaseFault_Type();
                    fault.setTimestamp(GregorianCalendar.getInstance());
                    fault.setMessage(message);
                    
                    choice.setInternalBaseFault(fault);
                }
            }

            x++;
        }

        GetActivityInfoResponse response = new GetActivityInfoResponse();
        response.setActivityInfoItem(activityInfoItemArray);

        logger.debug("END getActivityInfo");
        return response;
    }

    public GetActivityStatusResponse getActivityStatus(GetActivityStatus req) throws AccessControlFault, InternalBaseFault, VectorLimitExceededFault {
        logger.debug("BEGIN getActivityStatus");

        checkInitialization();

        if (req == null || req.getActivityID() == null) {
            throw new InternalBaseFault("ActivityId not specified!");
        }
        
        int index = 0;
        ActivityStatus_type0 status = null;
        ActivityStatusAttribute[] activityStatusAttributeArray = null;
        ActivityStatusItemChoice_type1 choice = null;
        ActivityStatusItem_type0[] activityStatusItem = new ActivityStatusItem_type0[req.getActivityID().length];

        ActivityCmd command = new ActivityCmd(ActivityCommandName.GET_ACTIVITY_STATUS);
        command.setUserId(CEUtils.getUserId());

        for (String activityId : req.getActivityID()) {
            command.addParameter(ActivityCommandField.ACTIVITY_ID, activityId);
            choice = new ActivityStatusItemChoice_type1();
            
            activityStatusItem[index] = new ActivityStatusItem_type0();
            activityStatusItem[index].setActivityID(activityId);
            activityStatusItem[index].setActivityStatusItemChoice_type1(choice);

            try {
                CommandManager.getInstance().execute(command);
                ActivityStatus activityStatus = (ActivityStatus)command.getResult().getParameter(ActivityCommandField.ACTIVITY_STATUS.name());
                
                status = new ActivityStatus_type0();
                status.setStatus(ActivityStatusState.Factory.fromValue(activityStatus.getStatusName().getName()));
                status.setTimestamp(activityStatus.getTimestamp().toGregorianCalendar());
                status.setDescription(activityStatus.getDescription());

                if ((activityStatus.getStatusAttributes() != null) && (activityStatus.getStatusAttributes().size() > 0)) {
                    activityStatusAttributeArray = new ActivityStatusAttribute[activityStatus.getStatusAttributes().size()];
                    int i = 0;
                    
                    for (StatusAttributeName attribute : activityStatus.getStatusAttributes()) {
                        activityStatusAttributeArray[i++] = ActivityStatusAttribute.Factory.fromValue(attribute.getName());
                    }

                    status.setAttribute(activityStatusAttributeArray);   
                }

                choice.setActivityStatus(status);
            } catch (Throwable t) {
                String message = t.getMessage();
                
                if (message == null) {
                    InternalBaseFault_Type fault = new InternalBaseFault_Type();
                    fault.setTimestamp(GregorianCalendar.getInstance());
                    fault.setMessage("N/A");
                    
                    choice.setInternalBaseFault(fault);
                } else if (message.indexOf("not found") != -1) {
                    ActivityNotFoundFault_type0 fault = new ActivityNotFoundFault_type0();
                    fault.setTimestamp(GregorianCalendar.getInstance());
                    fault.setMessage(message);
                    
                    choice.setActivityNotFoundFault(fault);
                } else if (message.indexOf("invalid state") != -1) {
                    OperationNotAllowedFault_type0 fault = new OperationNotAllowedFault_type0();
                    fault.setTimestamp(GregorianCalendar.getInstance());
                    fault.setMessage(message);
                    
                    choice.setOperationNotAllowedFault(fault);
                } else {
                    InternalBaseFault_Type fault = new InternalBaseFault_Type();
                    fault.setTimestamp(GregorianCalendar.getInstance());
                    fault.setMessage(message);
                    
                    choice.setInternalBaseFault(fault);
                }
            }

            index++;
        }

        GetActivityStatusResponse response = new GetActivityStatusResponse();
        response.setActivityStatusItem(activityStatusItem);
        
        logger.debug("END getActivityStatus");
        return response;
    }
 
    public ListActivitiesResponse listActivities(ListActivities req) throws AccessControlFault, InternalBaseFault, InvalidParameterFault{
        logger.debug("BEGIN listActivities");

        checkInitialization();

        if (req == null) {
            throw new InternalBaseFault("request not specified!");
        }

        ActivityCmd command = new ActivityCmd(ActivityCommandName.LIST_ACTIVITIES);
        command.setUserId(CEUtils.getUserId());
        command.addParameter(ActivityCommandField.FROM_DATE, req.getFromDate());
        command.addParameter(ActivityCommandField.TO_DATE, req.getToDate());
        command.addParameter(ActivityCommandField.LIMIT, req.getLimit());

        if (req.isActivityStatusSpecified()) {
            ActivityStatus activityStatus = null;
            List<ActivityStatus> activityStatusList = new ArrayList<ActivityStatus>(req.getActivityStatus().length);

            for (ActivityStatus_type1 status : req.getActivityStatus()) {
                activityStatus = new ActivityStatus(StatusName.fromValue(status.getStatus().getValue()));

                if (status.isAttributeSpecified()) {
                    for (ActivityStatusAttribute statusAttribute : status.getAttribute()) {
                        activityStatus.getStatusAttributes().add(StatusAttributeName.fromValue(statusAttribute.getValue()));
                    }
                }
                
                activityStatusList.add(activityStatus);
            }

            command.addParameter(ActivityCommandField.ACTIVITY_STATUS_LIST, activityStatusList);
        }

        ListActivitiesResponse response = new ListActivitiesResponse();
        
        try {
            CommandManager.getInstance().execute(command);
            List<String> activityIdList = (List<String>) command.getResult().getParameter(ActivityCommandField.ACTIVITY_ID_LIST.name());
            
            if (activityIdList != null && activityIdList.size() > 0) {
                String[] activityIds = new String[activityIdList.size()];
                
                response.setActivityID(activityIdList.toArray(activityIds));
            }
            response.setTruncated((Boolean) command.getResult().getParameter(ActivityCommandField.IS_TRUNCATED.name()));
        } catch (Throwable t) {
            InternalBaseFault_Type faultType = new InternalBaseFault_Type();
            faultType.setTimestamp(GregorianCalendar.getInstance());
            faultType.setMessage(t.getMessage());
            
            org.glite.ce.creamapi.ws.es.activityinfo.types.InternalBaseFault msg = new org.glite.ce.creamapi.ws.es.activityinfo.types.InternalBaseFault();
            msg.setInternalBaseFault(faultType);
            
            InternalBaseFault fault = new InternalBaseFault();
            fault.setFaultMessage(msg);

            throw fault;
        }

        logger.debug("END listActivities");
        return response;
    }
}
