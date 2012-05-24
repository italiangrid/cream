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
import java.util.GregorianCalendar;
import java.util.List;

import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axis2.AxisFault;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.context.ServiceContext;
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
import org.glite.ce.creamapi.activitymanagement.wrapper.glue.DateTime_t;
import org.glite.ce.creamapi.cmdmanagement.CommandManagerInterface;
import org.glite.ce.creamapi.ws.es.activityinfo.AccessControlFault;
import org.glite.ce.creamapi.ws.es.activityinfo.ActivityInfoServiceSkeletonInterface;
import org.glite.ce.creamapi.ws.es.activityinfo.InternalBaseFault;
import org.glite.ce.creamapi.ws.es.activityinfo.UnknownGlue2ActivityAttributeFault;
import org.glite.ce.creamapi.ws.es.activityinfo.VectorLimitExceededFault;
import org.glite.ce.creamapi.ws.es.activityinfo.types.ActivityInfoItemChoice_type1;
import org.glite.ce.creamapi.ws.es.activityinfo.types.ActivityInfoItem_type0;
import org.glite.ce.creamapi.ws.es.activityinfo.types.ActivityStatusAttribute;
import org.glite.ce.creamapi.ws.es.activityinfo.types.ActivityStatusItemChoice_type1;
import org.glite.ce.creamapi.ws.es.activityinfo.types.ActivityStatusItem_type0;
import org.glite.ce.creamapi.ws.es.activityinfo.types.ActivityStatus_type0;
import org.glite.ce.creamapi.ws.es.activityinfo.types.ComputingActivityHistory;
import org.glite.ce.creamapi.ws.es.activityinfo.types.GetActivityInfo;
import org.glite.ce.creamapi.ws.es.activityinfo.types.GetActivityInfoResponse;
import org.glite.ce.creamapi.ws.es.activityinfo.types.GetActivityStatus;
import org.glite.ce.creamapi.ws.es.activityinfo.types.GetActivityStatusResponse;
import org.glite.ce.creamapi.ws.es.activityinfo.types.InternalBaseFault_Type;
import org.glite.ce.creamapi.ws.es.activityinfo.types.InvalidActivityStateFault;
import org.glite.ce.creamapi.ws.es.activityinfo.types.ListActivities;
import org.glite.ce.creamapi.ws.es.activityinfo.types.ListActivitiesResponse;
import org.glite.ce.creamapi.ws.es.activityinfo.types.OperationalResult_type0;
import org.glite.ce.creamapi.ws.es.activityinfo.types.PrimaryActivityStatus;
import org.glite.ce.creamapi.ws.es.activityinfo.types.UnknownActivityIDFault;
import org.glite.ce.creamapi.ws.es.adl.ActivityTypeEnumeration;
import org.glite.ce.creamapi.ws.es.glue.ComputingActivityState_t;
import org.glite.ce.creamapi.ws.es.glue.ComputingActivityType_t;
import org.glite.ce.creamapi.ws.es.glue.ComputingActivity_t;
import org.glite.ce.creamapi.ws.es.glue.Extension_t;
import org.glite.ce.creamapi.ws.es.glue.Extensions_t;
import org.glite.ce.creamapi.ws.es.glue.JobDescription_t;
import org.glite.ce.creamapi.ws.es.glue.LocalID_t;

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

    public GetActivityInfoResponse getActivityInfo(GetActivityInfo req) throws VectorLimitExceededFault, InternalBaseFault, AccessControlFault, UnknownGlue2ActivityAttributeFault {
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
        ActivityInfoItemChoice_type1 activityInfoItemChoice = null;
        ComputingActivity_t computingActivity = null;
        ComputingActivityHistory activityHistory = null;
        OperationalResult_type0[] operationalResultArray = null;
        Extension_t activityHistoryExtension = null;
        Extension_t stageInURLExtension = null;
        Extension_t stageOutURLExtension = null;
        Extensions_t extensions = null;

        ActivityCmd command = new ActivityCmd(ActivityCommandName.GET_ACTIVITY_INFO);
        command.setUserId(CEUtils.getUserId());
        
        activityInfoItemArray = new ActivityInfoItem_type0[req.getActivityID().length];
        int x = 0;

        for (String activityId : req.getActivityID()) {
            command.addParameter(ActivityCommandField.ACTIVITY_ID, activityId);
            activityInfoItemChoice = new ActivityInfoItemChoice_type1();
            
            activityInfoItemArray[x] = new ActivityInfoItem_type0();
            activityInfoItemArray[x].setActivityID(activityId);
            activityInfoItemArray[x].setActivityInfoItemChoice_type1(activityInfoItemChoice);

            try {
                CommandManager.getInstance().execute(command);

                activity = (Activity)command.getResult().getParameter(ActivityCommandField.ACTIVITY_DESCRIPTION.name());

                JobDescription_t jobDescription = new JobDescription_t();
                jobDescription.setJobDescription_t("emi:adl");
                
                computingActivity = new ComputingActivity_t();
                computingActivity.setID(new URI(activityManagerURL + "?" + activity.getId()));
                computingActivity.setIDFromEndpoint(new URI(activityManagerURL + "?" + activity.getId()));
                computingActivity.setLocalIDFromManager(activity.getProperties().get(Activity.LRMS_ABS_LAYER_ID));
                computingActivity.setLocalOwner(activity.getProperties().get(Activity.LOCAL_USER));
                computingActivity.setOwner(CEUtils.getUserId());
                computingActivity.setJobDescription(jobDescription);
                //computingActivity.setProxyExpirationTime(value);

                if (activity.getActivityIdentification() != null) { 
                    computingActivity.setName(activity.getActivityIdentification().getName());
                }

                if (activity.getResources() != null) {
                    computingActivity.setQueue(activity.getResources().getQueueName());
                }

                if (activity.getApplication() != null) {
                    computingActivity.setStdErr(activity.getApplication().getError());
                    computingActivity.setStdIn(activity.getApplication().getInput());
                    computingActivity.setStdIn(activity.getApplication().getOutput());
                }

                //computingActivity.setSubmissionClientName(value);
                //computingActivity.getSubmissionHost();
                computingActivity.setUserDomain(activity.getProperties().get(Activity.LOCAL_USER_GROUP));
                
                if (activity.getProperties().containsKey(Activity.WORKER_NODE)) {
                    computingActivity.setExecutionNode(new String[] { activity.getProperties().get(Activity.WORKER_NODE) });
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
                
                computingActivity.setType(type);
                
                if (activity.getProperties().containsKey(Activity.EXIT_CODE)) {
                    try {
                        computingActivity.setExitCode(Integer.valueOf(activity.getProperties().get(Activity.EXIT_CODE)));
                    } catch (NumberFormatException e) {
                        logger.warn(e.getMessage());
                    }
                }

                int index = 0;

                activityHistory = new ComputingActivityHistory();

                activityStatusList = new ArrayList<ActivityStatus_type0>(0);
                computingActivityStateList = new ArrayList<ComputingActivityState_t>(0);
                
                for (org.glite.ce.creamapi.activitymanagement.ActivityStatus status : activity.getStates()) {
                    if (!status.isTransient()) {
                        activityStatus = new ActivityStatus_type0();
                        activityStatus.setStatus(PrimaryActivityStatus.Factory.fromValue(status.getStatusName().getName()));
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
                            DateTime_t time = new DateTime_t(status.getTimestamp().toGregorianCalendar());

                            if (PrimaryActivityStatus.value4.getValue().equals(activityStatus.getStatus().getValue())) {
                                computingActivity.setComputingManagerSubmissionTime(time);
                            } else if (PrimaryActivityStatus.value6.getValue().equals(activityStatus.getStatus().getValue())) {
                                computingActivity.setStartTime(time);
                            } else if (PrimaryActivityStatus.value8.getValue().equals(activityStatus.getStatus().getValue())) {
                                computingActivity.setComputingManagerEndTime(time);
                                computingActivity.setEndTime(time);
                            }
                        }
                    }
                }

                if (activityStatusList.size() > 0) {
                    ActivityStatus_type0[] activityStatusArray = new ActivityStatus_type0[activityStatusList.size()];
                    activityHistory.setActivityStatus(activityStatusList.toArray(activityStatusArray));
                    
                    computingActivityStateArray = new ComputingActivityState_t[computingActivityStateList.size()];
                    computingActivity.setState(computingActivityStateList.toArray(computingActivityStateArray));
                }
                
                index = 0;
                operationalResultArray = new OperationalResult_type0[activity.getCommands().size()];
                activityHistory.setOperationalResult(operationalResultArray);

                for (ActivityCommand activityCommand : activity.getCommands()) {
                    operationalResultArray[index] = new OperationalResult_type0();
                    operationalResultArray[index].setName(activityCommand.getName());
                    operationalResultArray[index].setSuccess(activityCommand.isSuccess());

                    if (activityCommand.getTimestamp() != null) {
                        DateTime_t time = new DateTime_t(activityCommand.getTimestamp().toGregorianCalendar());

                        if (ActivityCommandName.CREATE_ACTIVITY.name().equalsIgnoreCase(activityCommand.getName())) {
                            computingActivity.setCreationTime(time);
                            computingActivity.setSubmissionTime(time);
                        }

                        operationalResultArray[index].setTimestamp(activityCommand.getTimestamp().toGregorianCalendar());
                    }

                    index++;
                }
                
                LocalID_t localId = new LocalID_t();
                localId.setLocalID_t(activity.getId());
                
                activityHistoryExtension = new Extension_t();
                activityHistoryExtension.setLocalID(localId);
                activityHistoryExtension.setKey("ACTIVITY_HISTORY");
                activityHistoryExtension.setValue("ACTIVITY_HISTORY");
                activityHistoryExtension.setExtraElement(activityHistory.getOMElement(ComputingActivityHistory.MY_QNAME, OMAbstractFactory.getOMFactory()));

                stageInURLExtension = new Extension_t();
                stageInURLExtension.setLocalID(localId);
                stageInURLExtension.setKey("STAGE_IN_URI");
                stageInURLExtension.setValue(activity.getProperties().get(Activity.STAGE_IN_URI));
                
                stageOutURLExtension = new Extension_t();
                stageOutURLExtension.setLocalID(localId);
                stageOutURLExtension.setKey("STAGE_OUT_URI");
                stageOutURLExtension.setValue(activity.getProperties().get(Activity.STAGE_OUT_URI));
                
                extensions = new Extensions_t();
                extensions.addExtension(activityHistoryExtension);
                extensions.addExtension(stageInURLExtension);
                extensions.addExtension(stageOutURLExtension);

                computingActivity.setExtensions(extensions);

                activityInfoItemChoice.setActivityInfo(computingActivity);
            } catch (Throwable t) {
                activityInfoItemChoice.setInternalBaseFault(makeInternalBaseFaultType(t.getMessage()));
            }

            x++;
        }

        GetActivityInfoResponse response = new GetActivityInfoResponse();
        response.setActivityInfoItem(activityInfoItemArray);

        logger.debug("END getActivityInfo");
        return response;
    }


    public GetActivityStatusResponse getActivityStatus(GetActivityStatus req) throws VectorLimitExceededFault, InternalBaseFault, AccessControlFault {
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
                status.setStatus(PrimaryActivityStatus.Factory.fromValue(activityStatus.getStatusName().getName()));
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
                logger.error(t.getMessage(), t);
                choice.setInternalBaseFault(makeInternalBaseFaultType(t.getMessage()));
            }

            index++;
        }

        GetActivityStatusResponse response = new GetActivityStatusResponse();
        response.setActivityStatusItem(activityStatusItem);
        
        logger.debug("END getActivityStatus");
        return response;
    }
 
    public ListActivitiesResponse listActivities(ListActivities req) throws InternalBaseFault, AccessControlFault {
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

        if (req.getStatus() != null) {
            List<StatusName> statusList = new ArrayList<StatusName>(req.getStatus().length);

            for (PrimaryActivityStatus status : req.getStatus()) {
                statusList.add(StatusName.fromValue(status.getValue()));
            }

            command.addParameter(ActivityCommandField.ACTIVITY_STATUS_LIST, statusList);
        }

//        if (req.getStatusAttribute() != null) {
//            List<StatusAttributeName> statusAttributeList = new ArrayList<StatusAttributeName>(req.getStatusAttribute().length);
//
//            for (ActivityStatusAttribute attribute : req.getStatusAttribute()) {
//                statusAttributeList.add(StatusAttributeName.fromValue(attribute.getValue()));
//            }
//
//            command.addParameter(ActivityCommandField.ACTIVITY_STATUS_ATTRIBUTE_LIST, statusAttributeList);
//        }

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
            org.glite.ce.creamapi.ws.es.activityinfo.types.InternalBaseFault msg = new org.glite.ce.creamapi.ws.es.activityinfo.types.InternalBaseFault();
            msg.setInternalBaseFault(makeInternalBaseFaultType(t.getMessage()));
            
            InternalBaseFault fault = new InternalBaseFault();
            fault.setFaultMessage(msg);

            throw fault;
        }

        logger.debug("END listActivities");
        return response;
    }

    private InternalBaseFault_Type makeInternalBaseFaultType(String message) {
        InternalBaseFault_Type fault = null;

        if (message == null) {
            fault = new InternalBaseFault_Type();
            message = "N/A";
        } else if (message.indexOf("not found") != 0) {
            fault = new UnknownActivityIDFault();
        } else if (message.indexOf("invalid state") != 0) {
            fault = new InvalidActivityStateFault();
        } else {
            fault = new InternalBaseFault_Type();
            message = "N/A";
        }

        fault.setMessage(message);
        fault.setTimestamp(new GregorianCalendar());

        return fault;
    }
}
