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
import java.io.ByteArrayInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.net.InetAddress;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.impl.builder.StAXBuilder;
import org.apache.axiom.om.impl.builder.StAXOMBuilder;
import org.apache.axiom.om.impl.llom.OMElementImpl;
import org.apache.axiom.om.xpath.AXIOMXPath;
import org.apache.axis2.AxisFault;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.context.ServiceContext;
import org.apache.axis2.databinding.ADBException;
import org.apache.axis2.databinding.types.NCName;
import org.apache.axis2.databinding.types.URI;
import org.apache.axis2.description.AxisService;
import org.apache.axis2.description.Parameter;
import org.apache.axis2.engine.AxisConfiguration;
import org.apache.axis2.service.Lifecycle;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.xmlbeans.impl.values.NamespaceContext;
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
import org.glite.ce.creamapi.cmdmanagement.CommandManagerInterface;
import org.glite.ce.creamapi.ws.es.activitymanagement.AccessControlFault;
import org.glite.ce.creamapi.ws.es.activitymanagement.ActivityManagementServiceSkeletonInterface;
import org.glite.ce.creamapi.ws.es.activitymanagement.InternalBaseFault;
import org.glite.ce.creamapi.ws.es.activitymanagement.InternalNotificationFault;
import org.glite.ce.creamapi.ws.es.activitymanagement.OperationNotPossibleFault;
import org.glite.ce.creamapi.ws.es.activitymanagement.UnknownAttributeFault;
import org.glite.ce.creamapi.ws.es.activitymanagement.VectorLimitExceededFault;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.ActivityInfoDocument;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.ActivityInfoDocument_t;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.ActivityInfoItemChoice_type1;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.ActivityInfoItem_type0;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.ActivityNotFoundFault_type0;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.ActivityStatusAttribute;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.ActivityStatusItemChoice_type1;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.ActivityStatusItem_type0;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.ActivityStatusState;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.ActivityStatus_type0;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.AttributeInfoItem;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.AttributeInfoItem_type0;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.CancelActivity;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.CancelActivityResponse;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.CancelActivityResponseItemChoice_type1;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.CancelActivityResponseItem_type0;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.ComputingActivityHistory_type0;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.GetActivityInfo;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.GetActivityInfoResponse;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.GetActivityStatus;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.GetActivityStatusResponse;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.InternalBaseFault_Type;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.NotifyRequestItem_type0;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.NotifyResponseItemChoice_type0;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.NotifyResponseItem_type0;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.NotifyService;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.NotifyServiceResponse;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.OperationNotAllowedFault_type0;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.Operation_type0;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.PauseActivity;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.PauseActivityResponse;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.PauseActivityResponseItemChoice_type1;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.PauseActivityResponseItem_type0;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.RestartActivity;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.RestartActivityResponse;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.ResumeActivity;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.ResumeActivityResponse;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.ResumeActivityResponseItemChoice_type1;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.ResumeActivityResponseItem_type0;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.UnknownAttributeFault_type0;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.WipeActivity;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.WipeActivityResponse;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.WipeActivityResponseItemChoice_type1;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.WipeActivityResponseItem_type0;
import org.glite.ce.creamapi.ws.es.adl.ActivityTypeEnumeration;
import org.glite.ce.creamapi.ws.es.glue.ComputingActivityState_t;
import org.glite.ce.creamapi.ws.es.glue.ComputingActivityType_t;
import org.glite.ce.creamapi.ws.es.glue.ComputingService;
import org.glite.ce.creamapi.ws.es.glue.JobDescription_t;

public class ActivityManagementService implements ActivityManagementServiceSkeletonInterface, Lifecycle {
    private static final Logger logger = Logger.getLogger(ActivityManagementService.class.getName());
    private static final int INITIALIZATION_TBD = 0;
    private static final int INITIALIZATION_OK = 1;
    private static final int INITIALIZATION_ERROR = 2;
    private static int initialization = INITIALIZATION_TBD;
    private static String prefexIdFromEndpoint = null;

    public CancelActivityResponse cancelActivity(CancelActivity req) throws AccessControlFault, InternalBaseFault, VectorLimitExceededFault {
        logger.debug("BEGIN cancelActivity");

        checkInitialization();

        if (req == null || req.getActivityID() == null || req.getActivityID().length == 0) {
            throw new InternalBaseFault("none activityId found in the request");
        }

        String[] activityIds = req.getActivityID();

        CancelActivityResponseItem_type0[] responseItems = new CancelActivityResponseItem_type0[activityIds.length];
        CancelActivityResponseItemChoice_type1 responseItemChoice = null;
        ActivityCmd command = new ActivityCmd(ActivityCommandName.CANCEL_ACTIVITY);
        command.setUserId(CEUtils.getUserId());

        for (int i = 0; i < activityIds.length; i++) {
            command.setAsynchronous(false);
            command.setCommandGroupId(activityIds[i]);
            command.addParameter(ActivityCommandField.ACTIVITY_ID, activityIds[i]);
            command.addParameter(ActivityCommandField.ACTIVITY_CHECK, ActivityCommandField.ACTIVITY_CHECK_ON);
            responseItemChoice = new CancelActivityResponseItemChoice_type1();

            try {
                CommandManager.getInstance().execute(command);

                command.getParameterKeySet().remove(ActivityCommandField.ACTIVITY_CHECK.name());
                command.setAsynchronous(true);

                CommandManager.getInstance().execute(command);
            } catch (Throwable t) {
                String message = t.getMessage();
                
                if (message == null) {
                    InternalBaseFault_Type fault = new InternalBaseFault_Type();
                    fault.setTimestamp(GregorianCalendar.getInstance());
                    fault.setMessage("N/A");
                    
                    responseItemChoice.setInternalBaseFault(fault);
                } else if (message.indexOf("not found") != -1) {
                    ActivityNotFoundFault_type0 fault = new ActivityNotFoundFault_type0();
                    fault.setTimestamp(GregorianCalendar.getInstance());
                    fault.setMessage(message);
                    
                    responseItemChoice.setActivityNotFoundFault(fault);
                } else if (message.indexOf("invalid state") != -1) {
                    OperationNotAllowedFault_type0 fault = new OperationNotAllowedFault_type0();
                    fault.setTimestamp(GregorianCalendar.getInstance());
                    fault.setMessage(message);
                    
                    responseItemChoice.setOperationNotAllowedFault(fault);
                } else {
                    InternalBaseFault_Type fault = new InternalBaseFault_Type();
                    fault.setTimestamp(GregorianCalendar.getInstance());
                    fault.setMessage(message);
                    
                    responseItemChoice.setInternalBaseFault(fault);
                }
            }

            responseItems[i] = new CancelActivityResponseItem_type0();
            responseItems[i].setActivityID(activityIds[i]);
            responseItems[i].setCancelActivityResponseItemChoice_type1(responseItemChoice);
        }

        CancelActivityResponse response = new CancelActivityResponse();
        response.setCancelActivityResponseItem(responseItems);

        logger.debug("END cancelActivity");
        return response;
    }

    private void checkInitialization() throws InternalBaseFault {
        if (initialization == INITIALIZATION_ERROR) {
            throw new InternalBaseFault("ActivityManagementService not available: configuration failed!");
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

    public GetActivityInfoResponse getActivityInfo(GetActivityInfo req) throws AccessControlFault, InternalBaseFault, UnknownAttributeFault, VectorLimitExceededFault {
        logger.debug("BEGIN getActivityInfo");

        checkInitialization();

        if (req == null) {
            throw new InternalBaseFault("Empty request received!");
        }

        Activity activity = null;
        ActivityInfoItemChoice_type1 choice = null;
        ActivityInfoItem_type0 activityInfoItem = null;
        ActivityInfoDocument_t activityInfoDocument = null;

        ActivityCmd command = new ActivityCmd(ActivityCommandName.GET_ACTIVITY_INFO);
        command.setUserId(CEUtils.getUserId());

        GetActivityInfoResponse response = new GetActivityInfoResponse();

        for (String activityId : req.getActivityID()) {
            command.addParameter(ActivityCommandField.ACTIVITY_ID, activityId);
            choice = new ActivityInfoItemChoice_type1();

            activityInfoItem = new ActivityInfoItem_type0();
            activityInfoItem.setActivityID(activityId);
            activityInfoItem.setActivityInfoItemChoice_type1(choice);

            response.addActivityInfoItem(activityInfoItem);
            
            try {
                CommandManager.getInstance().execute(command);

                activity = (Activity) command.getResult().getParameter(ActivityCommandField.ACTIVITY_DESCRIPTION.name());

                JobDescription_t jobDescription = new JobDescription_t();
                jobDescription.setJobDescription_t("emi:adl");

                activityInfoDocument = new ActivityInfoDocument_t();
                activityInfoDocument.setCreationTime(GregorianCalendar.getInstance());
                activityInfoDocument.setID(new URI(activity.getId()));
                activityInfoDocument.setIDFromEndpoint(new URI(prefexIdFromEndpoint + activity.getId()));
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

                ComputingActivityHistory_type0 history = new ComputingActivityHistory_type0();

                activityInfoDocument.setComputingActivityHistory(history);
                
                ActivityStatus_type0 activityStatus = null;
                                
                for (org.glite.ce.creamapi.activitymanagement.ActivityStatus status : activity.getStates()) {                 
                    if (status.getStatusName() != activity.getStates().last().getStatusName() || activity.getStates().last().equals(status)) {
                        activityStatus = new ActivityStatus_type0();
                        activityStatus.setStatus(ActivityStatusState.Factory.fromValue(status.getStatusName().getName()));
                        activityStatus.setDescription(status.getDescription());
                        activityStatus.setTimestamp(status.getTimestamp().toGregorianCalendar());
                        
                        for (StatusAttributeName attribute : status.getStatusAttributes()) {
                            activityStatus.addAttribute(ActivityStatusAttribute.Factory.fromValue(attribute.getName()));
                        }
                                                
                        history.addActivityStatus(activityStatus);

                        if (status.getTimestamp() != null) {
                            if (ActivityStatusState.value4.getValue().equals(activityStatus.getStatus().getValue())) {
                                activityInfoDocument.setComputingManagerSubmissionTime(status.getTimestamp().toGregorianCalendar());
                            } else if (ActivityStatusState.value6.getValue().equals(activityStatus.getStatus().getValue())) {
                                activityInfoDocument.setStartTime(status.getTimestamp().toGregorianCalendar());
                            } else if (ActivityStatusState.value8.getValue().equals(activityStatus.getStatus().getValue())) {
                                activityInfoDocument.setComputingManagerEndTime(status.getTimestamp().toGregorianCalendar());
                                activityInfoDocument.setEndTime(activityInfoDocument.getComputingManagerEndTime());
                            }
                        }
                    }
                }

                if (activity.getStates().size() > 0) {
                    ActivityStatus lastStatus = activity.getStates().last();
                    
                    ComputingActivityState_t computingActivityState = new ComputingActivityState_t();                    
                    computingActivityState.setComputingActivityState_t("emies:" + lastStatus.getStatusName().getName());

                    activityInfoDocument.addState(computingActivityState);
                   
                    for (StatusAttributeName statusAttribute : lastStatus.getStatusAttributes()) {
                        computingActivityState = new ComputingActivityState_t();                    
                        computingActivityState.setComputingActivityState_t("emiesattr:" + statusAttribute.getName());

                        activityInfoDocument.addState(computingActivityState);
                    }                    
                }
          
                Operation_type0 operation = null;

                for (ActivityCommand activityCommand : activity.getCommands()) {
                    operation = new Operation_type0();
                    operation.setRequestedOperation(new NCName(activityCommand.getName().toLowerCase()));
                    operation.setSuccess(activityCommand.isSuccess());

                    if (activityCommand.getTimestamp() != null) {
                        if (ActivityCommandName.CREATE_ACTIVITY.name().equalsIgnoreCase(activityCommand.getName())) {
                            activityInfoDocument.setSubmissionTime(activityCommand.getTimestamp().toGregorianCalendar());
                        }

                        operation.setTimestamp(activityCommand.getTimestamp().toGregorianCalendar());
                    }

                    history.addOperation(operation);
                }
                
                if (req.isAttributeNameSpecified() && req.getAttributeName().length > 0) {
                    String xmlStream = activityInfoDocument.getOMElement(ActivityInfoDocument.MY_QNAME, OMAbstractFactory.getOMFactory()).toString();

                    StAXBuilder builder = new StAXOMBuilder(new ByteArrayInputStream(xmlStream.getBytes()));
                    OMElement activityInfoDocumentElement = builder.getDocumentElement();

                        for (int i = 0; i < req.getAttributeName().length; i++) {
                            Iterator<OMElementImpl> children = (Iterator<OMElementImpl>) activityInfoDocumentElement.getChildrenWithName(req.getAttributeName()[i]);

                            if (children != null) {
                                OMElementImpl elem = null;
                                AttributeInfoItem_type0 attributeInfoItem = null;

                                if (!children.hasNext()) {
                                    throw new Exception("attribute " + req.getAttributeName()[i].toString() + " unknown");
                                }

                                while (children.hasNext()) {
                                    elem = children.next();
                                    attributeInfoItem = new AttributeInfoItem_type0();
                                    attributeInfoItem.setAttributeName(new QName(AttributeInfoItem.MY_QNAME.getNamespaceURI(), elem.getLocalName()));

                                    if (elem.getText() != null && elem.getText().length() > 0) {
                                        attributeInfoItem.setAttributeValue(elem.getText());
                                    } else {
                                        StringWriter stringWriter = new StringWriter();
                                        elem.serializeAndConsume(stringWriter);
                                        
                                        attributeInfoItem.setAttributeValue(stringWriter.toString());
                                    }

                                    choice.addAttributeInfoItem(attributeInfoItem);
                                }
                            } else {
                                throw new Exception("attribute " + req.getAttributeName()[i].toString() + " unknown");
                            }
                    }
                } else {
                    choice.setActivityInfoDocument(activityInfoDocument);
                }
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
                    choice.setAttributeInfoItem(null);
                } else if (message.indexOf("unknown") != -1) {
                    UnknownAttributeFault_type0 fault = new UnknownAttributeFault_type0();
                    fault.setTimestamp(GregorianCalendar.getInstance());
                    fault.setMessage(message);
                    
                    choice.setUnknownAttributeFault(fault);
                } else {
                    InternalBaseFault_Type fault = new InternalBaseFault_Type();
                    fault.setTimestamp(GregorianCalendar.getInstance());
                    fault.setMessage(message);
                    
                    choice.setInternalBaseFault(fault);
                }
            }
        }
//        XMLStreamWriter writer;
//        try {
//            writer = XMLOutputFactory.newInstance().createXMLStreamWriter(new FileWriter("/tmp/pippo3.xml"));
//            response.serialize(GetActivityInfoResponse.MY_QNAME, writer);
//            writer.flush();
//            writer.close();
//        } catch (XMLStreamException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        } catch (FactoryConfigurationError e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        } catch (IOException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
               
        logger.debug("END getActivityInfo");
        return response;
    }

    public GetActivityStatusResponse getActivityStatus(GetActivityStatus req) throws AccessControlFault, InternalBaseFault, VectorLimitExceededFault {
        logger.debug("BEGIN getActivityStatus");

        checkInitialization();

        if (req == null || req.getActivityID() == null) {
            throw new InternalBaseFault("ActivityId not specified!");
        }

        ActivityStatus_type0 status = null;
        ActivityStatusItemChoice_type1 choice = null;
        ActivityStatusItem_type0 activityStatusItem = null;

        ActivityCmd command = new ActivityCmd(ActivityCommandName.GET_ACTIVITY_STATUS);
        command.setUserId(CEUtils.getUserId());

        GetActivityStatusResponse response = new GetActivityStatusResponse();      

        for (String activityId : req.getActivityID()) {
            command.addParameter(ActivityCommandField.ACTIVITY_ID, activityId);
            choice = new ActivityStatusItemChoice_type1();

            activityStatusItem = new ActivityStatusItem_type0();
            activityStatusItem.setActivityID(activityId);
            activityStatusItem.setActivityStatusItemChoice_type1(choice);

            response.addActivityStatusItem(activityStatusItem);

            try {
                CommandManager.getInstance().execute(command);
                ActivityStatus activityStatus = (ActivityStatus) command.getResult().getParameter(ActivityCommandField.ACTIVITY_STATUS.name());

                status = new ActivityStatus_type0();
                status.setStatus(ActivityStatusState.Factory.fromValue(activityStatus.getStatusName().getName()));
                status.setTimestamp(activityStatus.getTimestamp().toGregorianCalendar());
                status.setDescription(activityStatus.getDescription());

                for (StatusAttributeName attribute : activityStatus.getStatusAttributes()) {
                    status.addAttribute(ActivityStatusAttribute.Factory.fromValue(attribute.getName()));
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
        }

        logger.debug("END getActivityStatus");
        return response;
    }

    public void init(ServiceContext serviceContext) throws AxisFault {
        if (initialization == INITIALIZATION_ERROR) {
            throw new AxisFault("ActivityManagementService not available: configuration failed!");
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

        AxisService axisService = axisConfiguration.getService("ActivityManagementService");
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

        logger.info("starting the ActivityManagementService initialization...");

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
            prefexIdFromEndpoint = "https://" + InetAddress.getLocalHost().getCanonicalHostName() + ":8443/Activity/";
        } catch (Throwable t) {
            initialization = INITIALIZATION_ERROR;
            logger.error("Configuration error: cannot get the host name: " + t.getMessage());
            return;
        }

        initialization = INITIALIZATION_OK;
        logger.info("ActivityManagementService initialization done!");
        logger.info("ActivityManagementService started!");
    }

    public NotifyServiceResponse notifyService(NotifyService req) throws AccessControlFault, InternalBaseFault, InternalNotificationFault, VectorLimitExceededFault {
        logger.debug("BEGIN notifyService");

        checkInitialization();

        if (req == null) {
            throw new InternalBaseFault("Empty request received!");
        }

        int index = 0;
        ActivityCmd command = null;
        NotifyResponseItem_type0[] notifyResponseItem = new NotifyResponseItem_type0[req.getNotifyRequestItem().length];

        for (NotifyRequestItem_type0 item : req.getNotifyRequestItem()) {
            NotifyResponseItemChoice_type0 choice = new NotifyResponseItemChoice_type0();

            notifyResponseItem[index] = new NotifyResponseItem_type0();
            notifyResponseItem[index].setActivityID(item.getActivityID());
            notifyResponseItem[index].setNotifyResponseItemChoice_type0(choice);

            command = new ActivityCmd(ActivityCommandName.NOTIFY_SERVICE);
            command.setUserId(CEUtils.getUserId());
            command.setCommandGroupId(item.getActivityID());
            command.addParameter(ActivityCommandField.ACTIVITY_ID, item.getActivityID());
            command.addParameter(ActivityCommandField.NOTIFY_MESSAGE, item.getNotifyMessage().getValue());
            command.addParameter(ActivityCommandField.USER_DN_RFC2253, CEUtils.getUserDN_RFC2253());
            command.addParameter(ActivityCommandField.ACTIVITY_CHECK, ActivityCommandField.ACTIVITY_CHECK_ON);

            try {
                CommandManager.getInstance().execute(command);

                command.getParameterKeySet().remove(ActivityCommandField.ACTIVITY_CHECK.name());
                command.setAsynchronous(true);

                CommandManager.getInstance().execute(command);
                choice.setAcknowledgement(item.getActivityID());
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

        NotifyServiceResponse response = new NotifyServiceResponse();
        response.setNotifyResponseItem(notifyResponseItem);

        logger.debug("END notifyService");
        return response;
    }

    public PauseActivityResponse pauseActivity(PauseActivity req) throws AccessControlFault, InternalBaseFault, VectorLimitExceededFault {
        logger.debug("BEGIN pauseActivity");

        checkInitialization();

        if (req == null || req.getActivityID() == null || req.getActivityID().length == 0) {
            throw new InternalBaseFault("none activityId found in the request");
        }

        String[] activityIds = req.getActivityID();

        PauseActivityResponseItem_type0[] responseItems = new PauseActivityResponseItem_type0[activityIds.length];
        PauseActivityResponseItemChoice_type1 choice = null;
        ActivityCmd command = new ActivityCmd(ActivityCommandName.PAUSE_ACTIVITY);
        command.setUserId(CEUtils.getUserId());

        for (int i = 0; i < activityIds.length; i++) {
            command.setAsynchronous(false);
            command.setCommandGroupId(activityIds[i]);
            command.addParameter(ActivityCommandField.ACTIVITY_ID, activityIds[i]);
            command.addParameter(ActivityCommandField.ACTIVITY_CHECK, ActivityCommandField.ACTIVITY_CHECK_ON);
            choice = new PauseActivityResponseItemChoice_type1();

            try {
                CommandManager.getInstance().execute(command);

                command.getParameterKeySet().remove(ActivityCommandField.ACTIVITY_CHECK.name());
                command.setAsynchronous(true);

                CommandManager.getInstance().execute(command);
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

            responseItems[i] = new PauseActivityResponseItem_type0();
            responseItems[i].setActivityID(activityIds[i]);
            responseItems[i].setPauseActivityResponseItemChoice_type1(choice);
        }

        PauseActivityResponse response = new PauseActivityResponse();
        response.setPauseActivityResponseItem(responseItems);

        logger.debug("END pauseActivity");
        return response;
    }

    public RestartActivityResponse restartActivity(RestartActivity req) throws AccessControlFault, InternalBaseFault, OperationNotPossibleFault, VectorLimitExceededFault {
        logger.debug("BEGIN restartActivity");

        checkInitialization();

        throw new InternalBaseFault("operation not implemented");
    }

    public ResumeActivityResponse resumeActivity(ResumeActivity req) throws AccessControlFault, InternalBaseFault, VectorLimitExceededFault {
        logger.debug("BEGIN resumeActivity");

        checkInitialization();

        if (req == null || req.getActivityID() == null || req.getActivityID().length == 0) {
            throw new InternalBaseFault("none activityId found in the request");
        }

        String[] activityIds = req.getActivityID();

        ResumeActivityResponseItem_type0[] responseItems = new ResumeActivityResponseItem_type0[activityIds.length];
        ResumeActivityResponseItemChoice_type1 choice = null;
        ActivityCmd command = new ActivityCmd(ActivityCommandName.RESUME_ACTIVITY);
        command.setUserId(CEUtils.getUserId());

        for (int i = 0; i < activityIds.length; i++) {
            command.setAsynchronous(false);
            command.setCommandGroupId(activityIds[i]);
            command.addParameter(ActivityCommandField.ACTIVITY_ID, activityIds[i]);
            command.addParameter(ActivityCommandField.ACTIVITY_CHECK, ActivityCommandField.ACTIVITY_CHECK_ON);
            choice = new ResumeActivityResponseItemChoice_type1();

            try {
                CommandManager.getInstance().execute(command);

                command.getParameterKeySet().remove(ActivityCommandField.ACTIVITY_CHECK.name());
                command.setAsynchronous(true);

                CommandManager.getInstance().execute(command);
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

            responseItems[i] = new ResumeActivityResponseItem_type0();
            responseItems[i].setActivityID(activityIds[i]);
            responseItems[i].setResumeActivityResponseItemChoice_type1(choice);
        }

        ResumeActivityResponse response = new ResumeActivityResponse();
        response.setResumeActivityResponseItem(responseItems);

        logger.debug("END resumeActivity");
        return response;
    }

    public WipeActivityResponse wipeActivity(WipeActivity req) throws AccessControlFault, InternalBaseFault, VectorLimitExceededFault {
        logger.debug("BEGIN wipeActivity");

        checkInitialization();

        if (req == null || req.getActivityID() == null || req.getActivityID().length == 0) {
            throw new InternalBaseFault("none activityId found in the request");
        }

        String[] activityIds = req.getActivityID();

        WipeActivityResponseItem_type0[] responseItems = new WipeActivityResponseItem_type0[activityIds.length];
        WipeActivityResponseItemChoice_type1 choice = null;
        ActivityCmd command = new ActivityCmd(ActivityCommandName.WIPE_ACTIVITY);
        command.setUserId(CEUtils.getUserId());

        for (int i = 0; i < activityIds.length; i++) {
            command.setAsynchronous(false);
            command.setCommandGroupId(activityIds[i]);
            command.addParameter(ActivityCommandField.ACTIVITY_ID, activityIds[i]);
            command.addParameter(ActivityCommandField.ACTIVITY_CHECK, ActivityCommandField.ACTIVITY_CHECK_ON);
            choice = new WipeActivityResponseItemChoice_type1();

            try {
                CommandManager.getInstance().execute(command);

                command.getParameterKeySet().remove(ActivityCommandField.ACTIVITY_CHECK.name());
                command.setAsynchronous(true);

                CommandManager.getInstance().execute(command);
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

            responseItems[i] = new WipeActivityResponseItem_type0();
            responseItems[i].setActivityID(activityIds[i]);
            responseItems[i].setWipeActivityResponseItemChoice_type1(choice);
        }

        WipeActivityResponse response = new WipeActivityResponse();
        response.setWipeActivityResponseItem(responseItems);

        logger.debug("END wipeActivity");
        return response;
    }
}
