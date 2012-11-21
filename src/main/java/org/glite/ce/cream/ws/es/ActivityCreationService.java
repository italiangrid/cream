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
import java.util.GregorianCalendar;
import java.util.Hashtable;
import java.util.List;

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
import org.glite.ce.creamapi.activitymanagement.ActivityStatus;
import org.glite.ce.creamapi.activitymanagement.ActivityStatus.StatusAttributeName;
import org.glite.ce.creamapi.activitymanagement.ActivityStatus.StatusName;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.ActivityDescription;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.Application;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.DataStaging;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.ExecutableType;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.InputFile;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.Notification;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.OutputFile;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.RemoteLogging;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.Source;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.Target;
import org.glite.ce.creamapi.cmdmanagement.CommandManagerInterface;
import org.glite.ce.creamapi.delegationmanagement.Delegation;
import org.glite.ce.creamapi.delegationmanagement.DelegationCommand;
import org.glite.ce.creamapi.delegationmanagement.DelegationException;
import org.glite.ce.creamapi.ws.es.adl.ActivityDescription_type0;
import org.glite.ce.creamapi.ws.es.creation.AccessControlFault;
import org.glite.ce.creamapi.ws.es.creation.ActivityCreationServiceSkeletonInterface;
import org.glite.ce.creamapi.ws.es.creation.InternalBaseFault;
import org.glite.ce.creamapi.ws.es.creation.VectorLimitExceededFault;
import org.glite.ce.creamapi.ws.es.creation.types.ActivityCreationResponseSequence_type1;
import org.glite.ce.creamapi.ws.es.creation.types.ActivityCreationResponse_type0;
import org.glite.ce.creamapi.ws.es.creation.types.ActivityStatusAttribute;
import org.glite.ce.creamapi.ws.es.creation.types.ActivityStatusState;
import org.glite.ce.creamapi.ws.es.creation.types.ActivityStatus_type0;
import org.glite.ce.creamapi.ws.es.creation.types.CreateActivity;
import org.glite.ce.creamapi.ws.es.creation.types.CreateActivityResponse;
import org.glite.ce.creamapi.ws.es.creation.types.DirectoryReference;
import org.glite.ce.creamapi.ws.es.creation.types.InternalBaseFault_Type;
import org.glite.ce.creamapi.ws.es.creation.types.InvalidActivityDescriptionFault_type0;
import org.glite.ce.creamapi.ws.es.creation.types.InvalidActivityDescriptionSemanticFault_type0;
import org.glite.ce.creamapi.ws.es.creation.types.UnsupportedCapabilityFault_type0;

public class ActivityCreationService implements ActivityCreationServiceSkeletonInterface, Lifecycle {
    private static final Logger logger = Logger.getLogger(ActivityCreationService.class.getName());
    private static final int INITIALIZATION_TBD = 0;
    private static final int INITIALIZATION_OK = 1;
    private static final int INITIALIZATION_ERROR = 2;
    private static int initialization = INITIALIZATION_TBD;
    private static int vectorLimit = 200;
    private static String gsiURL = null;
    private static String activityManagerURL = null;
    private static String resourceInfoEndpointURL = null;

    private void checkInitialization() throws InternalBaseFault {
        if (initialization == INITIALIZATION_ERROR) {
            throw new InternalBaseFault("CreationService not available: configuration failed!");
        }
    }

    private Delegation getDelegation(String delegationId, String userDN, String localUser) throws DelegationException {
        DelegationCommand command = new DelegationCommand(DelegationCommand.GET_DELEGATION);
        command.addParameter(DelegationCommand.DELEGATION_ID, delegationId);
        command.addParameter(DelegationCommand.USER_DN_RFC2253, userDN);
        command.addParameter(DelegationCommand.LOCAL_USER, localUser);

        try {
            CommandManager.getInstance().execute(command);
        } catch (Throwable t) {
            throw new DelegationException(t.getMessage());
        } 

        Delegation delegation = (Delegation) command.getResult().getParameter("DELEGATION");

        if (delegation == null) {
            throw new DelegationException("delegation [id=" + delegationId + "] not found!");
        }

        if (!delegation.isValid()) {
            throw new DelegationException("the delegation [id=" + delegationId + "] is expired!");
        }

        return delegation;
    }

    private String checkValue(String key, String value) throws IllegalArgumentException {
        if (value == null || "".equals(value.trim())) {
            throw new IllegalArgumentException(key + " not specified!");
        }

        return value.trim();
    }

    private String checkValidity(ActivityDescription activity, String userDN, String localUser) throws IllegalArgumentException, DelegationException {
        if (activity == null) {
            throw new IllegalArgumentException("activity not specified!");
        }

        Hashtable<String, Delegation> delegationIdtable = new Hashtable<String, Delegation>(0);

        Application application = activity.getApplication();
        if (application == null) {
            throw new IllegalArgumentException("application not specified!");
        }

        for (Notification notification : application.getNotification()) {
            if (!notification.isOptional()) {
                throw new IllegalArgumentException("events notification capability not supported!");
            }
        }

        if (application.getExpirationTime() != null && !application.getExpirationTime().isOptional()) {
            throw new IllegalArgumentException("application expirationTime not supported!");
        }

        if (application.getWipeTime() != null && !application.getWipeTime().isOptional()) {
            throw new IllegalArgumentException("application wipeTime not supported!");
        }

        for (RemoteLogging remoteLogging : application.getRemoteLogging()) {
            if (!remoteLogging.isOptional()) {
                throw new IllegalArgumentException("remote logging capability not supported!");
            }
        }

        if (application.getExecutable() == null) {
            throw new IllegalArgumentException("executable not specified!");
        } else {
            application.getExecutable().setPath(checkValue("executable path", application.getExecutable().getPath()));
        }

        if (application.getInput() != null) {
            application.setInput(application.getInput().trim());
        }

        if (application.getOutput() != null) {
            application.setOutput(application.getOutput().trim());
        }

        if (application.getError() != null) {
            application.setError(application.getError().trim());
        }

        for (ExecutableType preExecutable : application.getPreExecutable()) {
            preExecutable.setPath(checkValue("preExecutable path", preExecutable.getPath()));
        }

        for (ExecutableType postExecutable : application.getPostExecutable()) {
            postExecutable.setPath(checkValue("postExecutable path", postExecutable.getPath()));
        }

        if (activity.getDataStaging() != null) {
            DataStaging dataStaging = activity.getDataStaging();
            String delegationId = null;
            String uri = null;

            for (InputFile inputFile : dataStaging.getInputFile()) {
                inputFile.setName(checkValue("inputFile name", inputFile.getName()));

                if (inputFile.getSource().size() == 0 && !dataStaging.isClientDataPush()) {
                    throw new IllegalArgumentException("datastaging not possible: whenever the clientDataPush attribute is not set, at least one inputFile source must be specified (inputFile " + inputFile.getName() + ")");
                }

                for (Source source : inputFile.getSource()) {
                    source.setURI(checkValue("source URI related to the inputFile " + inputFile.getName(), source.getURI()));

                    uri = source.getURI();

                    if (!uri.startsWith("https://") && !uri.startsWith("gsiftp://") && !uri.startsWith("file://")) {
                        throw new IllegalArgumentException("protocol " + uri.substring(0, uri.indexOf("://")) + " not supported for the inputFile " + inputFile.getName());
                    }

                    source.setDelegationID(checkValue("delegationId related to the inputFile " + inputFile.getName(), source.getDelegationID()));

                    delegationId = source.getDelegationID();

                    if (delegationId != null && !delegationIdtable.containsKey(delegationId)) {
                        delegationIdtable.put(delegationId, getDelegation(delegationId, userDN, localUser));
                    }
                }
            }

            for (OutputFile outputFile : dataStaging.getOutputFile()) {
                outputFile.setName(checkValue("outputFile name", outputFile.getName()));

                for (Target target : outputFile.getTarget()) {
                    target.setURI(checkValue("target URI related to the outputFile " + outputFile.getName(), target.getURI()));

                    uri = target.getURI();

                    if (!uri.startsWith("https://") && !uri.startsWith("gsiftp://") && !uri.startsWith("file://")) {
                        throw new IllegalArgumentException("protocol " + uri.substring(0, uri.indexOf("://")) + " not supported for the outputFile " + outputFile.getName());
                    }

                    target.setDelegationID(checkValue("delegationId related to the outputFile " + outputFile.getName(), target.getDelegationID()));

                    delegationId = target.getDelegationID();

                    if (delegationId != null && !delegationIdtable.containsKey(delegationId)) {
                        delegationIdtable.put(delegationId, getDelegation(delegationId, userDN, localUser));
                    }
                }
            }
        }

        //        if (activity.getResources() != null) {
        //        	Resources_type0 resources = seq.getResources();
        //        	
        //        	if (resources.isBenchmarkSpecified() && !resources.getBenchmark().getOptional()) {
        //        		throw new IllegalArgumentException("benchmark requirement not supported!");
        //        	}
        //
        //        	if (resources.isCoprocessorSpecified() && !resources.getCoprocessor().getOptional()) {
        //        		throw new IllegalArgumentException("coprocessor requirement not supported!");
        //        	}
        //
        //        	if (resources.isDiskSpaceRequirementSpecified()) {
        //        		throw new IllegalArgumentException("diskSpace requirement not supported!");
        //        	}
        //
        //        	if (resources.isIndividualCPUTimeSpecified()) {
        //        		throw new IllegalArgumentException("individualCPUTime requirement not supported!");
        //        	}
        //
        //        	if (resources.isIndividualPhysicalMemorySpecified()) {
        //        		throw new IllegalArgumentException("individualPhysicalMemory requirement not supported!");
        //        	}
        //        	
        //        	if (resources.isIndividualVirtualMemorySpecified()) {
        //        		throw new IllegalArgumentException("individualVirtualMemory requirement not supported!");
        //        	}
        //
        //        	if (resources.isNetworkInfoSpecified() && !resources.getNetworkInfo().getOptional()) {
        //        		throw new IllegalArgumentException("networkInfo requirement not supported!");
        //        	}
        //
        //        	if (resources.isNodeAccessSpecified()) {
        //        		throw new IllegalArgumentException("nodeAccess requirement not supported!");
        //        	}
        //
        //        	if (resources.isOperatingSystemSpecified()) {
        //        		throw new IllegalArgumentException("operatingSystem requirement not supported!");
        //        	}
        //
        //        	if (resources.isParallelEnvironmentSpecified()) {
        //        		throw new IllegalArgumentException("parallelEnvironment requirement not supported!");
        //        	}
        //
        //        	if (resources.isPlatformSpecified()) {
        //        		throw new IllegalArgumentException("platform requirement not supported!");
        //        	}
        //
        //        	if (resources.isRemoteSessionAccessSpecified()) {
        //        		throw new IllegalArgumentException("remoteSessionAccess requirement not supported!");
        //        	}
        //
        //        	if (resources.isRuntimeEnvironmentSpecified()) {
        //        		RuntimeEnvironment_type0[] runtimeEnvironment = resources.getRuntimeEnvironment();
        //
        //            	for (int i=0; i<runtimeEnvironment.length; i++) {
        //            		if (!runtimeEnvironment[i].getOptional()) {
        //            			throw new IllegalArgumentException("runtimeEnvironment requirement not supported!");
        //            		}
        //            	}
        //        	}
        //
        //        	if (resources.isSlotRequirementSpecified()) {
        //        		throw new IllegalArgumentException("slot requirement not supported!");
        //        	}
        //
        //        	if (resources.isTotalCPUTimeSpecified()) {
        //        		throw new IllegalArgumentException("totalCPUTime requirement not supported!");
        //        	}
        //
        //        	if (resources.isWallTimeSpecified()) {
        //        		throw new IllegalArgumentException("wallTime requirement not supported!");
        //        	}
        //        }

        String delegationSandboxPath = null;

        if (delegationIdtable.size() > 0) {
            Delegation delegation = delegationIdtable.elements().nextElement();
            delegationSandboxPath = delegation.getPath();
        }


        return delegationSandboxPath;

    }

    public CreateActivityResponse createActivity(CreateActivity createActivity) throws AccessControlFault, InternalBaseFault, VectorLimitExceededFault {
        checkInitialization();
        logger.debug("BEGIN createActivity");

        ActivityDescription_type0[] activityDescriptionArray = createActivity.getActivityDescription();
        if ((createActivity == null) || (activityDescriptionArray == null)) {
            throw new InternalBaseFault("Please provide at least an activity description");
        }

        if (activityDescriptionArray.length == 0) {
            throw new InternalBaseFault("Please provide at least an activity description");
        }

        if (activityDescriptionArray.length > vectorLimit) {
            throw new VectorLimitExceededFault("Vector limit exceeded! (limit=" + vectorLimit + ")");
        }

        String localUser = CEUtils.getLocalUser();
        if (localUser == null) {
            throw new InternalBaseFault("configuration problem: cannot get the localUser");
        }
        
        String activityId = null;
        String userFQAN = "";
        String userVO = CEUtils.getUserDefaultVO();
        List<String> fqanlist = null;

        if (userVO != null) {
            fqanlist = CEUtils.getFQAN(userVO);
        }

        if (fqanlist != null && fqanlist.size() > 0) {
            userFQAN = normalize(fqanlist.get(0).toString());
        }

        fqanlist = null;
        String userId = normalize(CEUtils.getUserDN_RFC2253()) + userFQAN;
        String userDN = CEUtils.getUserDN_RFC2253();
        String localUserGroup = CEUtils.getLocalUserGroup();
        String delegationSandboxPath = null;
        ActivityCmd command = null;
        ActivityDescription activityDescription = null;
        ActivityStatus status = null;
        ActivityStatus_type0 activityStatus = null;
        ActivityCreationResponse_type0[] activityCreationResponse = new ActivityCreationResponse_type0[activityDescriptionArray.length];
        ActivityCreationResponseSequence_type1 seq = null;

        for (int i=0; i<activityDescriptionArray.length; i++) {
            activityCreationResponse[i] = new ActivityCreationResponse_type0();
            activityDescription = new ActivityDescription(activityDescriptionArray[i].getActivityDescriptionSequence_type1());
            
            try {
                delegationSandboxPath = checkValidity(activityDescription, userDN, localUser);
            } catch (IllegalArgumentException e) {
                logger.error(e.getMessage());
//                InternalBaseFault_Type fault = null;
//
//                if (e.getMessage().contains("capability")) {
//                    fault = new UnsupportedCapabilityFault_type0();
//                } else if (e.getMessage().contains("requirement")) {
//                    fault = new InvalidActivityDescriptionFault_type0();
//                } else {
//                    fault = new InvalidActivityDescriptionSemanticFault_type0();
//                }
//
//                fault.setMessage(e.getMessage());
//                fault.setTimestamp(GregorianCalendar.getInstance());
//
//                activityCreationResponse[i].setInternalBaseFault(fault);

                if (e.getMessage().contains("capability")) {
                    UnsupportedCapabilityFault_type0 unsupportedCapabilityFault = new UnsupportedCapabilityFault_type0();
                    unsupportedCapabilityFault.setMessage(e.getMessage());
                    unsupportedCapabilityFault.setTimestamp(GregorianCalendar.getInstance());

                    activityCreationResponse[i].setUnsupportedCapabilityFault(unsupportedCapabilityFault);
                } else if (e.getMessage().contains("requirement")) {
                    InvalidActivityDescriptionFault_type0 invalidActivityDescriptionFault = new InvalidActivityDescriptionFault_type0();
                    invalidActivityDescriptionFault.setMessage(e.getMessage());
                    invalidActivityDescriptionFault.setTimestamp(GregorianCalendar.getInstance());

                    activityCreationResponse[i].setInvalidActivityDescriptionFault(invalidActivityDescriptionFault);
                } else {
                    InvalidActivityDescriptionSemanticFault_type0 invalidActivityDescriptionSemanticFault = new InvalidActivityDescriptionSemanticFault_type0();
                    invalidActivityDescriptionSemanticFault.setMessage(e.getMessage());
                    invalidActivityDescriptionSemanticFault.setTimestamp(GregorianCalendar.getInstance());

                    activityCreationResponse[i].setInvalidActivityDescriptionSemanticFault(invalidActivityDescriptionSemanticFault);
                }
                
                continue;
            } catch (Throwable de) {
                InternalBaseFault_Type fault = new InternalBaseFault_Type();
                fault.setMessage(de.getMessage());
                fault.setTimestamp(GregorianCalendar.getInstance());

                activityCreationResponse[i].setInternalBaseFault(fault);
                continue;
            }

            try {
                command = new ActivityCmd(ActivityCommandName.CREATE_ACTIVITY);
                command.setUserId(userId);
                command.addParameter(ActivityCommandField.USER_DN_RFC2253, userDN);
                command.addParameter(ActivityCommandField.USER_DN_X500, CEUtils.getUserDN_X500());
                command.addParameter(ActivityCommandField.USER_FQAN, userFQAN);
                command.addParameter(ActivityCommandField.VIRTUAL_ORGANISATION, userVO);
                command.addParameter(ActivityCommandField.LOCAL_USER, localUser);
                command.addParameter(ActivityCommandField.LOCAL_USER_GROUP, localUserGroup);
                command.addParameter(ActivityCommandField.SERVICE_URL, activityManagerURL);
                command.addParameter(ActivityCommandField.SERVICE_GSI_URL, gsiURL);
                command.addParameter(ActivityCommandField.ACTIVITY_DESCRIPTION, activityDescription);
                
                if (delegationSandboxPath != null) {
                    command.addParameter(ActivityCommandField.DELEGATION_SANDBOX_PATH, delegationSandboxPath);                    
                }
                
                CommandManager.getInstance().execute(command);

                activityId = command.getResult().getParameterAsString(ActivityCommandField.ACTIVITY_ID.name());
                status = (ActivityStatus)command.getResult().getParameter(ActivityCommandField.ACTIVITY_STATUS.name());

                ActivityStatusAttribute[] activityStatusAttributeArray = new ActivityStatusAttribute[status.getStatusAttributes().size()];
                int j=0;

                for (StatusAttributeName attribute : status.getStatusAttributes()) {
                    activityStatusAttributeArray[j++] = ActivityStatusAttribute.Factory.fromValue(attribute.getName());
                }

                activityStatus = new ActivityStatus_type0();
                activityStatus.setStatus(ActivityStatusState.Factory.fromValue(status.getStatusName().getName()));
                activityStatus.setDescription(status.getDescription());
                activityStatus.setTimestamp(status.getTimestamp().toGregorianCalendar());
                activityStatus.setAttribute(activityStatusAttributeArray);

                seq = new ActivityCreationResponseSequence_type1();
                seq.setActivityStatus(activityStatus);   
                seq.setActivityID(activityId);   
                seq.setActivityMgmtEndpointURL(new URI(activityManagerURL));
                seq.setResourceInfoEndpointURL(new URI(resourceInfoEndpointURL));

                if (command.getResult().getParameterAsString(ActivityCommandField.STAGE_IN_URI.name()) != null) {
                    DirectoryReference dir = new DirectoryReference();
                    dir.setURL(new URI[] { new URI(command.getResult().getParameterAsString(ActivityCommandField.STAGE_IN_URI.name())) });
                    seq.setStageInDirectory(dir);
                }

                if (command.getResult().getParameterAsString(ActivityCommandField.STAGE_OUT_URI.name()) != null) {
                    DirectoryReference dir = new DirectoryReference();
                    dir.setURL(new URI[] { new URI(command.getResult().getParameterAsString(ActivityCommandField.STAGE_OUT_URI.name())) });
                    seq.setStageOutDirectory(dir);
                }
               
                if (status.getStatusName() != StatusName.TERMINAL && !status.getStatusAttributes().contains(StatusAttributeName.CLIENT_STAGEIN_POSSIBLE)) {
                    command = new ActivityCmd(ActivityCommandName.START_ACTIVITY);
                    command.setUserId(userId);
                    command.setAsynchronous(true);
                    command.setCommandGroupId(activityId);
                    command.addParameter(ActivityCommandField.ACTIVITY_ID, activityId);

                    CommandManager.getInstance().execute(command);                    
                }      

                activityCreationResponse[i].setActivityCreationResponseSequence_type1(seq);
            } catch (Throwable e) {
                InternalBaseFault_Type fault = new InternalBaseFault_Type();
                fault.setMessage(e.getMessage());
                fault.setTimestamp(GregorianCalendar.getInstance());

                activityCreationResponse[i].setInternalBaseFault(fault);
            }
        }

        CreateActivityResponse response = new CreateActivityResponse();
        response.setActivityCreationResponse(activityCreationResponse);

        logger.debug("END createActivity");

        return response;
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
            throw new AxisFault("CreationService not available: configuration failed!");
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

        AxisService axisService = axisConfiguration.getService("ActivityCreationService");
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

        logger.info("starting the CreationService initialization...");

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

        if (!commandManager.checkCommandExecutor("DelegationExecutor", DelegationCommand.DELEGATION_MANAGEMENT)) {
            initialization = INITIALIZATION_ERROR;
            logger.error("Configuration error: DelegationExecutor not loaded!");
            return;
        }

        try {
            String hostName = InetAddress.getLocalHost().getCanonicalHostName();
            activityManagerURL = "https://" + hostName + ":8443/ce-cream-es/services/ActivityManagementService";
            resourceInfoEndpointURL = "https://" + hostName + ":8443/ce-cream-es/services/ResourceInfoService";
            gsiURL = "gsiftp://" + hostName;
        } catch (Throwable t) {
            initialization = INITIALIZATION_ERROR;
            logger.error("Configuration error: cannot get the host name: " + t.getMessage());
            return;
        }

        initialization = INITIALIZATION_OK;
        logger.info("CreationService initialization done!");
        logger.info("CreationService started!");
    }

    private String normalize(String s) {
        if (s != null) {
            return s.replaceAll("\\W", "_");
        }
        return null;
    }
}
