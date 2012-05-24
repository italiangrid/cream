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
 * Authors: Eric Frizziero <eric.frizziero@pd.infn.it> 
 *
 */

package org.glite.ce.cream.activitymanagement.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.log4j.Logger;
import org.glite.ce.commonj.db.DatabaseException;
import org.glite.ce.commonj.db.DatasourceManager;
import org.glite.ce.commonj.utils.CEUtils;
import org.glite.ce.cream.activitymanagement.db.table.ActivityCommandTable;
import org.glite.ce.cream.activitymanagement.db.table.ActivityExecutableTypeTable;
import org.glite.ce.cream.activitymanagement.db.table.ActivityPropertyTable;
import org.glite.ce.cream.activitymanagement.db.table.ActivityStatusAttributeTable;
import org.glite.ce.cream.activitymanagement.db.table.ActivityStatusTable;
import org.glite.ce.cream.activitymanagement.db.table.ActivityTable;
import org.glite.ce.cream.activitymanagement.db.table.AnnotationTable;
import org.glite.ce.cream.activitymanagement.db.table.EnvironmentTable;
import org.glite.ce.cream.activitymanagement.db.table.ExecutableTypeArgumentTable;
import org.glite.ce.cream.activitymanagement.db.table.ExecutableTypeTable;
import org.glite.ce.cream.activitymanagement.db.table.InputFileTable;
import org.glite.ce.cream.activitymanagement.db.table.NotificationOnStateTable;
import org.glite.ce.cream.activitymanagement.db.table.NotificationTable;
import org.glite.ce.cream.activitymanagement.db.table.OutputFileTable;
import org.glite.ce.cream.activitymanagement.db.table.RecipientTable;
import org.glite.ce.cream.activitymanagement.db.table.RemoteLoggingTable;
import org.glite.ce.creamapi.activitymanagement.Activity;
import org.glite.ce.creamapi.activitymanagement.ActivityCommand;
import org.glite.ce.creamapi.activitymanagement.ActivityStatus;
import org.glite.ce.creamapi.activitymanagement.ActivityStatus.StatusAttributeName;
import org.glite.ce.creamapi.activitymanagement.ActivityStatus.StatusName;
import org.glite.ce.creamapi.activitymanagement.ListActivitiesResult;
import org.glite.ce.creamapi.activitymanagement.db.ActivityDB;
import org.glite.ce.creamapi.activitymanagement.db.ActivityDBInterface;
import org.glite.ce.creamapi.activitymanagement.db.NotificationDB;
import org.glite.ce.creamapi.activitymanagement.db.table.ActivityCommandTableInterface;
import org.glite.ce.creamapi.activitymanagement.db.table.ActivityExecutableTypeTableInterface;
import org.glite.ce.creamapi.activitymanagement.db.table.ActivityPropertyTableInterface;
import org.glite.ce.creamapi.activitymanagement.db.table.ActivityStatusAttributeTableInterface;
import org.glite.ce.creamapi.activitymanagement.db.table.ActivityStatusTableInterface;
import org.glite.ce.creamapi.activitymanagement.db.table.ActivityTableInterface;
import org.glite.ce.creamapi.activitymanagement.db.table.AnnotationTableInterface;
import org.glite.ce.creamapi.activitymanagement.db.table.EnvironmentTableInterface;
import org.glite.ce.creamapi.activitymanagement.db.table.ExecutableTypeArgumentTableInterface;
import org.glite.ce.creamapi.activitymanagement.db.table.ExecutableTypeTableInterface;
import org.glite.ce.creamapi.activitymanagement.db.table.InputFileTableInterface;
import org.glite.ce.creamapi.activitymanagement.db.table.NotificationOnStateTableInterface;
import org.glite.ce.creamapi.activitymanagement.db.table.NotificationTableInterface;
import org.glite.ce.creamapi.activitymanagement.db.table.OutputFileTableInterface;
import org.glite.ce.creamapi.activitymanagement.db.table.RecipientTableInterface;
import org.glite.ce.creamapi.activitymanagement.db.table.RemoteLoggingTableInterface;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.ActivityIdentification;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.ActivityTypeEnumeration;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.Application;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.DataStaging;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.ExecutableType;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.InputFile;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.Notification;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.OptionType;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.OptionalDuration;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.OptionalTime;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.OutputFile;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.RemoteLogging;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.Resources;
import org.glite.ce.creamapi.activitymanagement.wrapper.creation.types.PrimaryActivityStatus;

public class ActivityDBImplementation implements ActivityDBInterface {
    private static final Logger logger = Logger.getLogger(ActivityDBImplementation.class.getName());
    
    private ActivityTableInterface activityTable = null;
    private ActivityStatusTableInterface activityStatusTable = null;
    private ActivityCommandTableInterface activityCommandTable = null;
    private ActivityPropertyTableInterface activityPropertyTable = null;
    private ActivityStatusAttributeTableInterface activityStatusAttributeTable = null;
    private ActivityExecutableTypeTableInterface activityExecutableTypeTable = null;
    private ExecutableTypeTableInterface executableTypeTable = null;
    private ExecutableTypeArgumentTableInterface executableTypeArgumentTable = null;
    private EnvironmentTableInterface environmentTable = null;
    private RemoteLoggingTableInterface remoteLoggingTable = null;
    private AnnotationTableInterface annotationTable = null;
    private NotificationTableInterface notificationTable = null;
    private RecipientTableInterface recipientTable = null;
    private NotificationOnStateTableInterface notificationOnStateTable = null;
    private InputFileTableInterface inputFileTable = null;
    private OutputFileTableInterface outputFileTable = null;
    
    public ActivityDBImplementation () throws IllegalArgumentException, DatabaseException {
        this.activityTable = new ActivityTable();
        this.activityStatusTable = new ActivityStatusTable();
        this.activityCommandTable = new ActivityCommandTable();
        this.activityPropertyTable = new ActivityPropertyTable();
        this.activityStatusAttributeTable = new ActivityStatusAttributeTable();
        this.activityExecutableTypeTable = new ActivityExecutableTypeTable();
        this.executableTypeTable = new ExecutableTypeTable();
        this.executableTypeArgumentTable = new ExecutableTypeArgumentTable(); 
        this.environmentTable = new EnvironmentTable();
        this.remoteLoggingTable = new RemoteLoggingTable();
        this.annotationTable = new AnnotationTable();
        this.notificationTable = new NotificationTable();
        this.recipientTable = new RecipientTable();
        this.notificationOnStateTable = new NotificationOnStateTable(); 
        this.inputFileTable = new InputFileTable();
        this.outputFileTable = new OutputFileTable();
    }

    public String insertActivity(Activity activity) throws DatabaseException, IllegalArgumentException {
        logger.debug("Begin insert ActivityDescription");
        
        if (activity == null) {
            throw new IllegalArgumentException("activityDescription is null.");
        }
        
        if (activity.getUserId() == null) {
            throw new IllegalArgumentException("userId is null.");
        }
        
        Connection connection = DatasourceManager.getConnection(ActivityDBInterface.ACTIVITY_DATASOURCE_NAME);
        String activityId = null;
        try {
            activityId = activityTable.executeInsert(activity, activity.getUserId(), connection);
            
           //Insert activityStatus, activityStatusAttribute 
            if (activity.getStates() != null) {
                long activityStatusId = 0;
                for (ActivityStatus activityStatus : activity.getStates()) {
                    activityStatusId = activityStatusTable.executeInsert(activityId, activityStatus, connection);
                    activityStatus.setId(activityStatusId);
                    if ((activityStatus.getStatusAttributes() != null) && (!activityStatus.getStatusAttributes().isEmpty())) {
                        activityStatusAttributeTable.executeInsert(activityStatusId, activityStatus.getStatusAttributes(), connection);
                    }
                }
            }

            //Insert activityCommand 
            if ((activity.getCommands() != null) && (!activity.getCommands().isEmpty())) {
                for (ActivityCommand activityCommand : activity.getCommands()) {
                    activityCommandTable.executeInsert(activityId,activityCommand, connection);
                }
            }

            //Insert activityProperty
            if ((activity.getProperties() != null) && (!activity.getProperties().isEmpty())) {
                activityPropertyTable.executeInsert(activityId, activity.getProperties(), connection);   
            }
            
            //Insert annotation
            if ((activity.getActivityIdentification() != null) && 
                    activity.getActivityIdentification().getAnnotation() != null) {
                for (String annotation : activity.getActivityIdentification().getAnnotation()) {
                    annotationTable.executeInsert(activityId, annotation, connection);   
                }
            }
            //Insert executable, preExecutable, postExecutable
            long executableTypeId = 0;
            if (activity.getApplication() != null) {
                if (activity.getApplication().getExecutable() != null) {
                    executableTypeId = executableTypeTable.executeInsert(activity.getApplication().getExecutable(), connection);
                    activityExecutableTypeTable.executeInsert(activityId, executableTypeId, ActivityExecutableTypeTableInterface.EXECUTABLE, connection);
                    if (activity.getApplication().getExecutable().getArgument() != null) {
                        for (String argument : activity.getApplication().getExecutable().getArgument()) {
                            executableTypeArgumentTable.executeInsert(executableTypeId, argument, connection);   
                        }   
                    }
                }
                if (activity.getApplication().getPreExecutable() != null) {
                    for (ExecutableType executableType : activity.getApplication().getPreExecutable()) {
                        executableTypeId = executableTypeTable.executeInsert(executableType, connection);
                        activityExecutableTypeTable.executeInsert(activityId, executableTypeId, ActivityExecutableTypeTableInterface.PRE_EXECUTABLE, connection);
                        if (executableType.getArgument() != null) {
                            for (String argument : executableType.getArgument()) {
                                executableTypeArgumentTable.executeInsert(executableTypeId, argument, connection);   
                            }   
                        }
                    }
                }
                if (activity.getApplication().getPostExecutable() != null) {
                    for (ExecutableType executableType : activity.getApplication().getPostExecutable()) {
                        executableTypeId = executableTypeTable.executeInsert(executableType, connection);   
                        activityExecutableTypeTable.executeInsert(activityId, executableTypeId, ActivityExecutableTypeTableInterface.POST_EXECUTABLE, connection);
                        if (executableType.getArgument() != null) {
                            for (String argument : executableType.getArgument()) {
                                executableTypeArgumentTable.executeInsert(executableTypeId, argument, connection);   
                            }   
                        }
                    }
                }
                //Insert environment
                if (activity.getApplication().getEnvironment() != null) {
                    for (OptionType optionType : activity.getApplication().getEnvironment()) {
                        environmentTable.executeInsert(activityId, optionType, connection);   
                    }
                }
                //Insert remoteLogging
                if (activity.getApplication().getRemoteLogging() != null) {
                    for (RemoteLogging remoteLogging : activity.getApplication().getRemoteLogging()) {
                        remoteLoggingTable.executeInsert(activityId, remoteLogging, connection);    
                    }
                }
                //Insert notification
                if (activity.getApplication().getNotification() != null) {
                    long notificationId = 0;
                    for (Notification notification : activity.getApplication().getNotification()) {
                        notificationId = notificationTable.executeInsert(activityId, notification, connection);
                      //Insert recipient
                        if (notification.getRecipient() != null) {
                            for (String recipient : notification.getRecipient()) {
                                recipientTable.executeInsert(notificationId, recipient, connection);
                            }
                        }
                      //Insert onstate
                        if (notification.getOnState() != null) {
                            for (PrimaryActivityStatus primaryActivityStatus : notification.getOnState()) {
                                notificationOnStateTable.executeInsert(notificationId, primaryActivityStatus, connection);
                            }
                        }
                    }
                }
            } // end if (activity.getApplication() != null)

            //Insert InputFile, OutputFile
            if (activity.getDataStaging() != null) {
                for (InputFile inputFile : activity.getDataStaging().getInputFile()) {
                    inputFileTable.executeInsert(activityId, inputFile, connection);   
                }
                for (OutputFile outputFile : activity.getDataStaging().getOutputFile()) {
                    outputFileTable.executeInsert(activityId, outputFile, connection);
                }
            }
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException sqle) {
                throw new DatabaseException("Rollback is fault. ERRORCODE = " + sqle.getErrorCode() + " Message =  " + sqle.getMessage());
            }
            throw new DatabaseException("Rollback is fault. ERRORCODE = " + e.getErrorCode() + " Message =  " + e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }
        logger.debug("End insert ActivityDescription, userId = " + activity.getUserId());
        return activityId;
    }

    public Activity getActivity(String activityId, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("Begin getActivity, activityId = " + activityId + ", userId = " + userId);
        
        if (activityId == null) {
            throw new IllegalArgumentException("activityId is null.");
        }
        
        Connection connection = DatasourceManager.getConnection(ActivityDBInterface.ACTIVITY_DATASOURCE_NAME);
        
        switch (activityTable.isUserEnabled(activityId, userId, connection)) {
        case NO: 
                throw new IllegalArgumentException("UserId = " + userId + " is not enabled for that operation!");
        case NO_FOR_ACTIVITY_NOT_FOUND: 
                throw new IllegalArgumentException("Activity not found!");
        }
        
        Activity activity = null;
        ActivityIdentification activityIdentification = null;
        Application application = null;
        DataStaging dataStaging = null;
        Resources resources = null;
        
        try {
            ActivityDB activityDB = activityTable.executeSelect(activityId, userId, connection);
            if (activityDB != null) {
                activity = new Activity();
                activity.setId(activityId);
                
                // get ActivityIdentification
                List<String> annotation = annotationTable.executeSelect(activityId, connection);
                if (isNotNull(activityDB.getName()) || isNotNull(activityDB.getDescription()) || 
                    isNotNull(activityDB.getType()) || isNotNull(annotation) ) {
                    
                    activityIdentification = new ActivityIdentification();

                    activityIdentification.setName(activityDB.getName());
                    activityIdentification.setDescription(activityDB.getDescription());
                    if (isNotNull(activityDB.getType())) {
                        activityIdentification.setType(ActivityTypeEnumeration.fromValue(activityDB.getType()));
                    }

                    if (isNotNull(annotation)){
                        activityIdentification.getAnnotation().addAll(annotation);
                    }
                }
                activity.setActivityIdentification(activityIdentification);
                
                // get ActivityStatus
                List<ActivityStatus> activityStatusList = null;
                activityStatusList = activityStatusTable.executeSelect(activityId, userId, connection);
                if (activityStatusList != null) {
                    for (ActivityStatus activityStatus : activityStatusList) {
                        List<StatusAttributeName> statusAttributeNameList = null;
                        statusAttributeNameList = activityStatusAttributeTable.executeSelect(activityStatus.getId(), connection);
                        activityStatus.getStatusAttributes().addAll(statusAttributeNameList);
                    }
                }
                activity.setStates(activityStatusList);

                // get ActivityCommand
                List<ActivityCommand> activityCommandList = null;
                activityCommandList = activityCommandTable.executeSelect(activityId, userId, connection);
                activity.setCommands(activityCommandList);
                
                // get ActivityProperty
                activity.setProperties(activityPropertyTable.executeSelect(activityId, connection));
                
                // get Application
                List<Long> preExecutableTypeIdList = null;
                preExecutableTypeIdList = activityExecutableTypeTable.executeSelect(activityId,ActivityExecutableTypeTableInterface.PRE_EXECUTABLE, connection);
                List<Long> executableTypeIdList = null;
                executableTypeIdList = activityExecutableTypeTable.executeSelect(activityId, ActivityExecutableTypeTableInterface.EXECUTABLE, connection);
                List<Long> postExecutableTypeIdList = null;
                postExecutableTypeIdList = activityExecutableTypeTable.executeSelect(activityId, ActivityExecutableTypeTableInterface.POST_EXECUTABLE, connection);
                
                List<OptionType> environment =  environmentTable.executeSelect(activityId, connection);
                
                List<RemoteLogging> remoteLoggingList = null;
                remoteLoggingList = remoteLoggingTable.executeSelect(activityId, connection);
                
                List<NotificationDB> notificationDBList = null;
                notificationDBList = notificationTable.executeSelect(activityId, connection);
                
                if ( isNotNull(activityDB.getInput()) || (isNotNull(activityDB.getOutput())) || (isNotNull(activityDB.getError())) ||
                     isNotNull(activityDB.getExpirationTimeDate()) || isNotNull(activityDB.getWipeTimeDuration())||
                     isNotNull(preExecutableTypeIdList) || isNotNull(executableTypeIdList) || isNotNull(postExecutableTypeIdList) ||
                     isNotNull(environment) || isNotNull(remoteLoggingList) || isNotNull(notificationDBList)) {
                    
                    application = new Application();
                    application.setInput(activityDB.getInput());
                    application.setOutput(activityDB.getOutput());
                    application.setError(activityDB.getError());
                
                    if (isNotNull(activityDB.getExpirationTimeDate())) {
                        OptionalTime optionalTime = new OptionalTime();
                        optionalTime.setValue(CEUtils.getXMLGregorianCalendar(activityDB.getExpirationTimeDate()));
                        optionalTime.setOptional(activityDB.isExpirationTimeOptional());
                        application.setExpirationTime(optionalTime);
                    }
                
                    if (isNotNull(activityDB.getWipeTimeDuration())) {
                        OptionalDuration optionalduration = new OptionalDuration();
                        optionalduration.setValue(activityDB.getWipeTimeDuration());
                        optionalduration.setOptional(activityDB.isWipeTimeOptional());
                        application.setWipeTime(optionalduration);
                    }
                
                    application.getEnvironment().addAll(environment);
                    application.getRemoteLogging().addAll(remoteLoggingList);
                    
                    ExecutableType executableType = null;
                    List<String> argumentList = null;
                    if (preExecutableTypeIdList != null) {
                        for (Long executableTypeId : preExecutableTypeIdList) {
                            executableType = executableTypeTable.executeSelect(executableTypeId, connection);
                            argumentList = executableTypeArgumentTable.executeSelect(executableTypeId, connection);
                            executableType.getArgument().addAll(argumentList);
                            application.getPreExecutable().add(executableType);
                        }
                    }
                    if (executableTypeIdList != null) {
                        executableType = executableTypeTable.executeSelect(executableTypeIdList.get(0), connection);
                        argumentList = executableTypeArgumentTable.executeSelect(executableTypeIdList.get(0), connection);
                        executableType.getArgument().addAll(argumentList);
                        application.setExecutable(executableType);
                    }
                    if (postExecutableTypeIdList != null) {
                        for (Long executableTypeId : postExecutableTypeIdList) {
                            executableType = executableTypeTable.executeSelect(executableTypeId, connection);
                            argumentList = executableTypeArgumentTable.executeSelect(executableTypeId, connection);
                            executableType.getArgument().addAll(argumentList);
                            application.getPostExecutable().add(executableType);
                        }
                    }
                    
                    List<Notification> notificationList = null;
                    if (notificationDBList != null) {
                        notificationList = new ArrayList<Notification>(0);
                        List<String> recipient = null;
                        List<PrimaryActivityStatus> onstateList = null;
                        for (NotificationDB notificationDB : notificationDBList) {
                            recipient = recipientTable.executeSelect(notificationDB.getId(), connection);
                            onstateList = notificationOnStateTable.executeSelect(notificationDB.getId(), connection);
                            notificationDB.getNotification().getRecipient().addAll(recipient);
                            notificationDB.getNotification().getOnState().addAll(onstateList);
                            notificationList.add(notificationDB.getNotification());
                        }
                    }
                    application.getNotification().addAll(notificationList);
                    activity.setApplication(application);
                }
                
                // get DataStaging
                List<InputFile> inputFileList = null;
                inputFileList = inputFileTable.executeSelect(activityId, connection);
                List<OutputFile> outputFileList = null;
                outputFileList = outputFileTable.executeSelect(activityId, connection);
                if (isNotNull(inputFileList) || isNotNull(outputFileList)){
                    dataStaging = new DataStaging();
                    dataStaging.setClientDataPush(activityDB.isClientDataPush());
                    dataStaging.getInputFile().addAll(inputFileList);
                    dataStaging.getOutputFile().addAll(outputFileList);
                    activity.setDataStaging(dataStaging);
                }
                // get Resources
                if (isNotNull(activityDB.getQueueName())) {
                    resources = new Resources();
                    resources.setQueueName(activityDB.getQueueName());
                    activity.setResources(resources);
                }
            } else {
                logger.info("Activity not found for activityId = " + activityId + " and userId = " + userId);
            }
        } catch (SQLException e) {
            logger.error("ERRORCODE = " + e.getErrorCode() + " Message =  " + e.getMessage());
            throw new DatabaseException("ERRORCODE = " + e.getErrorCode() + " Message =  " + e.getMessage());
        } catch (Exception e1) {
            logger.error("ERROR: " + e1.getMessage(), e1);
            throw new DatabaseException(e1.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }
        logger.debug("End getActivity, activityId = " + activityId + ", userId = " + userId);
        return activity;
    }

    public void deleteActivity(String activityId, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("Begin deleteActivity for activityId = " + activityId);
        if (activityId == null) {
            throw new IllegalArgumentException("activityId is null.");
        }
        
        Connection connection = DatasourceManager.getConnection(ActivityDBInterface.ACTIVITY_DATASOURCE_NAME);

        switch (activityTable.isUserEnabled(activityId, userId, connection)) {
        case NO: 
            throw new IllegalArgumentException("UserId = " + userId + " is not enabled for that operation!");
        case NO_FOR_ACTIVITY_NOT_FOUND: 
            throw new IllegalArgumentException("Activity not found!");
        }
        
        try { 
            activityTable.executeDelete(activityId, userId, connection);
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException sqle) {
                throw new DatabaseException("Rollback is fault. ERRORCODE = " + sqle.getErrorCode() + " Message =  " + sqle.getMessage());
            }
            throw new DatabaseException("Rollback is fault. ERRORCODE = " + e.getErrorCode() + " Message =  " + e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }
        logger.debug("End deleteActivity for activityId = " + activityId);
    }
    
    private boolean isNotNull(Object o){
        return (o != null);
    }

    public ListActivitiesResult listActivities(XMLGregorianCalendar fromDate, XMLGregorianCalendar toDate, List<StatusName> statusList, List<StatusAttributeName> statusAttributeNameList, int limit, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("Begin listActivities");
        ListActivitiesResult listActivitiesResult = null;
        Connection connection = DatasourceManager.getConnection(ActivityDBInterface.ACTIVITY_DATASOURCE_NAME);
        try { 
            listActivitiesResult = activityStatusTable.executeListActivities(fromDate, toDate, statusList, statusAttributeNameList, limit, userId, connection);
        } catch (SQLException e) {
            logger.error("ERRORCODE = " + e.getErrorCode() + " Message =  " + e.getMessage());
            throw new DatabaseException("ERRORCODE = " + e.getErrorCode() + " Message =  " + e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }
        logger.debug("End listActivities");
        return listActivitiesResult;
    }

    public void insertActivityStatus(String activityId, ActivityStatus activityStatus) throws DatabaseException, IllegalArgumentException {
       logger.debug("Begin insertActivityStatus for activityId = " + activityId);
        
        if (activityId == null) {
            throw new IllegalArgumentException("activityId is null.");
        }
        if (activityStatus == null) {
            throw new IllegalArgumentException("activityStatus is null.");
        }
        
        Connection connection = DatasourceManager.getConnection(ActivityDBInterface.ACTIVITY_DATASOURCE_NAME);
        long activityStatusId = 0;
        try { 
            activityStatusId = activityStatusTable.executeInsert(activityId, activityStatus, connection);
            if ((activityStatus.getStatusAttributes() != null) && (!activityStatus.getStatusAttributes().isEmpty())) {
                activityStatusAttributeTable.executeInsert(activityStatusId, activityStatus.getStatusAttributes(), connection);
            } 
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException sqle) {
                throw new DatabaseException("Rollback is fault. ERRORCODE = " + sqle.getErrorCode() + " Message =  " + sqle.getMessage());
            }
            throw new DatabaseException("Rollback is fault. ERRORCODE = " + e.getErrorCode() + " Message =  " + e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }
        logger.debug("End insertActivityStatus for activityId = " + activityId);
    }

    public void insertActivityCommand(String activityId, ActivityCommand activityCommand) throws DatabaseException, IllegalArgumentException {
        logger.debug("Begin insertActivityCommand for activityId = " + activityId);
        
        if (activityId == null) {
            throw new IllegalArgumentException("activityId is null.");
        }
        if (activityCommand == null) {
            throw new IllegalArgumentException("activityCommand is null.");
        }
        Connection connection = DatasourceManager.getConnection(ActivityDBInterface.ACTIVITY_DATASOURCE_NAME);
        try {
            activityCommandTable.executeInsert(activityId,activityCommand, connection);
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException sqle) {
                throw new DatabaseException("Rollback is fault. ERRORCODE = " + sqle.getErrorCode() + " Message =  " + sqle.getMessage());
            }
            throw new DatabaseException("Rollback is fault. ERRORCODE = " + e.getErrorCode() + " Message =  " + e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }
        logger.debug("End insertActivityCommand for activityId = " + activityId);
    }

    public void updateActivity(Activity activity) throws DatabaseException, IllegalArgumentException {
        logger.debug("Begin updateActivity");
        
        if (activity == null) {
            throw new IllegalArgumentException("activity is null.");
        }
        
        if (activity.getId() == null) {
            throw new IllegalArgumentException("activityId is null.");
        }
        
        Connection connection = DatasourceManager.getConnection(ActivityDBInterface.ACTIVITY_DATASOURCE_NAME);
        try {
            activityPropertyTable.executeUpdate(activity.getId(), activity.getProperties(), connection);
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException sqle) {
                throw new DatabaseException("Rollback is fault. ERRORCODE = " + sqle.getErrorCode() + " Message =  " + sqle.getMessage());
            }
            throw new DatabaseException("Rollback is fault. ERRORCODE = " + e.getErrorCode() + " Message =  " + e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }
        logger.debug("End updateActivity");
    }
    
    public void updateActivityCommand(ActivityCommand activityCommand) throws DatabaseException, IllegalArgumentException {
        logger.debug("Begin updateActivityCommand");
        
        if (activityCommand == null) {
            throw new IllegalArgumentException("ActivityCommand is null.");
        }
        
        if (activityCommand.getId() == 0) {
            throw new IllegalArgumentException("CommandId not initialized.");
        }
        
        Connection connection = DatasourceManager.getConnection(ActivityDBInterface.ACTIVITY_DATASOURCE_NAME);
        try {
            activityCommandTable.executeUpdate(activityCommand, connection);
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException sqle) {
                throw new DatabaseException("Rollback is fault. ERRORCODE = " + sqle.getErrorCode() + " Message =  " + sqle.getMessage());
            }
            throw new DatabaseException("Rollback is fault. ERRORCODE = " + e.getErrorCode() + " Message =  " + e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }
        logger.debug("End updateActivityCommand");
    }

    public void updateActivityStatus(ActivityStatus activityStatus) throws DatabaseException, IllegalArgumentException {
        logger.debug("Begin updateActivityStatus");
        
        if (activityStatus == null) {
            throw new IllegalArgumentException("ActivityStatus is null.");
        }
        
        if (activityStatus.getId() == 0) {
            throw new IllegalArgumentException("StatusId not initialized.");
        }
        
        Connection connection = DatasourceManager.getConnection(ActivityDBInterface.ACTIVITY_DATASOURCE_NAME);
        try {
            activityStatusTable.executeUpdate(activityStatus, connection);
            activityStatusAttributeTable.executeUpdate(activityStatus.getId(), activityStatus.getStatusAttributes(), connection);
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException sqle) {
                throw new DatabaseException("Rollback is fault. ERRORCODE = " + sqle.getErrorCode() + " Message =  " + sqle.getMessage());
            }
            throw new DatabaseException("Rollback is fault. ERRORCODE = " + e.getErrorCode() + " Message =  " + e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }
        logger.debug("End updateActivityStatus");
    }

    public String retrieveOlderActivityId(List<StatusName> statusList, String userId) throws DatabaseException, IllegalArgumentException {
        logger.debug("Begin retrieveOlderActivityId");
        
        if ((statusList == null) || (statusList.size() == 0)) {
            throw new IllegalArgumentException("statusList is empty.");
        }
        
        String activityId = null;
        Connection connection = DatasourceManager.getConnection(ActivityDBInterface.ACTIVITY_DATASOURCE_NAME);
        try { 
            activityId = activityStatusTable.executeRetrieveOlderActivityId(statusList, userId, connection);
        } catch (SQLException e) {
            logger.error("ERRORCODE = " + e.getErrorCode() + " Message =  " + e.getMessage());
            throw new DatabaseException("ERRORCODE = " + e.getErrorCode() + " Message =  " + e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }
        logger.debug("End retrieveOlderActivityId");
        return activityId;
    }

    public List<String> listActivitiesForStatus(List<StatusName> statusList, String userId, int dateValue) throws DatabaseException, IllegalArgumentException {
        logger.debug("Begin listActivitiesForStatus");
        
        if ((statusList == null) || (statusList.size() == 0)) {
            throw new IllegalArgumentException("statusList is empty.");
        }

        if (dateValue < 0) {
            throw new IllegalArgumentException("DateValue must be >= 0.");
        }
        
        List<String> listActivityId = null;
        Connection connection = DatasourceManager.getConnection(ActivityDBInterface.ACTIVITY_DATASOURCE_NAME);
        try { 
            listActivityId = activityStatusTable.executeListActivitiesForStatus(statusList, userId, dateValue, connection);
        } catch (SQLException e) {
            logger.error("ERRORCODE = " + e.getErrorCode() + " Message =  " + e.getMessage());
            throw new DatabaseException("ERRORCODE = " + e.getErrorCode() + " Message =  " + e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    logger.error(sqle);
                }
            }
        }
        logger.debug("End listActivitiesForStatus");
        return listActivityId;
    }

}
