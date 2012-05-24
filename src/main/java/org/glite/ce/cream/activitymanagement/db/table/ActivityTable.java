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

package org.glite.ce.cream.activitymanagement.db.table;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.Random;

import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.Duration;
import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.log4j.Logger;
import org.glite.ce.creamapi.activitymanagement.Activity;
import org.glite.ce.creamapi.activitymanagement.db.ActivityDB;
import org.glite.ce.creamapi.activitymanagement.db.table.ActivityTableInterface;

public class ActivityTable implements ActivityTableInterface {
    private static final Logger logger = Logger.getLogger(ActivityTable.class.getName());
    private static final String insertSP = "{call insertActivityTable (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)}";
    private static final String selectSP = "{call selectActivityTable (?, ?)}";
    private static final String deleteSP = "{call deleteActivityTable (?, ?)}";
    private static final String selectActivityUserIdSP = "{call selectActivityUserId (?, ?)}";

    private static final Random activityIdGenerator = new Random(); 
    
    private synchronized String generateActivityId() {
        String suffix = "000000000" + activityIdGenerator.nextInt(1000000000);
        suffix = suffix.substring(suffix.length() - 9);
        return "CR_ES" + suffix;
    } 
    
    public String executeInsert(Activity activity, String userId, Connection connection) throws SQLException {
        String activityId = null;
        CallableStatement insertCS = null;
        try {
            insertCS = connection.prepareCall(insertSP);
        
            insertCS.setString(2, userId);
            if (activity.getActivityIdentification() != null) {
                insertCS.setString(3, activity.getActivityIdentification().getName());
                insertCS.setString(4, activity.getActivityIdentification().getDescription());
                insertCS.setString(5, (activity.getActivityIdentification().getType() != null) ? activity.getActivityIdentification().getType().value() : null);
                insertCS.setString(6, activity.getApplication().getInput());
                insertCS.setString(7, activity.getApplication().getOutput());
                insertCS.setString(8, activity.getApplication().getError());
            }
            if ((activity.getApplication() != null) && (activity.getApplication().getExpirationTime() != null)) {
                XMLGregorianCalendar expiration_time_date = activity.getApplication().getExpirationTime().getValue();
                insertCS.setTimestamp(9, new java.sql.Timestamp(expiration_time_date.toGregorianCalendar().getTimeInMillis()));
                insertCS.setBoolean(10, activity.getApplication().getExpirationTime().isOptional());
            } else {
                insertCS.setTimestamp(9, null);
                insertCS.setBoolean(10, false);
            }
        
            if ((activity.getApplication() != null) && (activity.getApplication().getWipeTime() != null)) {
                Duration duration = activity.getApplication().getWipeTime().getValue();
                insertCS.setString(11, duration.toString());
                insertCS.setBoolean(12, activity.getApplication().getWipeTime().isOptional());
            } else {
                insertCS.setString(11, null);
                insertCS.setBoolean(12, false);
            }
            
            if ((activity.getDataStaging() != null) && (activity.getDataStaging().isClientDataPush() != null)) {
                insertCS.setBoolean(13, activity.getDataStaging().isClientDataPush().booleanValue());
            } else {
                insertCS.setBoolean(13, false);    
            }
            
            if ((activity.getResources() != null) && (activity.getResources().getQueueName() != null)) {
                insertCS.setString(14, activity.getResources().getQueueName());
            } else {
                throw new SQLException("QueueName must be set.");
            }

            boolean inserted = false;
            int attempt = 1;
            while (!inserted) {
                try{        
                    activityId = generateActivityId();
                    insertCS.setString(1, activityId);
                    insertCS.execute();
                    inserted = true;
                    logger.debug("insert into ActivityTable done");
                } catch(SQLException se){
                    attempt++;
                    // 5 = maximun number of attempts
                    if (attempt > 5) {
                        throw se;
                    }
                }
            }
        } catch(SQLException se){
            logger.error(se.getMessage());
           throw se;            
        } finally {
            //finally block used to close resources
            try{
               if(insertCS != null) 
                   insertCS.close();
            } catch(SQLException se2) {
            } // nothing we can do
        }
        return activityId;
    }    
    
    public ActivityDB executeSelect(String activityId, String userId, Connection connection) throws SQLException {  
        CallableStatement selectCS = null;
        ActivityDB activityDB = null;
        try {
            selectCS = connection.prepareCall(selectSP);
            selectCS.setString(1, activityId);
            selectCS.setString(2, userId);
            if (selectCS.execute()) {
                activityDB = new ActivityDB();
                ResultSet resultSet = selectCS.getResultSet();
                while (resultSet.next()) {
                    activityDB.setActivityId(activityId);
                    activityDB.setUserId(userId);
                    activityDB.setName(resultSet.getString(ActivityTableInterface.NAME_LABEL));
                    activityDB.setDescription(resultSet.getString(ActivityTableInterface.DESCRIPTION_LABEL));
                    activityDB.setType(resultSet.getString(ActivityTableInterface.TYPE_LABEL));
                    activityDB.setInput(resultSet.getString(ActivityTableInterface.INPUT_LABEL));
                    activityDB.setOutput(resultSet.getString(ActivityTableInterface.OUTPUT_LABEL));
                    activityDB.setError(resultSet.getString(ActivityTableInterface.ERROR_LABEL));
                    
                    Timestamp timeStampField = null;
                    Calendar calendar = null;
                    timeStampField = resultSet.getTimestamp(ActivityTableInterface.EXPIRATIONTIME_DATE_LABEL);
                    if (timeStampField != null) {
                        calendar = Calendar.getInstance();
                        calendar.setTimeInMillis(timeStampField.getTime());
                        activityDB.setExpirationTimeDate(calendar);
                    }
                    activityDB.setExpirationTimeOptional(resultSet.getBoolean(ActivityTableInterface.EXPIRATIONTIME_OPTIONAL_LABEL));

                    String durationString = resultSet.getString(ActivityTableInterface.WIPETIME_DURATION_LABEL);
                    
                    if (durationString != null) {
                        try {
                        activityDB.setWipeTimeDuration(DatatypeFactory.newInstance().newDurationDayTime(durationString));
                        } catch (Exception e) {
                            logger.error("Error in WipeTimeDuration: " + e.getMessage());
                            throw new SQLException("Error in WipeTimeDuration: " + e.getMessage());
                        }
                    }
                    activityDB.setWipeTimeOptional(resultSet.getBoolean(ActivityTableInterface.WIPETIME_OPTIONAL_LABEL));
                    activityDB.setClientDataPush(resultSet.getBoolean(ActivityTableInterface.CLIENT_DATA_PUSH_LABEL));
                    activityDB.setQueueName(resultSet.getString(ActivityTableInterface.QUEUE_NAME_LABEL));
                }
            }
            logger.debug("select ActivityTable done");
        } catch(SQLException se){
           throw se;            
        } finally {
            //finally block used to close resources
            try{
               if(selectCS != null) 
                   selectCS.close();
            } catch(SQLException se2) {
            } // nothing we can do
        }
        return activityDB;
    }

    public void executeDelete(String activityId, String userId, Connection connection) throws SQLException {
        CallableStatement deleteCS = null;
        int rowCount = 0;
        try {
            deleteCS = connection.prepareCall(deleteSP);
            deleteCS.setString(1, activityId);
            deleteCS.setString(2, userId);
            rowCount = deleteCS.executeUpdate();
            logger.info("activityId = " + activityId + " deleted.");
        } catch(SQLException se){
            logger.error(se.getMessage());
           throw se;            
        } finally {
            //finally block used to close resources
            try{
               if(deleteCS != null) 
                   deleteCS.close();
            } catch(SQLException se2) {
            } // nothing we can do
        }
    }

    public UserEnabledForActivity isUserEnabled(String activityId, String userId, Connection connection) {
        logger.debug("Begin isUserEnabled for userID = " + userId);
        CallableStatement selectActivityUserIdCS = null;
        UserEnabledForActivity userEnabledForActivity = UserEnabledForActivity.NO;
        if (userId == null){
            userEnabledForActivity = UserEnabledForActivity.YES; //administrator
        } else {
            try {
                selectActivityUserIdCS = connection.prepareCall(selectActivityUserIdSP);
                selectActivityUserIdCS.setString(1, activityId);
                selectActivityUserIdCS.registerOutParameter(2, Types.VARCHAR);
                selectActivityUserIdCS.execute();
                String userIdFromDB = selectActivityUserIdCS.getString(2);
                logger.debug("userIdFromDB = " + userIdFromDB + " for activityId = " + activityId);
                if (userIdFromDB == null) {
                    userEnabledForActivity = UserEnabledForActivity.NO_FOR_ACTIVITY_NOT_FOUND;
                } else if (userId.equals(userIdFromDB)){
                        userEnabledForActivity = UserEnabledForActivity.YES;
                }
            } catch (SQLException sqle){
                logger.error("Problem to retrieve userId from DB : " + sqle.getMessage());
            }
        }
        logger.debug("End isUserEnabled for userID = " + userId);
        return userEnabledForActivity;
    }

}
