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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.log4j.Logger;
import org.glite.ce.commonj.utils.CEUtils;
import org.glite.ce.creamapi.activitymanagement.ActivityStatus;
import org.glite.ce.creamapi.activitymanagement.ActivityStatus.StatusAttributeName;
import org.glite.ce.creamapi.activitymanagement.ActivityStatus.StatusName;
import org.glite.ce.creamapi.activitymanagement.ListActivitiesResult;
import org.glite.ce.creamapi.activitymanagement.db.table.ActivityStatusTableInterface;

public class ActivityStatusTable implements ActivityStatusTableInterface {
    private static final Logger logger = Logger.getLogger(ActivityStatusTable.class.getName());
    private static final String insertSP = "{call insertActivityStatusTable (?, ?, ?, ?, ?, ?)}";
    private static final String selectSP = "{call selectActivityStatusTable (?, ?)}";
    private static final String updateSP = "{call updateActivityStatusTable (?, ?)}";
    private static final String selectListActivitiesSP = "{call selectListActivities (?, ?, ?, ?, ?, ?)}";
    private static final String selectRetrieveOlderActivityIdSP = "{call selectRetrieveOlderActivityId (?, ?)}";
    private static final String selectListActivitiesForStatusSP = "{call selectListActivitiesForStatus (?, ?, ?)}";
    
    public long executeInsert(String activityId, ActivityStatus activityStatus, Connection connection) throws SQLException {  
        CallableStatement insertCS = null;
        long activityStatusId = 0;
        try {
            insertCS = connection.prepareCall(insertSP);
            insertCS.setString(1, activityId);
            insertCS.setString(2, activityStatus.getStatusName().getName());
            insertCS.setTimestamp(3, new java.sql.Timestamp(activityStatus.getTimestamp().toGregorianCalendar().getTimeInMillis()));
            insertCS.setString(4, activityStatus.getDescription());
            insertCS.setBoolean(5, activityStatus.isTransient());
            insertCS.registerOutParameter(6, Types.BIGINT);
            insertCS.execute();
            activityStatusId = insertCS.getLong(6);
            logger.debug("insert into ActivityStatusTable done");
        } catch(SQLException se){
           throw se;            
        } finally {
            //finally block used to close resources
            try{
               if(insertCS != null) 
                   insertCS.close();
            } catch(SQLException se2) {
            } // nothing we can do
        }
        return activityStatusId;
    }

    public List<ActivityStatus> executeSelect(String activityId, String userId, Connection connection) throws SQLException {
        CallableStatement selectCS = null;
        List<ActivityStatus> activityStatusList = null;
        try {
            selectCS = connection.prepareCall(selectSP);
            selectCS.setString(1, activityId);
            selectCS.setString(2, userId);
            if (selectCS.execute()) {
                activityStatusList = new ArrayList<ActivityStatus>(0);
                ActivityStatus activityStatus = null;
                ResultSet resultSet = selectCS.getResultSet();
                while (resultSet.next()) {
                    activityStatus = new ActivityStatus();
                    activityStatus.setId(resultSet.getLong(ActivityStatusTableInterface.ID_LABEL));
                    activityStatus.setStatusName(StatusName.fromValue(resultSet.getString(ActivityStatusTableInterface.STATUS_LABEL)));
                    activityStatus.setDescription(resultSet.getString(ActivityStatusTableInterface.DESCRIPTION_LABEL));
                   
                   Timestamp timeStampField = null;
                   timeStampField = resultSet.getTimestamp(ActivityStatusTableInterface.TIMESTAMP_LABEL);
                   if (timeStampField != null) {
                       Calendar calendar = Calendar.getInstance();
                       calendar.setTimeInMillis(timeStampField.getTime());
                       activityStatus.setTimestamp(CEUtils.getXMLGregorianCalendar(calendar));
                   }
                   activityStatus.setIsTransient(resultSet.getBoolean(ActivityStatusTableInterface.ISTRANSIENT_LABEL));
                   activityStatusList.add(activityStatus);
                }
            }
            logger.debug("select ActivityStatusTable done");
        } catch(SQLException se) {
           throw se;            
        } finally {
            //finally block used to close resources
            try{
               if(selectCS != null) 
                   selectCS.close();
            } catch(SQLException se2) {
            } // nothing we can do
        }
        return activityStatusList;
    }

    public ListActivitiesResult executeListActivities(XMLGregorianCalendar fromDate, XMLGregorianCalendar toDate, List<StatusName> statusList, List<StatusAttributeName> statusAttributeNameList, int limit, String userId, Connection connection)
            throws SQLException {
        logger.debug("Begin executeListActivities");
        ListActivitiesResult listActivitiesResult = null;
        CallableStatement selectListActivitiesCS = null;
        try {
            selectListActivitiesCS = connection.prepareCall(selectListActivitiesSP);
            if (fromDate != null) {
                selectListActivitiesCS.setTimestamp(1,  new java.sql.Timestamp(fromDate.toGregorianCalendar().getTimeInMillis()));    
            } else {
                selectListActivitiesCS.setTimestamp(1, null);
            }
            if (toDate != null) {
                selectListActivitiesCS.setTimestamp(2,  new java.sql.Timestamp(toDate.toGregorianCalendar().getTimeInMillis()));    
            } else {
                selectListActivitiesCS.setTimestamp(2, null);
            }
            
            String statusListString = null;
            StringBuffer statusListStringBuffer = null;
            if (statusList != null) {
                statusListStringBuffer = new StringBuffer();
                for (StatusName statusName : statusList) {
                    statusListStringBuffer.append(", '" + statusName.getName() + "'");
                }
                statusListString = statusListStringBuffer.substring(2);
            }
            selectListActivitiesCS.setString(3, statusListString);
            
            String statusAttributeNameListString = null;
            StringBuffer statusAttributeNameListStringBuffer = null;
            if (statusAttributeNameList != null) {
                statusAttributeNameListStringBuffer = new StringBuffer();
                for (StatusAttributeName statusAttributeName : statusAttributeNameList) {
                    statusAttributeNameListStringBuffer.append(statusAttributeName.getName() + ", ");
                }
                statusAttributeNameListString = statusAttributeNameListStringBuffer.substring(1);
                
            }
            
            //TODO
            //selectListActivitiesCS.setString(4, statusAttributeNameListString);
            selectListActivitiesCS.setString(4, null);
            
            selectListActivitiesCS.setString(5, userId);
            selectListActivitiesCS.setInt(6, limit+1); //trick

            if (selectListActivitiesCS.execute()) {
                listActivitiesResult = new ListActivitiesResult();
                List<String> activityIdList = new ArrayList<String>(0);
                ResultSet resultSet = selectListActivitiesCS.getResultSet();
                
                while (resultSet.next()) {
                    activityIdList.add(resultSet.getString(ActivityStatusTableInterface.ACTIVITY_ID_LABEL));
                }
                if (activityIdList.size() > limit) { 
                    activityIdList.remove(limit); //see trick
                    listActivitiesResult.setIsTruncated(true);
                } else {
                    listActivitiesResult.setIsTruncated(false);
                }
                listActivitiesResult.setActivityIdList(activityIdList);
            }
            logger.debug("select ListActivities done");
        } catch(SQLException se) {
           throw se;            
        } finally {
            //finally block used to close resources
            try{
               if(selectListActivitiesCS != null) 
                   selectListActivitiesCS.close();
            } catch(SQLException se2) {
            } // nothing we can do
        }
        logger.debug("End executeListActivities");
        return listActivitiesResult;
    }

    public void executeUpdate(ActivityStatus activityStatus, Connection connection) throws SQLException {
        CallableStatement updateCS = null;
        try {
            updateCS = connection.prepareCall(updateSP);
            updateCS.setLong(1, activityStatus.getId());
            updateCS.setBoolean(2, activityStatus.isTransient());
            updateCS.execute();
            logger.debug("update ActivityStatusTable done");
        } catch(SQLException se){
           throw se;            
        } finally {
            //finally block used to close resources
            try{
               if(updateCS != null) 
                   updateCS.close();
            } catch(SQLException se2) {
            } // nothing we can do
        }
    }
    
    public String executeRetrieveOlderActivityId(List<StatusName> statusList, String userId, Connection connection)
            throws SQLException {
        logger.debug("Begin executeRetrieveOlderActivityId");
        String activityIdResult = null;
        CallableStatement selectRetrieveOlderActivityIdCS = null;
        try {
            selectRetrieveOlderActivityIdCS = connection.prepareCall(selectRetrieveOlderActivityIdSP);
            String statusListString = null;
            StringBuffer statusListStringBuffer = null;
            if (statusList != null) {
                statusListStringBuffer = new StringBuffer();
                for (StatusName statusName : statusList) {
                    statusListStringBuffer.append(", '" + statusName.getName() + "'");
                }
                statusListString = statusListStringBuffer.substring(2);
            }
            selectRetrieveOlderActivityIdCS.setString(1, statusListString);
            selectRetrieveOlderActivityIdCS.setString(2, userId);
            if (selectRetrieveOlderActivityIdCS.execute()) {
                ResultSet resultSet = selectRetrieveOlderActivityIdCS.getResultSet();
                
                while (resultSet.next()) {
                    activityIdResult = resultSet.getString(ActivityStatusTableInterface.ACTIVITY_ID_LABEL);
                }
            }
            logger.debug("select executeRetrieveOlderActivityId done");
        } catch(SQLException se) {
           throw se;            
        } finally {
            //finally block used to close resources
            try{
               if(selectRetrieveOlderActivityIdCS != null) 
                   selectRetrieveOlderActivityIdCS.close();
            } catch(SQLException se2) {
            } // nothing we can do
        }
        logger.debug("End executeRetrieveOlderActivityId");
        return activityIdResult;
    }

    //dateValue (MINUTE).
    public List<String> executeListActivitiesForStatus(List<StatusName> statusList, String userId, int dateValue, Connection connection)
            throws SQLException {
        logger.debug("Begin executeListActivitiesForStatus");
        
        List<String> listActivitiesResult = null;
        CallableStatement selectListActivitiesForStatusCS = null;
        try {
            selectListActivitiesForStatusCS = connection.prepareCall(selectListActivitiesForStatusSP);
            
            String statusListString = null;
            StringBuffer statusListStringBuffer = null;
            if (statusList != null) {
                statusListStringBuffer = new StringBuffer();
                for (StatusName statusName : statusList) {
                    statusListStringBuffer.append(", '" + statusName.getName() + "'");
                }
                statusListString = statusListStringBuffer.substring(2);
            }
            selectListActivitiesForStatusCS.setString(1, statusListString);
            selectListActivitiesForStatusCS.setString(2, userId);
            selectListActivitiesForStatusCS.setInt(3, dateValue);
            
            if (selectListActivitiesForStatusCS.execute()) {
                listActivitiesResult = new ArrayList<String>(0);
                ResultSet resultSet = selectListActivitiesForStatusCS.getResultSet();
                while (resultSet.next()) {
                    listActivitiesResult.add(resultSet.getString(ActivityStatusTableInterface.ACTIVITY_ID_LABEL));
                }
            }
            logger.debug("select executeListActivitiesForStatus done");
        } catch(SQLException se) {
           throw se;            
        } finally {
            //finally block used to close resources
            try{
               if(selectListActivitiesForStatusCS != null) 
                   selectListActivitiesForStatusCS.close();
            } catch(SQLException se2) {
            } // nothing we can do
        }
        logger.debug("End executeListActivitiesForStatus");
        return listActivitiesResult;
    }
}
