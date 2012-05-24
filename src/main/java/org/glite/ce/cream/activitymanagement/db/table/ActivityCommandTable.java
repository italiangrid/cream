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

import org.apache.log4j.Logger;
import org.glite.ce.commonj.utils.CEUtils;
import org.glite.ce.creamapi.activitymanagement.ActivityCommand;
import org.glite.ce.creamapi.activitymanagement.db.table.ActivityCommandTableInterface;

public class ActivityCommandTable implements ActivityCommandTableInterface {
    private static final Logger logger = Logger.getLogger(ActivityCommandTable.class.getName());
    private static final String insertSP = "{call insertActivityCommandTable (?, ?, ?, ?, ?)}";
    private static final String selectSP = "{call selectActivityCommandTable (?, ?)}";
    private static final String updateSP = "{call updateActivityCommandTable (?, ?)}";
    
    
    public long executeInsert(String activityId, ActivityCommand activityCommand, Connection connection) throws SQLException {  
        CallableStatement insertCS = null;
        long activityCommandId = 0;
        try {
            insertCS = connection.prepareCall(insertSP);
            insertCS.setString(1, activityId);
            insertCS.setString(2, activityCommand.getName());
            insertCS.setTimestamp(3, new java.sql.Timestamp(activityCommand.getTimestamp().toGregorianCalendar().getTimeInMillis()));
            insertCS.setBoolean(4, activityCommand.isSuccess());
            insertCS.registerOutParameter(5, Types.BIGINT);
            insertCS.execute();
            activityCommandId = insertCS.getLong(5);
            logger.debug("insert into ActivityCommandTable done");
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
        return activityCommandId;
    }

    public List<ActivityCommand> executeSelect(String activityId, String userId, Connection connection) throws SQLException {
        CallableStatement selectCS = null;
        List<ActivityCommand> activityCommandList = null;
        try {
            selectCS = connection.prepareCall(selectSP);
            selectCS.setString(1, activityId);
            selectCS.setString(2, userId);
            if (selectCS.execute()) {
                activityCommandList = new ArrayList<ActivityCommand>(0);
                ActivityCommand activityCommand = null;
                ResultSet resultSet = selectCS.getResultSet();
                while (resultSet.next()) {
                    activityCommand = new ActivityCommand();
                    activityCommand.setName(resultSet.getString(ActivityCommandTableInterface.NAME_LABEL));
                   
                   Timestamp timeStampField = null;
                   timeStampField = resultSet.getTimestamp(ActivityCommandTableInterface.TIMESTAMP_LABEL);
                   if (timeStampField != null) {
                       Calendar calendar = Calendar.getInstance();
                       calendar.setTimeInMillis(timeStampField.getTime());
                       activityCommand.setTimestamp(CEUtils.getXMLGregorianCalendar(calendar));
                   }
                   activityCommand.setIsSuccess(resultSet.getBoolean(ActivityCommandTableInterface.ISSUCCESS_LABEL));
                   activityCommandList.add(activityCommand);
                }
            }
            logger.debug("select ActivityCommandTable done");
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
        return activityCommandList;
    }
    
    public void executeUpdate(ActivityCommand activityCommand, Connection connection) throws SQLException {
        CallableStatement updateCS = null;
        try {
            updateCS = connection.prepareCall(updateSP);
            updateCS.setLong(1, activityCommand.getId());
            updateCS.setBoolean(2, activityCommand.isSuccess());
            updateCS.execute();
            logger.debug("update ActivityCommandTable done");
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

}
