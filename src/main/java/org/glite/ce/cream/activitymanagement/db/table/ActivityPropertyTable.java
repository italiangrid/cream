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
import java.util.Hashtable;

import org.apache.log4j.Logger;
import org.glite.ce.creamapi.activitymanagement.db.table.ActivityPropertyTableInterface;


public class ActivityPropertyTable implements ActivityPropertyTableInterface {
    private static final Logger logger = Logger.getLogger(ActivityPropertyTable.class.getName());
    private static final String insertSP = "{call insertActivityPropertyTable (?, ?, ?)}";
    private static final String selectSP = "{call selectActivityPropertyTable (?)}";
    private static final String deleteSP = "{call deleteActivityPropertyTable (?)}";
    
    public void executeInsert(String activityId, Hashtable<String, String> properties, Connection connection) throws SQLException {  
        CallableStatement insertCS = null;
        try {
            insertCS = connection.prepareCall(insertSP);
            insertCS.setString(1, activityId);
            for (String key : properties.keySet()) {
                insertCS.setString(2, key);
                insertCS.setString(3, properties.get(key));
                insertCS.execute();
            }
            logger.debug("insert into ActivityPropertyTable done");
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
    }

    public Hashtable<String, String> executeSelect(String activityId, Connection connection) throws SQLException {
        CallableStatement selectCS = null;
        Hashtable<String, String> activityProperty = null;
        try {
            selectCS = connection.prepareCall(selectSP);
            selectCS.setString(1, activityId);
            if (selectCS.execute()) {
                activityProperty = new Hashtable<String, String>(0);
                ResultSet resultSet = selectCS.getResultSet();
                while (resultSet.next()) {
                    activityProperty.put(resultSet.getString(ActivityPropertyTableInterface.NAME_LABEL),resultSet.getString(ActivityPropertyTableInterface.VALUE_LABEL));
                }
            }
            logger.debug("select ActivityPropertyTable done");
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
        return activityProperty;
    }
    
    private void executeDelete(String activityId, Connection connection) throws SQLException {  
        CallableStatement deleteCS = null;
        try {
            deleteCS = connection.prepareCall(deleteSP);
            deleteCS.setString(1, activityId);
            deleteCS.execute();
            logger.debug("delete ActivityPropertyTable done");
        } catch(SQLException se){
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
    
    //first version
    public void executeUpdate( String activityId, Hashtable<String, String> properties, Connection connection) throws SQLException {  
        this.executeDelete(activityId, connection);
        if ((properties != null) && (!properties.isEmpty())) { 
            this.executeInsert(activityId, properties, connection);
        }
        logger.debug("update ActivityPropertyTable done");
    }
}
