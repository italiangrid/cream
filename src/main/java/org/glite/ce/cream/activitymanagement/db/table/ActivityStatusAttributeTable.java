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
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.glite.ce.creamapi.activitymanagement.ActivityStatus.StatusAttributeName;
import org.glite.ce.creamapi.activitymanagement.db.table.ActivityStatusAttributeTableInterface;

public class ActivityStatusAttributeTable implements ActivityStatusAttributeTableInterface {
    private static final Logger logger = Logger.getLogger(ActivityStatusAttributeTable.class.getName());
    private static final String insertSP = "{call insertActivityStatusAttributeTable (?, ?)}";
    private static final String selectSP = "{call selectActivityStatusAttributeTable (?)}";
    private static final String deleteSP = "{call deleteActivityStatusAttributeTable (?)}";
    
    public void executeInsert(long activityStatusId, List<StatusAttributeName> listStatusAttributeName, Connection connection) throws SQLException {  
        CallableStatement insertCS = null;
        try {
            insertCS = connection.prepareCall(insertSP);
            insertCS.setLong(1, activityStatusId);
            for (StatusAttributeName statusAttributeName : listStatusAttributeName) {
                insertCS.setString(2, statusAttributeName.getName());
                insertCS.execute();
            }
            logger.debug("insert into ActivityStatusAttributeTable done");
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

    public List<StatusAttributeName> executeSelect(long activityStatusId, Connection connection) throws SQLException {
        CallableStatement selectCS = null;
        List<StatusAttributeName> activityStatusAttributeNameList = null;
        try {
            selectCS = connection.prepareCall(selectSP);
            selectCS.setLong(1, activityStatusId);
            if (selectCS.execute()) {
                activityStatusAttributeNameList = new ArrayList<StatusAttributeName>(0);
                ResultSet resultSet = selectCS.getResultSet();
                while (resultSet.next()) {
                    activityStatusAttributeNameList.add(StatusAttributeName.fromValue(resultSet.getString(ActivityStatusAttributeTableInterface.ATTRIBUTE_LABEL)));
                }
            }
            logger.debug("select ActivityStatusAttributeTable done");
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
        return activityStatusAttributeNameList;
    }

    //first version
    public void executeUpdate(long activityStatusId, List<StatusAttributeName> listStatusAttributeName, Connection connection) throws SQLException {
        this.executeDelete(activityStatusId, connection);
        if ((listStatusAttributeName != null) && (!listStatusAttributeName.isEmpty())) { 
            this.executeInsert(activityStatusId, listStatusAttributeName, connection);
        }
        logger.debug("update ActivityStatusAttributeTable done");
    }
        
    private void executeDelete(long activityStatusId, Connection connection) throws SQLException {  
        CallableStatement deleteCS = null;
        try {
            deleteCS = connection.prepareCall(deleteSP);
            deleteCS.setLong(1, activityStatusId);
            deleteCS.execute();
            logger.debug("delete ActivityStatusAttributeTable done");
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
    
}
