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
import org.glite.ce.creamapi.activitymanagement.db.table.RecipientTableInterface;

public class RecipientTable implements RecipientTableInterface{
    private static final Logger logger = Logger.getLogger(RecipientTable.class.getName());
    private static final String insertSP = "{call insertRecipientTable (?, ?)}";
    private static final String selectSP = "{call selectRecipientTable (?)}";
    
    public void executeInsert(long notificationId, String value, Connection connection) throws SQLException {  
        CallableStatement insertCS = null;
        try {
            insertCS = connection.prepareCall(insertSP);
            insertCS.setLong(1, notificationId);
            insertCS.setString(2, value);
            insertCS.execute();
            logger.debug("insert into RecipientTable done");
        } catch(SQLException se) {
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

    public List<String> executeSelect(long notificationId, Connection connection) throws SQLException {
        CallableStatement selectCS = null;
        List<String> recipientList = null;
        try {
            selectCS = connection.prepareCall(selectSP);
            selectCS.setLong(1, notificationId);
            if (selectCS.execute()) {
                recipientList = new ArrayList<String>(0);
                ResultSet resultSet = selectCS.getResultSet();
                while (resultSet.next()) {
                    recipientList.add(resultSet.getString(RecipientTableInterface.VALUE_LABEL));
                }
            }
            logger.debug("select RecipientTable done");
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
        return recipientList;
    }

}
