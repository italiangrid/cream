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
import org.glite.ce.creamapi.activitymanagement.db.table.ActivityExecutableTypeTableInterface;

public class ActivityExecutableTypeTable implements ActivityExecutableTypeTableInterface {
    
    private static final Logger logger = Logger.getLogger(ActivityExecutableTypeTable.class.getName());
    private static final String insertSP = "{call insertActivityExecutableTypeTable (?, ?, ?)}";
    private static final String selectSP = "{call selectActivityExecutableTypeTable (?, ?)}";
    
    public void executeInsert(String activityId, long executableTypeId, String type, Connection connection) throws SQLException {  
        CallableStatement insertCS = null;
        try {
            insertCS = connection.prepareCall(insertSP);
            insertCS.setString(1, activityId);
            insertCS.setLong(2, executableTypeId);
            insertCS.setString(3, type);
            insertCS.execute();
            logger.debug("insert into ActivityExecutableTypeTable done");
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

    public List<Long> executeSelect(String activityId, String type, Connection connection) throws SQLException {
        CallableStatement selectCS = null;
        List<Long> executableTypeIdList = null;
        try {
            selectCS = connection.prepareCall(selectSP);
            selectCS.setString(1, activityId);
            selectCS.setString(2, type);
            if (selectCS.execute()) {
                executableTypeIdList = new ArrayList<Long>(0);
                ResultSet resultSet = selectCS.getResultSet();
                while (resultSet.next()) {
                   executableTypeIdList.add(resultSet.getLong(ActivityExecutableTypeTableInterface.EXECUTABLETYPE_ID_LABEL));
                }
            }
            logger.debug("select ActivityExecutableTypeTable done");
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
        return executableTypeIdList;
    }
}
