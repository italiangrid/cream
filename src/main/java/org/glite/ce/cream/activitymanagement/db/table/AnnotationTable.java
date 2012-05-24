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
import org.glite.ce.creamapi.activitymanagement.db.table.AnnotationTableInterface;

public class AnnotationTable implements AnnotationTableInterface {
    private static final Logger logger = Logger.getLogger(AnnotationTable.class.getName());
    private static final String insertSP = "{call insertAnnotationTable (?, ?)}";
    private static final String selectSP = "{call selectAnnotationTable (?)}";
            
    public void executeInsert(String activityId, String value, Connection connection) throws SQLException {  
        CallableStatement insertCS = null;
        try {
            insertCS = connection.prepareCall(insertSP);
            insertCS.setString(1, activityId);
            insertCS.setString(2, value);
            insertCS.execute();
            logger.debug("insert into AnnotationTable done");
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

    public List<String> executeSelect(String activityId, Connection connection) throws SQLException {  
        CallableStatement selectCS = null;
        List<String> annotation = null;
        try {
            selectCS = connection.prepareCall(selectSP);
            selectCS.setString(1, activityId);
            if (selectCS.execute()) {
                annotation = new ArrayList<String>(0);
                ResultSet resultSet = selectCS.getResultSet();
                while (resultSet.next()) {
                    annotation.add(resultSet.getString(AnnotationTableInterface.VALUE_LABEL));
                }
            }
            logger.debug("select AnnotationTable done");
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
        return annotation;
    }
}
