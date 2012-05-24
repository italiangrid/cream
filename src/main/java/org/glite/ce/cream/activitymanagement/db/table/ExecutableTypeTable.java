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

import java.math.BigInteger;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.log4j.Logger;
import org.glite.ce.creamapi.activitymanagement.db.table.ExecutableTypeTableInterface;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.ExecutableType;

public class ExecutableTypeTable implements ExecutableTypeTableInterface {
    
    private static final Logger logger = Logger.getLogger(ExecutableTypeTable.class.getName());
    private static final String insertSP = "{call insertExecutableTypeTable (?, ?, ?)}";
    private static final String selectSP = "{call selectExecutableTypeTable (?)}";
    
    public long executeInsert(ExecutableType executableType, Connection connection) throws SQLException {
        CallableStatement insertCS = null;
        long executableTypeId = 0;
        try {
            insertCS = connection.prepareCall(insertSP);
            insertCS.setString(1, executableType.getPath());
            if (executableType.getFailIfExitCodeNotEqualTo() != null) {
                insertCS.setString(2, executableType.getFailIfExitCodeNotEqualTo().longValue() + "");
            } else {
                insertCS.setString(2, null);
            }
            insertCS.registerOutParameter(3, Types.BIGINT);
            insertCS.execute();
            executableTypeId = insertCS.getLong(3);
            logger.debug("insert into ExecutableTypeTable done");
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
        return executableTypeId;
    }

    public ExecutableType executeSelect(long executableTypeId, Connection connection) throws SQLException {
        CallableStatement selectCS = null;
        ExecutableType executableType = null;
        try {
            selectCS = connection.prepareCall(selectSP);
            selectCS.setLong(1, executableTypeId);
            if (selectCS.execute()) {
                executableType = new ExecutableType();
                ResultSet resultSet = selectCS.getResultSet();
                while (resultSet.next()) {
                    executableType.setPath(resultSet.getString(ExecutableTypeTableInterface.PATH_LABEL));
                    executableType.setFailIfExitCodeNotEqualTo(BigInteger.valueOf(resultSet.getLong(ExecutableTypeTableInterface.FAILIFEXITCODENOTEQUALTO_LABEL)));
                    if (resultSet.getString(ExecutableTypeTableInterface.FAILIFEXITCODENOTEQUALTO_LABEL) != null) {
                        try {
                            long failIfExitCodeNotEqualToField = Long.parseLong(resultSet.getString(ExecutableTypeTableInterface.FAILIFEXITCODENOTEQUALTO_LABEL));
                            executableType.setFailIfExitCodeNotEqualTo(BigInteger.valueOf(failIfExitCodeNotEqualToField));
                        } catch (NumberFormatException nfe) {
                            logger.error("FailIfExitCodeNotEqualTo field must be integer");
                            throw new SQLException("FailIfExitCodeNotEqualTo field must be integer");
                        }
                    } else {
                        executableType.setFailIfExitCodeNotEqualTo(null);
                    }
                }
            }
            logger.debug("select ExecutableTypeTable done");
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
        return executableType;
    }

}

