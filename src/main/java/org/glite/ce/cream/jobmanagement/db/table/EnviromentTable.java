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

package org.glite.ce.cream.jobmanagement.db.table;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Hashtable;

import org.apache.log4j.Logger;
import org.glite.ce.creamapi.jobmanagement.db.table.EnviromentTableInterface;

public class EnviromentTable implements EnviromentTableInterface{

	public final static String NAME_TABLE   = "environment";
	
	public final static String NAME_FIELD   = "name";
	public final static String VALUE_FIELD  = "value";
	public final static String JOB_ID_FIELD = "jobId";

	private final static String insertQuery = getInsertQuery();
	private final static String selectQuery = getSelectQuery();
	private final static String deleteQuery = getDeleteQuery();
	
	private final static Logger logger = Logger.getLogger(EnviromentTable.class);

	public EnviromentTable() throws SQLException {
		logger.debug("Call EnviromentTable constructor");
	}

	public int executeInsert(String jobId, Hashtable<String, String> enviroment, Connection connection) throws SQLException {
		logger.debug("Begin executeInsert");
		//logger.debug("insertQuery (EnviromentTable)= " + insertQuery);
		int rowCountTot = 0;
		PreparedStatement insertPreparedStatement = null;
		try{
		  insertPreparedStatement = connection.prepareStatement(insertQuery);
		  int rowCount    = 0;
  		  String name = null;
		  String value = null;

		  for (Enumeration<String> e = enviroment.keys(); e.hasMoreElements(); ) {
			name  = e.nextElement();
			value = enviroment.get(name);
			insertPreparedStatement = fillInsertPreparedStatement(jobId, name, value, insertPreparedStatement);
		    // execute query, and return number of rows created
		    rowCount = insertPreparedStatement.executeUpdate();
		    insertPreparedStatement.clearParameters();
		    rowCountTot += rowCount;
		  }// for
		  logger.debug("Rows inserted: " + rowCountTot); 
        } catch (SQLException sqle) {
          throw sqle;
        } finally {
            if (insertPreparedStatement != null) {
              try {
            	  insertPreparedStatement.close();
              } catch (SQLException sqle1) {
                logger.error(sqle1);
              }
            }
        }
		logger.debug("End executeInsert");
		return rowCountTot;
	}
	
	public Hashtable<String, String> executeSelect(String jobId, Connection connection) throws SQLException {
		logger.debug("Begin executeSelect");
		Hashtable<String, String> nameAndValueHash = new Hashtable<String, String>();
		//logger.debug("selectQuery (EnviromentTable)= " + selectQuery);
		PreparedStatement selectPreparedStatement = null;
		try{
		  selectPreparedStatement = connection.prepareStatement(selectQuery);
		  selectPreparedStatement = fillSelectPreparedStatement(jobId, selectPreparedStatement);
		  // execute query, and return number of rows created
		  ResultSet resultSet = selectPreparedStatement.executeQuery();
		  String name  = null;
		  String value = null;
		  while (resultSet.next()) {
			   name  = resultSet.getString(EnviromentTable.NAME_FIELD);
		       value = resultSet.getString(EnviromentTable.VALUE_FIELD);
		       nameAndValueHash.put(name, value);
	      }
       } catch (SQLException sqle) {
         throw sqle;
       } finally {
          if (selectPreparedStatement != null) {
            try {
            	selectPreparedStatement.close();
            } catch (SQLException sqle1) {
              logger.error(sqle1);
            }
          }
       }
	   logger.debug("End executeSelect");
	     return nameAndValueHash;
    }
	
	public int executeDelete(String jobId, Connection connection) throws SQLException {
		logger.debug("Begin executeDelete");
		//logger.debug("deleteQuery (EnviromentTable)= " + deleteQuery);
		PreparedStatement deletePreparedStatement = null;
		int rowCount    = 0;
		try{
		  deletePreparedStatement = connection.prepareStatement(deleteQuery);
		  deletePreparedStatement = fillDeletePreparedStatement(jobId, deletePreparedStatement);
		  // execute query, and return number of rows created
		  rowCount = deletePreparedStatement.executeUpdate();
        } catch (SQLException sqle) {
          throw sqle;
        } finally {
           if (deletePreparedStatement != null) {
             try {
            	 deletePreparedStatement.close();
             } catch (SQLException sqle1) {
               logger.error(sqle1);
             }
           }
          }
		  logger.debug("End executeDelete");
		  return rowCount;
	}
	
	private PreparedStatement fillInsertPreparedStatement(
			String jobId, String name, String value, PreparedStatement insertPreparedStatement) throws SQLException {
		insertPreparedStatement.setString(1, name);
		insertPreparedStatement.setString(2, value);
		insertPreparedStatement.setString(3, jobId);
		return insertPreparedStatement;
	}
	
	private PreparedStatement fillDeletePreparedStatement(
			String jobId, PreparedStatement deletePreparedStatement) throws SQLException {
		deletePreparedStatement.setString(1, jobId);
		return deletePreparedStatement;
	}
	
	private PreparedStatement fillSelectPreparedStatement(
			String jobId, PreparedStatement selectPreparedStatement) throws SQLException {
		selectPreparedStatement.setString(1, jobId);
		return selectPreparedStatement;
	}
	
	private static String getInsertQuery(){
		  StringBuffer insertQuery = new StringBuffer();
		  insertQuery.append("insert into ");
		  insertQuery.append(EnviromentTable.NAME_TABLE);
		  insertQuery.append(" ( ");
		  insertQuery.append(EnviromentTable.NAME_FIELD + ", ");
		  insertQuery.append(EnviromentTable.VALUE_FIELD + ", ");
		  insertQuery.append(EnviromentTable.JOB_ID_FIELD);
		  insertQuery.append(" ) ");
		  insertQuery.append("values(?, ?, ?)");  
		  return insertQuery.toString();
	}

	private static String getDeleteQuery(){
		  StringBuffer deleteQuery = new StringBuffer();
		  deleteQuery.append("delete from ");
		  deleteQuery.append(EnviromentTable.NAME_TABLE);
		  deleteQuery.append(" where ");
		  deleteQuery.append(EnviromentTable.JOB_ID_FIELD + " = ?");
		  return deleteQuery.toString();
	}
	
	private static String getSelectQuery(){
		  StringBuffer selectQuery = new StringBuffer();
		  selectQuery.append("select ");
		  selectQuery.append(EnviromentTable.NAME_FIELD + ", ");
		  selectQuery.append(EnviromentTable.VALUE_FIELD);
		  selectQuery.append(" from " + EnviromentTable.NAME_TABLE);
		  selectQuery.append(" where ");
		  selectQuery.append(ArgumentTable.JOB_ID_FIELD + " = ?");
		  return selectQuery.toString();
	}
}
