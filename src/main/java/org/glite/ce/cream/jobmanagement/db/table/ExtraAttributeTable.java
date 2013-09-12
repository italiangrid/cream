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
import org.glite.ce.creamapi.jobmanagement.db.table.ExtraAttributeTableInterface;

public class ExtraAttributeTable implements ExtraAttributeTableInterface {

    public final static String NAME_TABLE = "extra_attribute";
    public final static String NAME_FIELD = "name";
    public final static String VALUE_FIELD = "value";
    public final static String JOB_ID_FIELD = "jobId";

    private final static String insertQuery = getInsertQuery();
    private final static String selectQuery = getSelectQuery();
    private final static String deleteQuery = getDeleteQuery();

    private final static Logger logger = Logger.getLogger(ExtraAttributeTable.class);

    public ExtraAttributeTable() throws SQLException {
        logger.debug("Call ExtraAttributeTable constructor");
    }

    public int executeInsert(String jobId, Hashtable<String, String> extraAttribute, Connection connection) throws SQLException {
        logger.debug("Begin executeInsert");
        // logger.debug("insertQuery (EnviromentTable)= " + insertQuery);
        int rowCountTot = 0;
        PreparedStatement insertPreparedStatement = null;
        try {
            insertPreparedStatement = connection.prepareStatement(insertQuery);
            int rowCount = 0;
            String name = null;
            String value = null;

            for (Enumeration<String> e = extraAttribute.keys(); e.hasMoreElements();) {
                name = e.nextElement();
                value = extraAttribute.get(name);
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

    public int executeInsert(String jobId, String name, String value, Connection connection) throws SQLException {
        logger.debug("Begin executeInsert");
        // logger.debug("insertQuery (EnviromentTable)= " + insertQuery);
        int rowCount = 0;
        PreparedStatement insertPreparedStatement = null;
        try {
            insertPreparedStatement = connection.prepareStatement(insertQuery);
            insertPreparedStatement = fillInsertPreparedStatement(jobId, name, value, insertPreparedStatement);
            // execute query, and return number of rows created
            rowCount = insertPreparedStatement.executeUpdate();
            insertPreparedStatement.clearParameters();
            logger.debug("Rows inserted: " + rowCount);
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
        return rowCount;
    }

    public Hashtable<String, String> executeSelect(String jobId, Connection connection) throws SQLException {
        logger.debug("Begin executeSelect");
        Hashtable<String, String> extraAttribute = new Hashtable<String, String>(0);
        // logger.debug("selectQuery (ExtraAttributeTable)= " + selectQuery);
        PreparedStatement selectPreparedStatement = null;
        try {
            selectPreparedStatement = connection.prepareStatement(selectQuery);
            selectPreparedStatement = fillSelectPreparedStatement(jobId, selectPreparedStatement);
            // execute query, and return number of rows created
            ResultSet resultSet = selectPreparedStatement.executeQuery();
            String name = null;
            String value = null;
            while (resultSet.next()) {
                name = resultSet.getString(ExtraAttributeTable.NAME_FIELD);
                value = resultSet.getString(ExtraAttributeTable.VALUE_FIELD);
                extraAttribute.put(name, value);
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
        return extraAttribute;
    }

    public int executeDelete(String jobId, Connection connection) throws SQLException {
        logger.debug("Begin executeDelete");
        // logger.debug("deleteQuery (EnviromentTable)= " + deleteQuery);
        PreparedStatement deletePreparedStatement = null;
        int rowCount = 0;
        try {
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

    public void executeUpdate(String jobId, Hashtable<String, String> extraAttribute, Connection connection) throws SQLException {
        logger.debug("Begin executeUpdate");
        PreparedStatement updatePreparedStatement = null;
        try {
            updatePreparedStatement = connection.prepareStatement(getUpdateQuery());
            String name = null;
            String value = null;
            for (Enumeration<String> e = extraAttribute.keys(); e.hasMoreElements();) {
                updatePreparedStatement.clearParameters();
                name = e.nextElement();
                value = extraAttribute.get(name);
                updatePreparedStatement = fillUpdatePreparedStatement(jobId, name, value, updatePreparedStatement);
                updatePreparedStatement.executeUpdate();
            }
        } catch (SQLException sqle) {
            throw sqle;
        } finally {
            if (updatePreparedStatement != null) {
                try {
                    updatePreparedStatement.close();
                } catch (SQLException sqle1) {
                    logger.error(sqle1);
                }
            }
        }
        logger.debug("End executeUpdate");
    }

    public int executeUpdate(String jobId, String name, String value, Connection connection) throws SQLException {
        logger.debug("Begin executeUpdate");
        int rowUpdated = 0;
        PreparedStatement updatePreparedStatement = null;
        try {
            updatePreparedStatement = connection.prepareStatement(getUpdateQuery());
            updatePreparedStatement.clearParameters();
            updatePreparedStatement = fillUpdatePreparedStatement(jobId, name, value, updatePreparedStatement);
            rowUpdated = updatePreparedStatement.executeUpdate();
        } catch (SQLException sqle) {
            throw sqle;
        } finally {
            if (updatePreparedStatement != null) {
                try {
                    updatePreparedStatement.close();
                } catch (SQLException sqle1) {
                    logger.error(sqle1);
                }
            }
        }
        logger.debug("End executeUpdate");
        return rowUpdated;
    }

    private PreparedStatement fillInsertPreparedStatement(String jobId, String name, String value, PreparedStatement insertPreparedStatement) throws SQLException {
        insertPreparedStatement.setString(1, name);
        insertPreparedStatement.setString(2, value);
        insertPreparedStatement.setString(3, jobId);
        return insertPreparedStatement;
    }

    private PreparedStatement fillDeletePreparedStatement(String jobId, PreparedStatement deletePreparedStatement) throws SQLException {
        deletePreparedStatement.setString(1, jobId);
        return deletePreparedStatement;
    }

    private PreparedStatement fillSelectPreparedStatement(String jobId, PreparedStatement selectPreparedStatement) throws SQLException {
        selectPreparedStatement.setString(1, jobId);
        return selectPreparedStatement;
    }

    private PreparedStatement fillUpdatePreparedStatement(String jobId, String attributeName, String attributeValue, PreparedStatement updatePreparedStatement) throws SQLException {
        updatePreparedStatement.setString(1, attributeValue);
        updatePreparedStatement.setString(2, jobId);
        updatePreparedStatement.setString(3, attributeName);
        return updatePreparedStatement;
    }

    private static String getInsertQuery() {
        StringBuffer insertQuery = new StringBuffer();
        insertQuery.append("insert into ");
        insertQuery.append(ExtraAttributeTable.NAME_TABLE);
        insertQuery.append(" ( ");
        insertQuery.append(ExtraAttributeTable.NAME_FIELD + ", ");
        insertQuery.append(ExtraAttributeTable.VALUE_FIELD + ", ");
        insertQuery.append(ExtraAttributeTable.JOB_ID_FIELD);
        insertQuery.append(" ) values(?, ?, ?)");
        return insertQuery.toString();
    }

    private static String getDeleteQuery() {
        StringBuffer deleteQuery = new StringBuffer();
        deleteQuery.append("delete from ");
        deleteQuery.append(ExtraAttributeTable.NAME_TABLE);
        deleteQuery.append(" where ");
        deleteQuery.append(ExtraAttributeTable.JOB_ID_FIELD + "=?");
        return deleteQuery.toString();
    }

    private static String getSelectQuery() {
        StringBuffer selectQuery = new StringBuffer();
        selectQuery.append("select ");
        selectQuery.append(ExtraAttributeTable.NAME_FIELD + ", ");
        selectQuery.append(ExtraAttributeTable.VALUE_FIELD);
        selectQuery.append(" from " + ExtraAttributeTable.NAME_TABLE);
        selectQuery.append(" where ");
        selectQuery.append(ArgumentTable.JOB_ID_FIELD + "=?");
        return selectQuery.toString();
    }

    private static String getUpdateQuery() {
        StringBuffer queryUpdateJobStatusTable = new StringBuffer();
        queryUpdateJobStatusTable.append("update ");
        queryUpdateJobStatusTable.append(ExtraAttributeTable.NAME_TABLE);
        queryUpdateJobStatusTable.append(" set " + ExtraAttributeTable.VALUE_FIELD + "=?");
        queryUpdateJobStatusTable.append(" where ");
        queryUpdateJobStatusTable.append(ExtraAttributeTable.JOB_ID_FIELD + "=?");
        queryUpdateJobStatusTable.append(" and " + ExtraAttributeTable.NAME_FIELD + "=?");
        return queryUpdateJobStatusTable.toString();
    }
}
