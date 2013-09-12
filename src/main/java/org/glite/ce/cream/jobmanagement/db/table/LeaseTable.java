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
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.log4j.Logger;
import org.glite.ce.creamapi.jobmanagement.Lease;
import org.glite.ce.creamapi.jobmanagement.db.table.LeaseTableInterface;

public class LeaseTable implements LeaseTableInterface {

    public final static String NAME_TABLE = "job_lease";
    public final static String LEASE_ID_FIELD = "leaseId";
    public final static String LEASE_TIME_FIELD = "leaseTime";
    public final static String USER_ID_FIELD = "userId";

    private final static String insertQuery = getInsertQuery();
    private final static String updateQuery = getUpdateQuery();
    private final static String deleteQuery = getDeleteQuery();

    private final static Logger logger = Logger.getLogger(LeaseTable.class);

    public LeaseTable() throws SQLException {
        logger.debug("Call LeaseTable constructor");
    }

    public int executeDelete(String leaseId, String userId, Connection connection) throws SQLException {
        logger.debug("Begin executeDelete for leaseId = " + leaseId);
        // logger.debug("deleteQuery (LeaseTable)= " + deleteQuery);
        PreparedStatement deletePreparedStatement = null;
        int rowCount = 0;
        try {
            deletePreparedStatement = connection.prepareStatement(deleteQuery);
            deletePreparedStatement = fillDeletePreparedStatement(leaseId, userId, deletePreparedStatement);
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
        logger.debug("End executeDelete for leaseId = " + leaseId);
        return rowCount;
    }

    public int executeUpdate(Lease jobLease, Connection connection) throws SQLException {
        logger.debug("Begin executeUpdate");
        logger.debug("updateQuery (LeaseTable)= " + updateQuery);
        PreparedStatement updatePreparedStatement = null;
        int rowCount = 0;
        try {
            updatePreparedStatement = connection.prepareStatement(updateQuery);
            updatePreparedStatement = fillUpdatePreparedStatement(jobLease, updatePreparedStatement);
            // execute query, and return number of rows created
            rowCount = updatePreparedStatement.executeUpdate();
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
        return rowCount;
    }

    private PreparedStatement fillDeletePreparedStatement(String leaseId, String userId, PreparedStatement deletePreparedStatement) throws SQLException {
        deletePreparedStatement.setString(1, leaseId);
        deletePreparedStatement.setString(2, userId);
        return deletePreparedStatement;
    }

    public int executeInsert(Lease jobLease, Connection connection) throws SQLException {
        logger.debug("Begin executeInsert");
        // logger.debug("insertQuery (LeaseTable)= " + insertQuery);
        PreparedStatement insertPreparedStatement = null;
        int rowCount = 0;
        // only for debug
        if (jobLease != null) {
            logger.debug("UserId    = " + jobLease.getUserId());
            logger.debug("LeaseId   = " + jobLease.getLeaseId());
            logger.debug("LeaseTime = " + jobLease.getLeaseTime());
        }

        try {
            insertPreparedStatement = connection.prepareStatement(insertQuery);
            insertPreparedStatement = fillInsertPreparedStatement(jobLease, insertPreparedStatement);
            logger.debug("Insert query = " + insertPreparedStatement.toString());
            // execute query, and return number of rows created
            rowCount = insertPreparedStatement.executeUpdate();
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

    private PreparedStatement fillInsertPreparedStatement(Lease jobLease, PreparedStatement insertPreparedStatement) throws SQLException {
        insertPreparedStatement.setString(1, jobLease.getLeaseId());
        insertPreparedStatement.setTimestamp(2, new java.sql.Timestamp(jobLease.getLeaseTime().getTimeInMillis()));
        insertPreparedStatement.setString(3, jobLease.getUserId());
        return insertPreparedStatement;
    }

    private PreparedStatement fillUpdatePreparedStatement(Lease jobLease, PreparedStatement updatePreparedStatement) throws SQLException {
        updatePreparedStatement.setTimestamp(1, new java.sql.Timestamp(jobLease.getLeaseTime().getTimeInMillis()));
        updatePreparedStatement.setString(2, jobLease.getUserId());
        updatePreparedStatement.setString(3, jobLease.getLeaseId());
        return updatePreparedStatement;
    }

    public List<Lease> executeSelect(String leaseId, String userId, Calendar leaseTime, Connection connection) throws SQLException {
        logger.debug("Begin executeSelect");
        List<Lease> leaseList = new ArrayList<Lease>(0);
        Lease lease = null;
        int index = 0;

        PreparedStatement selectPreparedStatement = connection.prepareStatement(getSelectQuery(leaseTime, leaseId, userId));

        if (userId != null) {
            selectPreparedStatement.setString(++index, userId);
        }

        if (leaseId != null) {
            selectPreparedStatement.setString(++index, leaseId);
        }

        if (leaseTime != null) {
            selectPreparedStatement.setTimestamp(++index, new java.sql.Timestamp(leaseTime.getTimeInMillis()));
        }

        // execute query, and return number of rows created
        ResultSet rs = selectPreparedStatement.executeQuery();

        if (rs != null) {
            while (rs.next()) {
                lease = new Lease();
                lease.setLeaseId(rs.getString(LeaseTable.LEASE_ID_FIELD));
                lease.setUserId(rs.getString(LeaseTable.USER_ID_FIELD));

                Timestamp timeStampField = rs.getTimestamp(LeaseTable.LEASE_TIME_FIELD);
                Calendar calendar = null;
                calendar = Calendar.getInstance();
                calendar.setTimeInMillis(timeStampField.getTime());

                lease.setLeaseTime(calendar);
                leaseList.add(lease);
            }
        }

        logger.debug("End executeSelect");
        return leaseList;
    }

    private static String getInsertQuery() {
        StringBuffer insertQuery = new StringBuffer();
        insertQuery.append("insert into ");
        insertQuery.append(LeaseTable.NAME_TABLE);
        insertQuery.append(" ( ");
        insertQuery.append(LeaseTable.LEASE_ID_FIELD + ", ");
        insertQuery.append(LeaseTable.LEASE_TIME_FIELD + ", ");
        insertQuery.append(LeaseTable.USER_ID_FIELD);
        insertQuery.append(" ) values(?, ?, ?)");

        return insertQuery.toString();
    }

    private static String getDeleteQuery() {
        StringBuffer deleteQuery = new StringBuffer();
        deleteQuery.append("delete from ");
        deleteQuery.append(LeaseTable.NAME_TABLE);
        deleteQuery.append(" where ");
        deleteQuery.append(LeaseTable.LEASE_ID_FIELD + "=?");
        deleteQuery.append(" and " + LeaseTable.USER_ID_FIELD + "=?");
        return deleteQuery.toString();
    }

    private static String getUpdateQuery() {
        StringBuffer deleteQuery = new StringBuffer();
        deleteQuery.append("update ");
        deleteQuery.append(LeaseTable.NAME_TABLE);
        deleteQuery.append(" set " + LeaseTable.LEASE_TIME_FIELD + "=?");
        deleteQuery.append(" where ");
        deleteQuery.append(LeaseTable.USER_ID_FIELD + "=?");
        deleteQuery.append(" and " + LeaseTable.LEASE_ID_FIELD + "=?");
        return deleteQuery.toString();
    }

    private static String getSelectQuery(Calendar leaseTime, String leaseId, String userId) {
        StringBuffer selectQuery = new StringBuffer();
        selectQuery.append("select ");
        selectQuery.append(LeaseTable.LEASE_ID_FIELD + ", ");
        selectQuery.append(LeaseTable.USER_ID_FIELD + ", ");
        selectQuery.append(LeaseTable.LEASE_TIME_FIELD);
        selectQuery.append(" from " + LeaseTable.NAME_TABLE);
        selectQuery.append(" where true");

        if (userId != null) {
            selectQuery.append(" and " + LeaseTable.USER_ID_FIELD + "=?");
        }

        if (leaseId != null) {
            selectQuery.append(" and " + LeaseTable.LEASE_ID_FIELD + "=?");
        }

        if (leaseTime != null) {
            selectQuery.append(" and " + LeaseTable.LEASE_TIME_FIELD + "<=?");
        }

        return selectQuery.toString();
    }
}
