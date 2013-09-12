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
 * Authors: Luigi Zangrando <zangrando@pd.infn.it>
 *
 */

package org.glite.ce.cream.delegationmanagement;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;
import org.glite.ce.commonj.db.DatabaseException;
import org.glite.ce.commonj.db.DatasourceManager;
import org.glite.ce.commonj.utils.CEUtils;
import org.glite.ce.creamapi.jobmanagement.db.DBInfoManager;
import org.glite.ce.creamapi.cmdmanagement.queue.CommandQueueException;
import org.glite.ce.creamapi.delegationmanagement.Delegation;
import org.glite.ce.creamapi.delegationmanagement.DelegationException;
import org.glite.ce.creamapi.delegationmanagement.DelegationManagerException;
import org.glite.ce.creamapi.delegationmanagement.DelegationManagerInterface;
import org.glite.ce.creamapi.delegationmanagement.DelegationRequest;

public class DelegationManager implements DelegationManagerInterface {
    private static final Logger logger = Logger.getLogger(DelegationManager.class.getName());
    private static final String DELEGATION_TABLE = "delegation";
    private static final String DELEGATION_REQUEST_TABLE = "delegation_request";
    private static final String ID_FIELD = "id";
    private static final String DN_FIELD = "dn";
    private static final String VO_FIELD = "vo";
    private static final String FQAN_FIELD = "fqan";
    private static final String INFO_FIELD = "info";
    private static final String VOMS_ATTRIBUTE_FIELD = "vomsAttribute";
    private static final String LOCAL_USER_FIELD = "localUser";
    private static final String LOCAL_USER_GROUP_FIELD = "localUserGroup";
    private static final String CERTIFICATE_FIELD = "certificate";
    private static final String CERTIFICATE_REQUEST_FIELD = "certificateRequest";
    private static final String START_TIME_FIELD = "startTime";
    private static final String EXPIRATION_TIME_FIELD = "expirationTime";
    private static final String LAST_UPDATE_TIME_FIELD = "lastUpdateTime";
    private static final String TIMESTAMP_FIELD = "timestamp";
    private static final String PUBLIC_KEY_FIELD = "publicKey";
    private static final String PRIVATE_KEY_FIELD = "privateKey";
    private static boolean terminated = false;
    private static DelegationManager delegationManager = null;

    public static DelegationManager getInstance() throws DelegationManagerException {    
        if (terminated) {
            throw new DelegationManagerException("DelegationManager already terminated!");
        }

        if (delegationManager == null) {
            delegationManager = new DelegationManager();
            delegationManager.init();
        }

        return delegationManager;
    }

    public static void main(String[] args) {
        try {
            DelegationManager dm = DelegationManager.getInstance();
            DelegationRequest req = new DelegationRequest("delegId2");

            // dm.deleteDelegationTable();
            // dm.createDelegationTable();

            ArrayList<String> vomsAttr = new ArrayList<String>(2);
            vomsAttr.add("attr1");
            vomsAttr.add("attr2");

            req.setDN("/C=IT/O=INFN/OU=Personal Certificate/L=Padova/CN=Luigi Zangrando");
            req.setCertificateRequest("certificateRequest");
            req.setPrivateKey("primaryKey");
            req.setVOMSAttributes(vomsAttr);
            req.setLocalUser("lisa");

            // dm.deleteDelegationRequest("delegId",
            // "/C=IT/O=INFN/OU=Personal Certificate/L=Padova/CN=Luigi Zangrando",
            // "lisa");
            // dm.insertDelegationRequest(req);
            // DelegationRequest req2 = dm.getDelegationRequest("delegId",
            // "/C=IT/O=INFN/OU=Personal Certificate/L=Padova/CN=Luigi Zangrando",
            // "lisa");
            // System.out.println(req2.getId());
            // System.out.println(req2.getDN());
            // System.out.println(req2.getPrimaryKey());
            // System.out.println(req2.getCertificateRequest());
            // System.out.println(req2.getLocalUser());
            // System.out.println(req2.getVOMSAttributes().size());

            //
            // for(DelegationRequest dreq :
            // dm.listDelegationRequests("/C=IT/O=INFN/OU=Personal Certificate/L=Padova/CN=Luigi Zangrando",
            // "lisa")) {
            // System.out.println(dreq.getId());
            // System.out.println(dreq.getDN());
            // System.out.println(dreq.getPrimaryKey());
            // System.out.println(dreq.getCertificateRequest());
            // System.out.println(dreq.getLocalUser());
            // System.out.println(dreq.getVOMSAttributes().size());
            // }

            Delegation deleg = new Delegation("delegId2");
            deleg.setCertificate("certificate");
            deleg.setDN("/C=IT/O=INFN/OU=Personal Certificate/L=Padova/CN=Luigi Zangrando");
            deleg.setExpirationTime(Calendar.getInstance().getTime());
            deleg.setStartTime(Calendar.getInstance().getTime());
            deleg.setFQAN("fqan");
            deleg.setLocalUser("lisa");
            deleg.setLocalUserGroup("lisa group");
            deleg.setInfo("CIAO");
            deleg.setVO("VO");
            deleg.setVOMSAttributes(vomsAttr);

            // dm.insertDelegation(deleg);

            Delegation dd = dm.getDelegation("delegId2", "/C=IT/O=INFN/OU=Personal Certificate/L=Padova/CN=Luigi Zangrando", "lisa");
            System.out.println(dd.getId());
            System.out.println(dd.getDN());
            System.out.println(dd.getInfo());
            System.out.println(dd.getFQAN());
            System.out.println(dd.getLocalUser());
            System.out.println(dd.getLocalUserGroup());
            System.out.println(dd.getStartTime());
            System.out.println(dd.getExpirationTime());
            System.out.println(dd.getLastUpdateTime());
            System.out.println(dd.getVO());
            System.out.println(dd.getVOMSAttributes().size());

            // dm.deleteDelegation(deleg);

            // for(Delegation d : dm.getExpiredDelegations()) {
            // System.out.println(d.getId());
            // System.out.println(d.getDN());
            // System.out.println(d.getInfo());
            // System.out.println(d.getFQAN());
            // System.out.println(d.getLocalUser());
            // System.out.println(d.getLocalUserGroup());
            // System.out.println(d.getStartTime());
            // System.out.println(d.getExpirationTime());
            // System.out.println(d.getLastUpdateTime());
            // System.out.println(d.getVO());
            // System.out.println(d.getVOMSAttributes().size());
            //
            // dm.deleteDelegation(deleg);
            // }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * Creates the queue table.
     * 
     * @throws CommandQueueException
     */
    private void createDelegationTable() throws DelegationManagerException {
        logger.debug("BEGIN createDelegationTable");

        PreparedStatement pstmt = null;
        Connection connection = null;

        try {
            int result = 0;
            connection = getConnection();

            StringBuffer query = new StringBuffer("create table if not exists ");
            query.append(DELEGATION_TABLE).append(" (");
            query.append(ID_FIELD).append(" VARCHAR(255) NOT NULL, ");
            query.append(DN_FIELD).append(" VARCHAR(255) NOT NULL, ");
            query.append(FQAN_FIELD).append(" VARCHAR(255) NULL, ");
            query.append(VO_FIELD).append(" VARCHAR(50) NOT NULL, ");
            query.append(VOMS_ATTRIBUTE_FIELD).append(" TEXT NOT NULL, ");
            query.append(CERTIFICATE_FIELD).append(" TEXT NOT NULL, ");
            query.append(INFO_FIELD).append(" TEXT NULL, ");
            query.append(LOCAL_USER_FIELD).append(" VARCHAR(100) NOT NULL, ");
            query.append(LOCAL_USER_GROUP_FIELD).append(" VARCHAR(100) NOT NULL, ");
            query.append(START_TIME_FIELD).append(" DATETIME NOT NULL, ");
            query.append(EXPIRATION_TIME_FIELD).append(" DATETIME NOT NULL, ");
            query.append(LAST_UPDATE_TIME_FIELD).append(" DATETIME NULL, primary key (");
            query.append(ID_FIELD).append(", ").append(DN_FIELD).append(")) engine=InnoDB");

            pstmt = connection.prepareStatement(query.toString());
            result = pstmt.executeUpdate();

            if (result > 0) {
                logger.info(DELEGATION_TABLE + " table created");
            }
            
            query = new StringBuffer("create table if not exists ");
            query.append(DELEGATION_REQUEST_TABLE).append(" (");
            query.append(ID_FIELD).append(" VARCHAR(255) NOT NULL, ");
            query.append(DN_FIELD).append(" VARCHAR(255) NOT NULL, ");
            query.append(LOCAL_USER_FIELD).append(" VARCHAR(100) NOT NULL, ");
            query.append(CERTIFICATE_REQUEST_FIELD).append(" TEXT NOT NULL, ");
            query.append(PUBLIC_KEY_FIELD).append(" TEXT NOT NULL, ");
            query.append(PRIVATE_KEY_FIELD).append(" TEXT NOT NULL, ");
            query.append(TIMESTAMP_FIELD).append(" DATETIME NOT NULL, ");
            query.append(VOMS_ATTRIBUTE_FIELD).append(" TEXT NOT NULL, primary key ( ");
            query.append(ID_FIELD).append(", ").append(DN_FIELD).append(")) engine=InnoDB");

            pstmt = connection.prepareStatement(query.toString());
            result = pstmt.executeUpdate();
            
            if (result > 0) {
                logger.info(DELEGATION_REQUEST_TABLE + " table created");
            }

            // Commit
            connection.commit();
        } catch (SQLException sqle) {
            String rollbackMessage = null;

            if (connection != null) {
                try {
                    connection.rollback();
                    rollbackMessage = " (rollback performed)";
                } catch (SQLException sqle1) {
                    rollbackMessage = " (rollback failed: " + sqle1.getMessage() + ")";
                }
            }

            logger.error("createDelegationTable failed: cannot create the " + DELEGATION_TABLE + " table: " + sqle.getMessage() + (rollbackMessage != null ? rollbackMessage : ""));
            throw new DelegationManagerException("Cannot create the " + DELEGATION_TABLE + " table: " + sqle.getMessage() + (rollbackMessage != null ? rollbackMessage : ""));
        } finally {
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (SQLException sqle1) {
                    logger.error(sqle1.getMessage());
                }
            }

            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle2) {
                    logger.error("Problem in closing connection: " + sqle2.getMessage());
                    throw new DelegationManagerException("Problem in closing connection: " + sqle2.getMessage());
                }
            }
        }
        logger.debug("END createDelegationTable");
    }

    public void delete(Delegation delegation) throws DelegationException, DelegationManagerException {
        if (delegation == null) {
            throw new DelegationException("delegation not specified!");
        }

        if (delegation.getId() == null || delegation.getId().length() == 0) {
            throw new DelegationException("delegationId not specified!");
        }

        if (delegation.getDN() == null || delegation.getDN().length() == 0) {
            throw new DelegationException("user DN not specified!");
        }

        if (delegation.getLocalUser() == null || delegation.getLocalUser().length() == 0) {
            throw new DelegationException("localUser not specified!");
        }

        logger.debug("BEGIN deleteDelegation for delegationId=" + delegation.getId() + " dn=" + delegation.getDN() + " localUser=" + delegation.getLocalUser());

        Connection connection = getConnection();

        PreparedStatement deletePreparedStatement = null;

        try {
            StringBuffer query = new StringBuffer("delete from ");
            query.append(DELEGATION_TABLE).append(" where ");
            query.append(ID_FIELD).append(" = ? and ");
            query.append(DN_FIELD).append(" = ? and ");
            query.append(LOCAL_USER_FIELD).append(" = ?");

            deletePreparedStatement = connection.prepareStatement(query.toString());
            deletePreparedStatement.setString(1, delegation.getId());
            deletePreparedStatement.setString(2, delegation.getDN());
            deletePreparedStatement.setString(3, delegation.getLocalUser());
            deletePreparedStatement.executeUpdate();

            query = new StringBuffer("delete from ");
            query.append(DELEGATION_REQUEST_TABLE).append(" where ");
            query.append(ID_FIELD).append(" = ? and ");
            query.append(DN_FIELD).append(" = ? and ");
            query.append(LOCAL_USER_FIELD).append(" = ?");

            deletePreparedStatement = connection.prepareStatement(query.toString());
            deletePreparedStatement.setString(1, delegation.getId());
            deletePreparedStatement.setString(2, delegation.getDN());
            deletePreparedStatement.setString(3, delegation.getLocalUser());
            deletePreparedStatement.executeUpdate();

            connection.commit();
        } catch (SQLException sqle) {
            logger.error("Failure on db interaction", sqle);
            try {
                connection.rollback();
            } catch (SQLException e) {
                logger.error(e);
            }
            throw new DelegationException("Failure on db interaction: " + sqle.getMessage());
        } finally {
            if (deletePreparedStatement != null) {
                try {
                    deletePreparedStatement.close();
                } catch (SQLException sqle1) {
                    logger.error(sqle1);
                }
            }

            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    logger.error(e);
                }
            }
        }

        logger.debug("END deleteDelegation for delegationId=" + delegation.getId() + " dn=" + delegation.getDN() + " localUser=" + delegation.getLocalUser());
    }

    public void delete(DelegationRequest delegationRequest) throws DelegationException, DelegationManagerException {
        if (delegationRequest == null) {
            throw new DelegationException("delegation not specified!");
        }

        if (delegationRequest.getId() == null || delegationRequest.getId().length() == 0) {
            throw new DelegationException("delegationId not specified!");
        }

        if (delegationRequest.getDN() == null || delegationRequest.getDN().length() == 0) {
            throw new DelegationException("user DN not specified!");
        }

        if (delegationRequest.getLocalUser() == null || delegationRequest.getLocalUser().length() == 0) {
            throw new DelegationException("localUser not specified!");
        }

        logger.debug("BEGIN deleteDelegationRequest for delegationId=" + delegationRequest.getId() + " dn=" + delegationRequest.getDN() + " localUser="
                + delegationRequest.getLocalUser());

        Connection connection = getConnection();

        PreparedStatement deletePreparedStatement = null;

        try {
            StringBuffer query = new StringBuffer("delete from ");
            query.append(DELEGATION_REQUEST_TABLE).append(" where ");
            query.append(ID_FIELD).append(" = ? and ");
            query.append(DN_FIELD).append(" = ? and ");
            query.append(LOCAL_USER_FIELD).append(" = ?");

            deletePreparedStatement = connection.prepareStatement(query.toString());
            deletePreparedStatement.setString(1, delegationRequest.getId());
            deletePreparedStatement.setString(2, delegationRequest.getDN());
            deletePreparedStatement.setString(3, delegationRequest.getLocalUser());
            deletePreparedStatement.executeUpdate();

            connection.commit();
        } catch (SQLException sqle) {
            logger.error("Failure on db interaction", sqle);
            try {
                connection.rollback();
            } catch (SQLException e) {
                logger.error(e);
            }
            throw new DelegationException("Failure on db interaction: " + sqle.getMessage());
        } finally {
            if (deletePreparedStatement != null) {
                try {
                    deletePreparedStatement.close();
                } catch (SQLException sqle1) {
                    logger.error(sqle1);
                }
            }

            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    logger.error(e);
                }
            }
        }

        logger.debug("END deleteDelegationRequest for delegationId=" + delegationRequest.getId() + " dn=" + delegationRequest.getDN() + " localUser="
                + delegationRequest.getLocalUser());
    }

    public int deleteDelegationRequestsToDate(Calendar timestamp) throws DelegationException, DelegationManagerException {
        logger.debug("BEGIN deleteDelegationRequestsToDate");

        Connection connection = getConnection();

        PreparedStatement deletePreparedStatement = null;
        ResultSet rs = null;
        int rowCount = 0;

        try {
            StringBuffer query = new StringBuffer("delete from ");
            query.append(DELEGATION_REQUEST_TABLE).append(" where ").append(TIMESTAMP_FIELD).append(" < ?");

            deletePreparedStatement = connection.prepareStatement(query.toString());
            deletePreparedStatement.setTimestamp(1, new Timestamp(Calendar.getInstance().getTimeInMillis()));
            rowCount = deletePreparedStatement.executeUpdate();

            connection.commit();
        } catch (SQLException sqle) {
            logger.error("Failure on db interaction", sqle);
            throw new DelegationException("Failure on db interaction: " + sqle.getMessage());
        } finally {
            if (deletePreparedStatement != null) {
                try {
                    deletePreparedStatement.close();
                } catch (SQLException sqle1) {
                    logger.error(sqle1);
                }
            }

            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    logger.error(e);
                }
            }

            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    logger.error(e);
                }
            }
        }

        logger.debug("END deleteDelegationRequestsToDate (deleted " + rowCount + " delegation requests created before " + timestamp.getTime() + ")");
        return rowCount;
    }

    private void deleteDelegationTable() throws DelegationManagerException {
        logger.debug("BEGIN deleteDelegationTable");

        Connection connection = getConnection();
        PreparedStatement pstmt = null;

        try {
            pstmt = connection.prepareStatement("drop table if exists " + DELEGATION_TABLE);
            pstmt.executeUpdate();

            logger.debug(DELEGATION_TABLE + " table dropped!");

            pstmt = connection.prepareStatement("drop table if exists " + DELEGATION_REQUEST_TABLE);
            pstmt.executeUpdate();

            logger.debug(DELEGATION_REQUEST_TABLE + " table dropped!");

            // Commit
            connection.commit();

            logger.info(DELEGATION_TABLE + " and " + DELEGATION_REQUEST_TABLE + " tables dropped!");
        } catch (SQLException sqle) {
            String rollbackMessage = null;

            if (connection != null) {
                try {
                    connection.rollback();
                    rollbackMessage = " (rollback performed)";
                } catch (SQLException sqle1) {
                    rollbackMessage = " (rollback failed: " + sqle1.getMessage() + ")";
                }
            }

            logger.error("deleteDelegationTable failed: cannot delete the delegation tables: " + sqle.getMessage() + (rollbackMessage != null ? rollbackMessage : ""));
            throw new DelegationManagerException("Cannot delete the delegation tables: " + sqle.getMessage() + (rollbackMessage != null ? rollbackMessage : ""));
        } finally {
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (SQLException sqle1) {
                    logger.error(sqle1.getMessage());
                }
            }

            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqle2) {
                    logger.error("Problem in closing connection: " + sqle2.getMessage());
                    throw new DelegationManagerException("Problem in closing connection: " + sqle2.getMessage());
                }
            }
        }

        logger.debug("END deleteDelegationTable");
    }

    /**
     * Returns the connection to the database.
     * 
     * @return The connection to the database.
     * @throws DatabaseException
     */
    private Connection getConnection() throws DelegationManagerException {
        Connection connection = null;

        // try {
        // Class.forName("org.gjt.mm.mysql.Driver");
        // connection =
        // DriverManager.getConnection("jdbc:mysql://localhost:3306/creamdb",
        // "root", "zaq1qazz");
        // connection.setAutoCommit(false);
        // } catch (ClassNotFoundException e) {
        // throw new
        // DelegationManagerException("cannot get the database connection (datasourceName="
        // + DelegationManagerInterface.DELEGATION_DATASOURCE_NAME + ")");
        // } catch (SQLException e) {
        // // TODO Auto-generated catch block
        // e.printStackTrace();
        // }

        try {
            connection = DatasourceManager.getConnection(DelegationManagerInterface.DELEGATION_DATASOURCE_NAME);
        } catch (DatabaseException ex) {
            throw new DelegationManagerException(ex.getMessage());
        }

        if (connection == null) {
            logger.error("cannot get the database connection (datasourceName=" + DelegationManagerInterface.DELEGATION_DATASOURCE_NAME + ")");
            throw new DelegationManagerException("cannot get the database connection (datasourceName=" + DelegationManagerInterface.DELEGATION_DATASOURCE_NAME + ")");
        }

        return connection;
    }

    public Delegation getDelegation(String delegationId, String dn, String localUser) throws DelegationException, DelegationManagerException {
        return getDelegation(delegationId, dn, localUser, true);
    }

    public Delegation getDelegation(String delegationId, String dn, String localUser, boolean includeCertificate) throws DelegationException, DelegationManagerException {
        if (delegationId == null || delegationId.length() == 0) {
            throw new DelegationException("delegationId not specified!");
        }

        if (dn == null || dn.length() == 0) {
            throw new DelegationException("user DN not specified!");
        }

        if (localUser == null || localUser.length() == 0) {
            throw new DelegationException("localUser not specified!");
        }

        logger.debug("BEGIN getDelegation for delegationId=" + delegationId + " dn=" + dn + " localUser=" + localUser);

        StringBuffer query = new StringBuffer("select ");
        query.append(DELEGATION_TABLE).append(".").append(ID_FIELD).append(" as ").append(ID_FIELD).append(", ");
        query.append(DELEGATION_TABLE).append(".").append(DN_FIELD).append(" as ").append(DN_FIELD).append(", ");
        query.append(DELEGATION_TABLE).append(".").append(FQAN_FIELD).append(" as ").append(FQAN_FIELD).append(", ");
        query.append(DELEGATION_TABLE).append(".").append(VO_FIELD).append(" as ").append(VO_FIELD).append(", ");
        query.append(DELEGATION_TABLE).append(".").append(VOMS_ATTRIBUTE_FIELD).append(" as ").append(VOMS_ATTRIBUTE_FIELD).append(", ");
        query.append(DELEGATION_TABLE).append(".").append(INFO_FIELD).append(" as ").append(INFO_FIELD).append(", ");
        query.append(DELEGATION_TABLE).append(".").append(CERTIFICATE_FIELD).append(" as ").append(CERTIFICATE_FIELD).append(", ");
        query.append(DELEGATION_TABLE).append(".").append(LOCAL_USER_FIELD).append(" as ").append(LOCAL_USER_FIELD).append(", ");
        query.append(DELEGATION_TABLE).append(".").append(LOCAL_USER_GROUP_FIELD).append(" as ").append(LOCAL_USER_GROUP_FIELD).append(", ");
        query.append(DELEGATION_TABLE).append(".").append(START_TIME_FIELD).append(" as ").append(START_TIME_FIELD).append(", ");
        query.append(DELEGATION_TABLE).append(".").append(EXPIRATION_TIME_FIELD).append(" as ").append(EXPIRATION_TIME_FIELD).append(", ");
        query.append(DELEGATION_TABLE).append(".").append(LAST_UPDATE_TIME_FIELD).append(" as ").append(LAST_UPDATE_TIME_FIELD).append(" from ");
        query.append(DELEGATION_TABLE).append(" where ").append(ID_FIELD).append(" = ? and ").append(DN_FIELD).append(" = ? and ");
        query.append(LOCAL_USER_FIELD).append(" = ? ");

        Delegation delegation = null;
        PreparedStatement getProxyStatement = null;
        ResultSet rs = null;

        Connection connection = getConnection();

        try {
            getProxyStatement = connection.prepareStatement(query.toString());
            getProxyStatement.setString(1, delegationId);
            getProxyStatement.setString(2, dn);
            getProxyStatement.setString(3, localUser);

            rs = getProxyStatement.executeQuery();

            if (rs.next()) {
                delegation = makeDelegation(rs, includeCertificate);
            }
        } catch (SQLException sqle) {
            logger.error("Failure on storage interaction", sqle);
            throw new DelegationException("Failure on storage interaction: " + sqle.getMessage());
        } finally {
            if (getProxyStatement != null) {
                try {
                    getProxyStatement.close();
                } catch (SQLException e) {
                    logger.error(e);
                }
            }

            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    logger.error(e);
                }
            }

            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    logger.error(e);
                }
            }
        }

        logger.debug("END getDelegation for delegationId=" + delegationId + " dn=" + dn + " localUser=" + localUser);

        return delegation;
    }

    public DelegationRequest getDelegationRequest(String delegationId, String dn, String localUser) throws DelegationException, DelegationManagerException {
        if (delegationId == null || delegationId.length() == 0) {
            throw new DelegationException("delegationId not specified!");
        }

        if (dn == null || dn.length() == 0) {
            throw new DelegationException("user DN not specified!");
        }

        if (localUser == null || localUser.length() == 0) {
            throw new DelegationException("localUser not specified!");
        }

        logger.debug("BEGIN getDelegationRequest for delegationId=" + delegationId + " dn=" + dn + " localUser=" + localUser);

        StringBuffer query = new StringBuffer("select ");
        query.append(DELEGATION_REQUEST_TABLE).append(".").append(ID_FIELD).append(" as ").append(ID_FIELD).append(", ");
        query.append(DELEGATION_REQUEST_TABLE).append(".").append(DN_FIELD).append(" as ").append(DN_FIELD).append(", ");
        query.append(DELEGATION_REQUEST_TABLE).append(".").append(LOCAL_USER_FIELD).append(" as ").append(LOCAL_USER_FIELD).append(", ");
        query.append(DELEGATION_REQUEST_TABLE).append(".").append(CERTIFICATE_REQUEST_FIELD).append(" as ").append(CERTIFICATE_REQUEST_FIELD).append(", ");
        query.append(DELEGATION_REQUEST_TABLE).append(".").append(VOMS_ATTRIBUTE_FIELD).append(" as ").append(VOMS_ATTRIBUTE_FIELD).append(", ");
        query.append(DELEGATION_REQUEST_TABLE).append(".").append(TIMESTAMP_FIELD).append(" as ").append(TIMESTAMP_FIELD).append(", ");
        query.append(DELEGATION_REQUEST_TABLE).append(".").append(PUBLIC_KEY_FIELD).append(" as ").append(PUBLIC_KEY_FIELD).append(", ");
        query.append(DELEGATION_REQUEST_TABLE).append(".").append(PRIVATE_KEY_FIELD).append(" as ").append(PRIVATE_KEY_FIELD).append(" from ");
        query.append(DELEGATION_REQUEST_TABLE).append(" where ").append(ID_FIELD).append(" = ? and ").append(DN_FIELD).append(" = ? and ").append(LOCAL_USER_FIELD).append(" = ?");

        DelegationRequest delegationRequest = null;
        PreparedStatement getProxyStatement = null;
        ResultSet rs = null;

        Connection connection = getConnection();

        try {
            getProxyStatement = connection.prepareStatement(query.toString());
            getProxyStatement.setString(1, delegationId);
            getProxyStatement.setString(2, dn);
            getProxyStatement.setString(3, localUser);

            rs = getProxyStatement.executeQuery();

            if (rs.next()) {
                delegationRequest = makeDelegationRequest(rs);
            }
            // else {
            // throw new DelegationException("Delegation request " +
            // delegationId + " [dn=" + dn + "; localUser=" + localUser +
            // "] not found!");
            // }
        } catch (SQLException sqle) {
            logger.error("Failure on storage interaction", sqle);
            throw new DelegationException("Failure on storage interaction: " + sqle.getMessage());
        } finally {
            if (getProxyStatement != null) {
                try {
                    getProxyStatement.close();
                } catch (SQLException e) {
                    logger.error(e);
                }
            }

            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    logger.error(e);
                }
            }

            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    logger.error(e);
                }
            }
        }

        logger.debug("END getDelegationRequest for delegationId=" + delegationId + " dn=" + dn + " localUser=" + localUser);

        return delegationRequest;
    }

    public List<DelegationRequest> getDelegationRequests(String dn, String localUser) throws DelegationException, DelegationManagerException {
        if (dn == null || dn.length() == 0) {
            throw new IllegalArgumentException("user DN not specified!");
        }

        if (localUser == null || localUser.length() == 0) {
            throw new IllegalArgumentException("localUser not specified!");
        }

        logger.debug("BEGIN getDelegationRequests");

        StringBuffer query = new StringBuffer("select ");
        query.append(DELEGATION_REQUEST_TABLE).append(".").append(ID_FIELD).append(" as ").append(ID_FIELD).append(", ");
        query.append(DELEGATION_REQUEST_TABLE).append(".").append(DN_FIELD).append(" as ").append(DN_FIELD).append(", ");
        query.append(DELEGATION_REQUEST_TABLE).append(".").append(LOCAL_USER_FIELD).append(" as ").append(LOCAL_USER_FIELD).append(", ");
        query.append(DELEGATION_REQUEST_TABLE).append(".").append(CERTIFICATE_REQUEST_FIELD).append(" as ").append(CERTIFICATE_REQUEST_FIELD).append(", ");
        query.append(DELEGATION_REQUEST_TABLE).append(".").append(VOMS_ATTRIBUTE_FIELD).append(" as ").append(VOMS_ATTRIBUTE_FIELD).append(", ");
        query.append(DELEGATION_REQUEST_TABLE).append(".").append(TIMESTAMP_FIELD).append(" as ").append(TIMESTAMP_FIELD).append(", ");
        query.append(DELEGATION_REQUEST_TABLE).append(".").append(PUBLIC_KEY_FIELD).append(" as ").append(PUBLIC_KEY_FIELD).append(", ");
        query.append(DELEGATION_REQUEST_TABLE).append(".").append(PRIVATE_KEY_FIELD).append(" as ").append(PRIVATE_KEY_FIELD).append(" from ");
        query.append(DELEGATION_REQUEST_TABLE).append(" where ").append(DN_FIELD).append(" = ? and ").append(LOCAL_USER_FIELD).append(" = ?");

        Connection connection = getConnection();

        List<DelegationRequest> result = new ArrayList<DelegationRequest>(0);
        PreparedStatement selectPreparedStatement = null;
        ResultSet rs = null;

        try {
            selectPreparedStatement = connection.prepareStatement(query.toString());
            selectPreparedStatement.setString(1, dn);
            selectPreparedStatement.setString(2, localUser);

            // execute query, and return number of rows created
            rs = selectPreparedStatement.executeQuery();

            while (rs.next()) {
                result.add(makeDelegationRequest(rs));
            }
        } catch (SQLException sqle) {
            logger.error("Failure on storage interaction", sqle);
            throw new DelegationException("Failure on storage interaction: " + sqle.getMessage());
        } finally {
            if (selectPreparedStatement != null) {
                try {
                    selectPreparedStatement.close();
                } catch (SQLException sqle1) {
                    logger.error(sqle1);
                }
            }

            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    logger.error(e);
                }
            }

            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    logger.error(e);
                }
            }
        }

        logger.debug("END getDelegationRequests");
        return result;
    }

    public List<Delegation> getDelegations(Calendar expirationTime) throws DelegationException, DelegationManagerException {
        return getDelegations(expirationTime, true);
    }

    public List<Delegation> getDelegations(Calendar expirationTime, boolean includeCertificate) throws DelegationException, DelegationManagerException {
        if (expirationTime == null) {
            throw new IllegalArgumentException("expirationTime not specified!");
        }

        logger.debug("BEGIN getDelegations [expirationTime < " + expirationTime.getTime() + "]");

        StringBuffer query = new StringBuffer("select ");
        query.append(DELEGATION_TABLE).append(".").append(ID_FIELD).append(" as ").append(ID_FIELD).append(", ");
        query.append(DELEGATION_TABLE).append(".").append(DN_FIELD).append(" as ").append(DN_FIELD).append(", ");
        query.append(DELEGATION_TABLE).append(".").append(FQAN_FIELD).append(" as ").append(FQAN_FIELD).append(", ");
        query.append(DELEGATION_TABLE).append(".").append(VO_FIELD).append(" as ").append(VO_FIELD).append(", ");
        query.append(DELEGATION_TABLE).append(".").append(VOMS_ATTRIBUTE_FIELD).append(" as ").append(VOMS_ATTRIBUTE_FIELD).append(", ");
        query.append(DELEGATION_TABLE).append(".").append(INFO_FIELD).append(" as ").append(INFO_FIELD).append(", ");

        if (includeCertificate) {
            query.append(DELEGATION_TABLE).append(".").append(CERTIFICATE_FIELD).append(" as ").append(CERTIFICATE_FIELD).append(", ");
        }

        query.append(DELEGATION_TABLE).append(".").append(LOCAL_USER_FIELD).append(" as ").append(LOCAL_USER_FIELD).append(", ");
        query.append(DELEGATION_TABLE).append(".").append(LOCAL_USER_GROUP_FIELD).append(" as ").append(LOCAL_USER_GROUP_FIELD).append(", ");
        query.append(DELEGATION_TABLE).append(".").append(START_TIME_FIELD).append(" as ").append(START_TIME_FIELD).append(", ");
        query.append(DELEGATION_TABLE).append(".").append(EXPIRATION_TIME_FIELD).append(" as ").append(EXPIRATION_TIME_FIELD).append(", ");
        query.append(DELEGATION_TABLE).append(".").append(LAST_UPDATE_TIME_FIELD).append(" as ").append(LAST_UPDATE_TIME_FIELD).append(" from ");
        query.append(DELEGATION_TABLE).append(" where ").append(EXPIRATION_TIME_FIELD).append(" <= ?");

        Connection connection = getConnection();

        List<Delegation> result = new ArrayList<Delegation>(0);
        PreparedStatement selectPreparedStatement = null;
        ResultSet rs = null;

        try {
            selectPreparedStatement = connection.prepareStatement(query.toString());
            selectPreparedStatement.setTimestamp(1, new java.sql.Timestamp(expirationTime.getTimeInMillis()));

            // execute query, and return number of rows created
            rs = selectPreparedStatement.executeQuery();

            while (rs.next()) {
                result.add(makeDelegation(rs, includeCertificate));
            }
        } catch (SQLException sqle) {
            logger.error("Failure on storage interaction", sqle);
            throw new DelegationException("Failure on storage interaction: " + sqle.getMessage());
        } finally {
            if (selectPreparedStatement != null) {
                try {
                    selectPreparedStatement.close();
                } catch (SQLException sqle1) {
                    logger.error(sqle1);
                }
            }

            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    logger.error(e);
                }
            }

            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    logger.error(e);
                }
            }
        }

        logger.debug("END getDelegations");
        return result;
    }

    public List<Delegation> getDelegations(String dn, String localUser) throws DelegationException, DelegationManagerException {
        return getDelegations(dn, localUser, true);
    }

    public List<Delegation> getDelegations(String dn, String localUser, boolean includeCertificate) throws DelegationException, DelegationManagerException {
        if (dn == null || dn.length() == 0) {
            throw new IllegalArgumentException("user DN not specified!");
        }

        if (localUser == null || localUser.length() == 0) {
            throw new IllegalArgumentException("localUser not specified!");
        }

        logger.debug("BEGIN getDelegations");

        StringBuffer query = new StringBuffer("select ");
        query.append(DELEGATION_TABLE).append(".").append(ID_FIELD).append(" as ").append(ID_FIELD).append(", ");
        query.append(DELEGATION_TABLE).append(".").append(DN_FIELD).append(" as ").append(DN_FIELD).append(", ");
        query.append(DELEGATION_TABLE).append(".").append(FQAN_FIELD).append(" as ").append(FQAN_FIELD).append(", ");
        query.append(DELEGATION_TABLE).append(".").append(VO_FIELD).append(" as ").append(VO_FIELD).append(", ");
        query.append(DELEGATION_TABLE).append(".").append(VOMS_ATTRIBUTE_FIELD).append(" as ").append(VOMS_ATTRIBUTE_FIELD).append(", ");
        query.append(DELEGATION_TABLE).append(".").append(INFO_FIELD).append(" as ").append(INFO_FIELD).append(", ");

        if (includeCertificate) {
            query.append(DELEGATION_TABLE).append(".").append(CERTIFICATE_FIELD).append(" as ").append(CERTIFICATE_FIELD).append(", ");
        }

        query.append(DELEGATION_TABLE).append(".").append(LOCAL_USER_FIELD).append(" as ").append(LOCAL_USER_FIELD).append(", ");
        query.append(DELEGATION_TABLE).append(".").append(LOCAL_USER_GROUP_FIELD).append(" as ").append(LOCAL_USER_GROUP_FIELD).append(", ");
        query.append(DELEGATION_TABLE).append(".").append(START_TIME_FIELD).append(" as ").append(START_TIME_FIELD).append(", ");
        query.append(DELEGATION_TABLE).append(".").append(EXPIRATION_TIME_FIELD).append(" as ").append(EXPIRATION_TIME_FIELD).append(", ");
        query.append(DELEGATION_TABLE).append(".").append(LAST_UPDATE_TIME_FIELD).append(" as ").append(LAST_UPDATE_TIME_FIELD).append(" from ");
        query.append(DELEGATION_TABLE).append(" where ").append(DN_FIELD).append(" = ? and ").append(LOCAL_USER_FIELD).append(" = ?");

        Connection connection = getConnection();

        List<Delegation> result = new ArrayList<Delegation>(0);
        PreparedStatement selectPreparedStatement = null;
        ResultSet rs = null;

        try {
            selectPreparedStatement = connection.prepareStatement(query.toString());
            selectPreparedStatement.setString(1, dn);
            selectPreparedStatement.setString(2, localUser);

            // execute query, and return number of rows created
            rs = selectPreparedStatement.executeQuery();

            while (rs.next()) {
                result.add(makeDelegation(rs, includeCertificate));
            }
        } catch (SQLException sqle) {
            logger.error("Failure on storage interaction", sqle);
            throw new DelegationException("Failure on storage interaction: " + sqle.getMessage());
        } finally {
            if (selectPreparedStatement != null) {
                try {
                    selectPreparedStatement.close();
                } catch (SQLException sqle1) {
                    logger.error(sqle1);
                }
            }

            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    logger.error(e);
                }
            }

            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    logger.error(e);
                }
            }
        }

        logger.debug("END getDelegations");
        return result;
    }

    public String getDelegationSuffix() throws DelegationException, DelegationManagerException {
        try {
            return DBInfoManager.getDelegationSuffix(DelegationManagerInterface.DELEGATION_DATASOURCE_NAME);
        } catch (IllegalArgumentException ex) {
            throw new DelegationManagerException(ex.getMessage());
        }
    }

    public List<Delegation> getExpiredDelegations() throws DelegationException, DelegationManagerException {
        logger.debug("BEGIN getExpiredDelegations");

        List<Delegation> result = getDelegations(Calendar.getInstance(), false);

        logger.debug("END getExpiredDelegations (found " + result.size() + " delegations)");

        return result;
    }

    public void init() throws DelegationManagerException {
        logger.info("Initializing the DelegationManager...");

        createDelegationTable();

        DelegationPurger.getInstance();

        logger.info("DelegationManager initialized!");

        // ServiceConfig serviceConf = ServiceConfig.getConfiguration();
        //
        // int purgerRateInMinutes = 720;
        //
        // try {
        // purgerRateInMinutes =
        // Integer.parseInt(serviceConf.getStringParameter(ServiceAttribute.DELEGATION_PURGE_RATE_LABEL));
        // } catch (Throwable t) {
        // logger.warn("Configuration warning: wrong value for " +
        // ServiceAttribute.DELEGATION_PURGE_RATE_LABEL +
        // " property => using default " + purgerRateInMinutes);
        // }
        //
        // if (purgerRateInMinutes == -1) {
        // logger.info("the delegation proxy purger is disabled!");
        // } else {
        // if (timer == null) {
        // Calendar time = Calendar.getInstance();
        // time.add(Calendar.SECOND, 10);
        //
        // timer = new Timer("TIMER", true);
        // timer.schedule(new DelegationPurger(), time.getTime(),
        // purgerRateInMinutes * 60000);
        // }
        //
        // logger.info("DelegationManager's initialization done!");
        // }
    }

    public void insert(Delegation delegation) throws DelegationException, DelegationManagerException {
        if (delegation == null) {
            throw new IllegalArgumentException("delegation not specified!");
        }

        logger.debug("BEGIN insertDelegation for delegationId=" + delegation.getId() + " dn=" + delegation.getDN() + " localUser=" + delegation.getLocalUser());

        if (delegation.getLocalUser() == null) {
            delegation.setLocalUser(CEUtils.getLocalUser());
        }

        if (delegation.getLocalUserGroup() == null) {
            delegation.setLocalUserGroup(CEUtils.getLocalUserGroup());
        }

        StringBuffer query = new StringBuffer("insert into ");
        query.append(DELEGATION_TABLE).append(" (").append(ID_FIELD).append(", ");
        query.append(DN_FIELD).append(", ").append(FQAN_FIELD).append(", ").append(VO_FIELD).append(", ");
        query.append(LOCAL_USER_FIELD).append(", ").append(LOCAL_USER_GROUP_FIELD).append(", ");
        query.append(CERTIFICATE_FIELD).append(", ").append(INFO_FIELD).append(", ").append(VOMS_ATTRIBUTE_FIELD).append(", ");
        query.append(START_TIME_FIELD).append(", ").append(EXPIRATION_TIME_FIELD).append(", ").append(LAST_UPDATE_TIME_FIELD);
        query.append(") values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

        PreparedStatement insertStatement = null;

        Connection connection = getConnection();

        try {
            insertStatement = connection.prepareStatement(query.toString());
            insertStatement.setString(1, delegation.getId());
            insertStatement.setString(2, delegation.getDN());
            insertStatement.setString(3, delegation.getFQAN());
            insertStatement.setString(4, delegation.getVO());
            insertStatement.setString(5, delegation.getLocalUser());
            insertStatement.setString(6, delegation.getLocalUserGroup());
            insertStatement.setString(7, delegation.getCertificate());
            insertStatement.setString(8, delegation.getInfo());

            if (!delegation.getVOMSAttributes().isEmpty()) {
                String vomsAttrsStr = "";

                for (String vomsAttribute : delegation.getVOMSAttributes()) {
                    vomsAttrsStr += "\t" + vomsAttribute;
                }

                insertStatement.setString(9, vomsAttrsStr);
            } else {
                insertStatement.setString(9, null);
            }

            if (delegation.getStartTime() != null) {
                insertStatement.setTimestamp(10, new java.sql.Timestamp(delegation.getStartTime().getTime()));
            } else {
                insertStatement.setTimestamp(10, null);
            }

            if (delegation.getExpirationTime() != null) {
                insertStatement.setTimestamp(11, new java.sql.Timestamp(delegation.getExpirationTime().getTime()));
            } else {
                insertStatement.setTimestamp(11, null);
            }

            if (delegation.getLastUpdateTime() != null) {
                insertStatement.setTimestamp(12, new java.sql.Timestamp(delegation.getLastUpdateTime().getTime()));
            } else {
                insertStatement.setTimestamp(12, null);
            }

            insertStatement.executeUpdate();

            // Commit
            connection.commit();
        } catch (SQLException sqle) {
            logger.error("Failure on db interaction", sqle);
            try {
                connection.rollback();
            } catch (SQLException e) {
                logger.error(e);
            }
            throw new DelegationException("Failure on db interaction: " + sqle.getMessage());
        } finally {
            if (insertStatement != null) {
                try {
                    insertStatement.close();
                } catch (SQLException e) {
                    logger.error(e);
                }
            }

            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    logger.error(e);
                }
            }
        }

        logger.debug("END insertDelegation for delegationId=" + delegation.getId() + " dn=" + delegation.getDN() + " localUser=" + delegation.getLocalUser());
    }

    public void insert(DelegationRequest delegationRequest) throws DelegationException, DelegationManagerException {
        if (delegationRequest == null) {
            throw new IllegalArgumentException("delegationRequest not specified!");
        }

        logger.debug("BEGIN insertDelegationRequest for delegationId=" + delegationRequest.getId() + " dn=" + delegationRequest.getDN());

        StringBuffer query = new StringBuffer("insert into ");
        query.append(DELEGATION_REQUEST_TABLE).append(" (").append(ID_FIELD).append(", ");
        query.append(DN_FIELD).append(", ").append(VOMS_ATTRIBUTE_FIELD).append(", ");
        query.append(PUBLIC_KEY_FIELD).append(", ").append(PRIVATE_KEY_FIELD).append(", ");
        query.append(CERTIFICATE_REQUEST_FIELD).append(", ").append(LOCAL_USER_FIELD).append(", ");
        query.append(TIMESTAMP_FIELD).append(") values (?, ?, ?, ?, ?, ?, ?, ?)");
        PreparedStatement insertStatement = null;

        Connection connection = getConnection();

        try {
            insertStatement = connection.prepareStatement(query.toString());
            insertStatement.setString(1, delegationRequest.getId());
            insertStatement.setString(2, delegationRequest.getDN());
            insertStatement.setString(4, delegationRequest.getPublicKey());
            insertStatement.setString(5, delegationRequest.getPrivateKey());
            insertStatement.setString(6, delegationRequest.getCertificateRequest());
            insertStatement.setString(7, delegationRequest.getLocalUser());

            if (!delegationRequest.getVOMSAttributes().isEmpty()) {
                String vomsAttrsStr = "";

                for (String vomsAttribute : delegationRequest.getVOMSAttributes()) {
                    vomsAttrsStr += "\t" + vomsAttribute;
                }

                insertStatement.setString(3, vomsAttrsStr);
            } else {
                insertStatement.setString(3, null);
            }

            if (delegationRequest.getTimestamp() != null) {
                insertStatement.setTimestamp(8, new java.sql.Timestamp(delegationRequest.getTimestamp().getTime()));
            } else {
                insertStatement.setTimestamp(8, new java.sql.Timestamp(Calendar.getInstance().getTimeInMillis()));
            }

            insertStatement.executeUpdate();

            // Commit
            connection.commit();
        } catch (SQLException sqle) {
            logger.error("Failure on db interaction", sqle);
            try {
                connection.rollback();
            } catch (SQLException e) {
                logger.error(e);
            }
            throw new DelegationManagerException("Failure on db interaction: " + sqle.getMessage());
        } finally {
            if (insertStatement != null) {
                try {
                    insertStatement.close();
                } catch (SQLException e) {
                    logger.error(e);
                }
            }

            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    logger.error(e);
                }
            }
        }

        logger.debug("END insertDelegationRequest for delegationId=" + delegationRequest.getId() + " dn=" + delegationRequest.getDN());
    }

    private Delegation makeDelegation(ResultSet rs, boolean includeCertificate) throws DelegationException {
        if (rs == null) {
            throw new DelegationException("rs new defined!");
        }

        try {
            Delegation delegation = new Delegation(rs.getString(ID_FIELD));
            delegation.setDN(rs.getString(DN_FIELD));
            delegation.setFQAN(rs.getString(FQAN_FIELD));
            delegation.setVO(rs.getString(VO_FIELD));
            delegation.setLocalUser(rs.getString(LOCAL_USER_FIELD));
            delegation.setLocalUserGroup(rs.getString(LOCAL_USER_GROUP_FIELD));
            delegation.setInfo(rs.getString(INFO_FIELD));
            delegation.setStartTime(rs.getTimestamp(START_TIME_FIELD));
            delegation.setExpirationTime(rs.getTimestamp(EXPIRATION_TIME_FIELD));
            delegation.setLastUpdateTime(rs.getTimestamp(LAST_UPDATE_TIME_FIELD));

            if (includeCertificate) {
                delegation.setCertificate(rs.getString(CERTIFICATE_FIELD));
            }

            String vomsAttr = rs.getString(VOMS_ATTRIBUTE_FIELD);

            if (vomsAttr != null) {
                StringTokenizer st = new StringTokenizer(vomsAttr, "\t");
                List<String> vomsAttributes = new ArrayList<String>(0);

                while (st.hasMoreTokens()) {
                   vomsAttributes.add(st.nextToken());
                }

                delegation.setVOMSAttributes(vomsAttributes);
            }


            return delegation;
        } catch (SQLException sqle) {
            logger.error("makeDelegation: failure on ResultSet: ", sqle);
            throw new DelegationException("Failure on ResultSet: " + sqle.getMessage());
        }
    }

    private DelegationRequest makeDelegationRequest(ResultSet rs) throws DelegationException {
        if (rs == null) {
            throw new DelegationException("rs new defined!");
        }

        try {
            DelegationRequest delegationRequest = new DelegationRequest(rs.getString(ID_FIELD));
            delegationRequest.setDN(rs.getString(DN_FIELD));
            delegationRequest.setLocalUser(rs.getString(LOCAL_USER_FIELD));
            delegationRequest.setPublicKey(rs.getString(PUBLIC_KEY_FIELD));
            delegationRequest.setPrivateKey(rs.getString(PRIVATE_KEY_FIELD));
            delegationRequest.setCertificateRequest(rs.getString(CERTIFICATE_REQUEST_FIELD));
            delegationRequest.setTimestamp(rs.getTimestamp(TIMESTAMP_FIELD));

            String vomsAttr = rs.getString(VOMS_ATTRIBUTE_FIELD);
            if (vomsAttr != null) {
                StringTokenizer st = new StringTokenizer(rs.getString(VOMS_ATTRIBUTE_FIELD), "\t");
                List<String> vomsAttributes = new ArrayList<String>(0);

                while (st.hasMoreTokens()) {
                    vomsAttributes.add(st.nextToken());
                }

                delegationRequest.setVOMSAttributes(vomsAttributes);
            }

            return delegationRequest;
        } catch (SQLException sqle) {
            logger.error("makeDelegation: failure on ResultSet: ", sqle);
            throw new DelegationException("Failure on ResultSet: " + sqle.getMessage());
        }
    }
    
    public void terminate() {
        logger.info("terminate invoked!");

        DelegationPurger.getInstance().terminate();
        delegationManager = null;
        terminated = true;

        logger.info("terminated!");
    }
    
    public void update(Delegation delegation) throws DelegationException, DelegationManagerException {
        if (delegation == null) {
            throw new DelegationException("delegation not specified!");
        }

        logger.debug("BEGIN update " + delegation.toString());

        delegation.setLastUpdateTime(Calendar.getInstance().getTime());

        StringBuffer query = new StringBuffer("update ");
        query.append(DELEGATION_TABLE).append(" set ").append(CERTIFICATE_FIELD).append(" = ?, ").append(INFO_FIELD).append(" = ?, ");
        query.append(VOMS_ATTRIBUTE_FIELD).append(" = ?, ").append(START_TIME_FIELD).append(" = ?, ");
        query.append(EXPIRATION_TIME_FIELD).append(" = ?, ").append(LAST_UPDATE_TIME_FIELD);
        query.append(" = ? where ").append(ID_FIELD).append(" = ? and ").append(DN_FIELD).append(" = ?");


        logger.info(query.toString());

        Connection connection = getConnection();

        PreparedStatement updateStatement = null;

        try {
            updateStatement = connection.prepareStatement(query.toString());
            updateStatement.setString(1, delegation.getCertificate());
            updateStatement.setString(2, delegation.getInfo());
            updateStatement.setTimestamp(4, new java.sql.Timestamp(delegation.getStartTime().getTime()));
            updateStatement.setTimestamp(5, new java.sql.Timestamp(delegation.getExpirationTime().getTime()));
            updateStatement.setTimestamp(6, new java.sql.Timestamp(delegation.getLastUpdateTime().getTime()));
            updateStatement.setString(7, delegation.getId());
            updateStatement.setString(8, delegation.getDN());

            if (!delegation.getVOMSAttributes().isEmpty()) {
                String vomsAttrsStr = "";

                for (String vomsAttribute : delegation.getVOMSAttributes()) {
                    vomsAttrsStr += "\t" + vomsAttribute;
                }

                updateStatement.setString(3, vomsAttrsStr);
            } else {
                updateStatement.setString(3, null);
            }

            updateStatement.executeUpdate();

            // Commit
            connection.commit();
        } catch (SQLException sqle) {
            logger.error("Failure on db interaction", sqle);
            try {
                connection.rollback();
            } catch (SQLException e) {
                logger.error(e);
            }
            throw new DelegationException("Failure on db interaction: " + sqle.getMessage());
        } finally {
            if (updateStatement != null) {
                try {
                    updateStatement.close();
                } catch (SQLException e) {
                    logger.error(e);
                }
            }

            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    logger.error(e);
                }
            }
        }

        logger.debug("END update " + delegation.toString());
    }

    public void update(DelegationRequest delegationRequest) throws DelegationException, DelegationManagerException {
        if (delegationRequest == null) {
            throw new DelegationException("delegationRequest not specified!");
        }

        logger.debug("BEGIN updateDelegationRequest for delegationId=" + delegationRequest.getId() + " dn=" + delegationRequest.getDN() + " localUser="
                + delegationRequest.getLocalUser());

        StringBuffer query = new StringBuffer("update ");
        query.append(DELEGATION_REQUEST_TABLE).append(" set ").append(CERTIFICATE_REQUEST_FIELD).append(" = ?, ");
        query.append(PUBLIC_KEY_FIELD).append(" = ?, ").append(PRIVATE_KEY_FIELD).append(" = ?, ");
        query.append(VOMS_ATTRIBUTE_FIELD).append(" = ?, ");
        query.append(TIMESTAMP_FIELD).append(" = ? where ").append(ID_FIELD).append(" = ? and ").append(DN_FIELD).append(" = ?");

        Connection connection = getConnection();

        PreparedStatement updateStatement = null;

        try {
            updateStatement = connection.prepareStatement(query.toString());
            updateStatement.setString(1, delegationRequest.getCertificateRequest());
            updateStatement.setString(2, delegationRequest.getPublicKey());
            updateStatement.setString(3, delegationRequest.getPrivateKey());
            updateStatement.setString(5, delegationRequest.getId());
            updateStatement.setString(6, delegationRequest.getDN());
            updateStatement.setTimestamp(7, new java.sql.Timestamp(delegationRequest.getTimestamp().getTime()));

            if (!delegationRequest.getVOMSAttributes().isEmpty()) {
                String vomsAttrsStr = "";

                for (String vomsAttribute : delegationRequest.getVOMSAttributes()) {
                    vomsAttrsStr += "\t" + vomsAttribute;
                }

                updateStatement.setString(4, vomsAttrsStr);
            } else {
                updateStatement.setString(4, null);
            }

            updateStatement.executeUpdate();

            // Commit
            connection.commit();
        } catch (SQLException sqle) {
            logger.error("Failure on db interaction", sqle);
            try {
                connection.rollback();
            } catch (SQLException e) {
                logger.error(e);
            }
            throw new DelegationException("Failure on db interaction: " + sqle.getMessage());
        } finally {
            if (updateStatement != null) {
                try {
                    updateStatement.close();
                } catch (SQLException e) {
                    logger.error(e);
                }
            }

            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    logger.error(e);
                }
            }
        }

        logger.debug("END updateDelegationRequest for delegationId=" + delegationRequest.getId() + " dn=" + delegationRequest.getDN() + " localUser="
                + delegationRequest.getLocalUser());
    }
}
