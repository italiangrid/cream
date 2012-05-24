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
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.glite.ce.creamapi.activitymanagement.db.NotificationDB;
import org.glite.ce.creamapi.activitymanagement.db.table.NotificationTableInterface;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.Notification;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.ProtocolTypeEnumeration;

public class NotificationTable implements NotificationTableInterface {
    private static final Logger logger = Logger.getLogger(NotificationTable.class.getName());
    private static final String insertSP = "{call insertNotificationTable (?, ?, ?, ?)}";
    private static final String selectSP = "{call selectNotificationTable (?)}";
    
    public long executeInsert(String activityId, Notification notification, Connection connection) throws SQLException {
        CallableStatement insertCS = null;
        long notificationId = 0;
        try {
            insertCS = connection.prepareCall(insertSP);
            insertCS.setString(1, activityId);
            insertCS.setString(2, (notification.getProtocol() != null) ? notification.getProtocol().value(): null);
            insertCS.setBoolean(3, notification.isOptional());
            insertCS.registerOutParameter(4, Types.BIGINT);
            insertCS.execute();
            notificationId = insertCS.getLong(4);
            logger.debug("insert into NotificationTable done");
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
        return notificationId;
    }

    public List<NotificationDB> executeSelect(String activityId, Connection connection) throws SQLException {
        CallableStatement selectCS = null;
        List<NotificationDB> notificationDBList = null;
        try {
            selectCS = connection.prepareCall(selectSP);
            selectCS.setString(1, activityId);
            if (selectCS.execute()) {
            	notificationDBList = new ArrayList<NotificationDB>(0);
            	NotificationDB notificationDB = null;
                ResultSet resultSet = selectCS.getResultSet();
                while (resultSet.next()) {
                	notificationDB = new NotificationDB();
                	notificationDB.setId(resultSet.getLong(NotificationTableInterface.ID_LABEL));
                	notificationDB.setNotification(new Notification());
                	notificationDB.getNotification().setProtocol(ProtocolTypeEnumeration.fromValue(resultSet.getString(NotificationTableInterface.PROTOCOLO_LABEL)));
                	notificationDB.getNotification().setOptional(resultSet.getBoolean(NotificationTableInterface.OPTIONAL_LABEL));
                	notificationDBList.add(notificationDB);
                }
            }
            logger.debug("select NotificationTable done");
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
        return notificationDBList;
    }
}
