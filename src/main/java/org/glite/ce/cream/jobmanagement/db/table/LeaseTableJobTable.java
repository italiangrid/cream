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
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.glite.ce.creamapi.jobmanagement.db.table.LeaseTableJobTableInterface;

public class LeaseTableJobTable implements LeaseTableJobTableInterface{

	private final static Logger logger = Logger.getLogger(LeaseTableJobTable.class);

	public LeaseTableJobTable() throws SQLException {
		logger.debug("Call LeaseTableJobTable constructor");
	}
	
	public int setLeaseId(String leaseId, String jobId, String userId, Connection connection) throws SQLException {
		logger.debug("Begin setLease");
		int rowCount  = 0;
		/*
		if (!JobTable.isLeaseIdAllowed(leaseId)){
			throw new IllegalArgumentException("LeaseId is invalid. It mustn't contain " + JobTable.UNDERSCORE +
					                            " and the '" + JobTable.EXPIRED_STRING + "' string.");
		}
		*/
		PreparedStatement updateLeasePreparedStatement = null;
		try{
		  updateLeasePreparedStatement = connection.prepareStatement(getUpdateLeaseIdQuery(leaseId, jobId, userId));
		  // execute query, and return number of rows created
		  rowCount = updateLeasePreparedStatement.executeUpdate();
		  logger.debug("Lease updated: " + rowCount); 
        } catch (SQLException sqle) {
          throw sqle;
        } finally {
          if (updateLeasePreparedStatement != null) {
            try {
            	updateLeasePreparedStatement.close();
            } catch (SQLException sqle1) {
              logger.error(sqle1);
            } 
          }
        }
		logger.debug("End setLease");
		return rowCount;
	}
	
	private static String getUpdateLeaseIdQuery(String leaseId, String jobId, String userId) {
		StringBuffer updateQuery = new StringBuffer();
		updateQuery.append("update  ");
		updateQuery.append(JobTable.NAME_TABLE);		
		if (leaseId != null){
			updateQuery.append(", " + LeaseTable.NAME_TABLE);
			updateQuery.append(" set " + JobTable.NAME_TABLE + "." + JobTable.LEASE_ID_FIELD + " = '" + leaseId + "'");
		} else {
			updateQuery.append(" set " + JobTable.NAME_TABLE + "." + JobTable.LEASE_ID_FIELD + " = " + leaseId);
		}
		updateQuery.append(" where ");
		updateQuery.append(JobTable.NAME_TABLE + "." + JobTable.USER_ID_FIELD      + " = '" + userId + "'");
		updateQuery.append(" and " + JobTable.NAME_TABLE + "." + JobTable.ID_FIELD + " = '" + jobId  + "'");
		updateQuery.append(" and " + JobTable.NAME_TABLE + "." + JobTable.LEASE_TIME_FIELD  + " IS NULL");
		if (leaseId != null){
			updateQuery.append(" and " + LeaseTable.NAME_TABLE + "." + LeaseTable.LEASE_ID_FIELD + " = '"  + leaseId + "'");
			updateQuery.append(" and " + LeaseTable.NAME_TABLE + "." + LeaseTable.USER_ID_FIELD  + " = "  + 
					           JobTable.NAME_TABLE + "." + JobTable.USER_ID_FIELD);              
		}
		return updateQuery.toString();
	}
}
