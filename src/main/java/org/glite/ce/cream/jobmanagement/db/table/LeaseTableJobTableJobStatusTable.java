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
import org.glite.ce.creamapi.jobmanagement.db.table.LeaseTableJobTableJobStatusInterface;

public class LeaseTableJobTableJobStatusTable implements LeaseTableJobTableJobStatusInterface{

	private final static Logger logger = Logger.getLogger(LeaseTableJobTableJobStatusTable.class);

	public LeaseTableJobTableJobStatusTable() throws SQLException {
		logger.debug("Call LeaseTableJobTableJobStatusTable constructor");
	}
	
	public List<String> executeSelectToRetrieveJobIdByLease(int[] jobStatusType, String leaseId, Calendar maxLeaseTime, String userId, Connection connection) throws SQLException{
		logger.debug("Begin executeSelectToRetrieveJobIdByLease");
		List<String> jobIdList = new ArrayList<String>();
		PreparedStatement selectToRetrieveJobIdPreparedStatement = connection.prepareStatement(getSelectToRetrieveJobIdQuery(jobStatusType, leaseId, maxLeaseTime, userId));
		// execute query, and return number of rows created
		ResultSet rs = selectToRetrieveJobIdPreparedStatement.executeQuery();
        if(rs != null) {
          while (rs.next()) {
            jobIdList.add(rs.getString(JobTable.ID_FIELD));
          }
        }
		logger.debug("End executeSelectToRetrieveJobIdByLease");
		return jobIdList;
	}
	
	public static String getSelectToRetrieveJobIdQuery(int[] jobStatusType, String leaseId, Calendar maxLeaseTime, String userId) {
		StringBuffer selectToRetrieveJobIdQuery = new StringBuffer();
		selectToRetrieveJobIdQuery.append("select ");
		selectToRetrieveJobIdQuery.append(JobTable.NAME_TABLE + "." + JobTable.ID_FIELD + " AS " + JobTable.ID_FIELD);
		selectToRetrieveJobIdQuery.append(" from " + JobTable.NAME_TABLE);
		
		if ((jobStatusType != null) && (jobStatusType.length>0)){
            selectToRetrieveJobIdQuery.append(", " +JobStatusTable.NAME_TABLE + " AS " + JobStatusTable.NAME_TABLE + " LEFT OUTER JOIN " + JobStatusTable.NAME_TABLE + " AS jslatest ");          
            selectToRetrieveJobIdQuery.append("ON jslatest." + JobStatusTable.JOB_ID_FIELD +  " = " + JobStatusTable.NAME_TABLE + "." + JobStatusTable.JOB_ID_FIELD); 
            selectToRetrieveJobIdQuery.append(" and " + JobStatusTable.NAME_TABLE + "." + JobStatusTable.ID_FIELD + " < jslatest." + JobStatusTable.ID_FIELD); 
		}
		
		if (maxLeaseTime != null){
			selectToRetrieveJobIdQuery.append(", " + LeaseTable.NAME_TABLE);
		}
		
		//trick
		selectToRetrieveJobIdQuery.append(" where true ");

		if (userId != null){
			selectToRetrieveJobIdQuery.append(" and " + JobTable.NAME_TABLE + "." + JobTable.USER_ID_FIELD + " = " + "'" + userId + "'");
		}

		if (leaseId != null){
			selectToRetrieveJobIdQuery.append(" and " + JobTable.NAME_TABLE + "." + JobTable.LEASE_ID_FIELD + " = " + "'" + leaseId + "'");
		}

		if (maxLeaseTime != null){
			selectToRetrieveJobIdQuery.append(" and " + JobTable.NAME_TABLE + "." + JobTable.LEASE_ID_FIELD + " = " + LeaseTable.NAME_TABLE + "." + LeaseTable.LEASE_ID_FIELD);
			Timestamp maxLeaseTimestampField =new java.sql.Timestamp(maxLeaseTime.getTimeInMillis());
		    selectToRetrieveJobIdQuery.append(" and " + LeaseTable.LEASE_TIME_FIELD + " <= " + "'" + maxLeaseTimestampField.toString() + "'");
		}

		if ((jobStatusType != null) && (jobStatusType.length>0)){
			  selectToRetrieveJobIdQuery.append(" and jslatest." + JobStatusTable.ID_FIELD + " IS NULL");  
			  StringBuffer jobStatusTypeList = new StringBuffer();
			  for (int i = 0; i<jobStatusType.length; i++){
				  jobStatusTypeList.append(", " + "'" + jobStatusType[i] + "'");
			  }
			  jobStatusTypeList.deleteCharAt(0);
			  selectToRetrieveJobIdQuery.append(" and " + JobStatusTable.NAME_TABLE + "." + JobStatusTable.TYPE_FIELD  + " IN (" + jobStatusTypeList.toString() + ")");
			  selectToRetrieveJobIdQuery.append(" and " + JobStatusTable.NAME_TABLE + "." + JobStatusTable.JOB_ID_FIELD + " = " + JobTable.NAME_TABLE + "." + JobTable.ID_FIELD);
		}	
        
		logger.debug("selectToRetrieveJobIdQuery = " + selectToRetrieveJobIdQuery.toString());
		return selectToRetrieveJobIdQuery.toString();
	}

}
