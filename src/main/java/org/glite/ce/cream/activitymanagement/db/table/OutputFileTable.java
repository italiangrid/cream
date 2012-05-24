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

import org.glite.ce.creamapi.activitymanagement.db.table.OutputFileTableInterface;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.CreationFlagEnumeration;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.OptionType;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.OutputFile;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.Target;

public class OutputFileTable implements OutputFileTableInterface {
    private static final Logger logger = Logger.getLogger(OutputFileTable.class.getName());
    private static final String insertOutputFileSP = "{call insertOutputFileTable (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)}";
    private static final String insertOptionTypeOutputFileSP = "{call insertOptionTypeOutputFileTable (?, ?, ?)}";
    private static final String selectOutputFileSP = "{call selectOutputFileTable (?)}";
    private static final String selectOptionTypeOutputFileSP = "{call selectOptionTypeOutputFileTable (?)}";
    
    public void executeInsert(String activityId, OutputFile outputFile, Connection connection) throws SQLException {  
        CallableStatement insertOutputFileCS = null;
        CallableStatement insertOptionTypeOutputFileCS = null;
        try {
            insertOutputFileCS = connection.prepareCall(insertOutputFileSP);
            insertOptionTypeOutputFileCS = connection.prepareCall(insertOptionTypeOutputFileSP);

            insertOutputFileCS.setString(1, activityId);
            insertOutputFileCS.setString(2, outputFile.getName());
            
            long outputFileId = 0;
            for (Target target : outputFile.getTarget()) {
                insertOutputFileCS.setString(3, target.getURI());
                insertOutputFileCS.setString(4, target.getDelegationID());
                insertOutputFileCS.setBoolean(5, target.isMandatory());
                if (target.getCreationFlag() !=null) {
                    insertOutputFileCS.setString(6, target.getCreationFlag().value());
                } else {
                    insertOutputFileCS.setString(6, null);
                }
                insertOutputFileCS.setBoolean(7, target.isUseIfFailure());
                insertOutputFileCS.setBoolean(8, target.isUseIfCancel());
                insertOutputFileCS.setBoolean(9, target.isUseIfSuccess());
                insertOutputFileCS.registerOutParameter(10, Types.BIGINT);
                insertOutputFileCS.execute();
                outputFileId = insertOutputFileCS.getLong(10);
                if (target.getOption().size() != 0) {
                    for (OptionType optionType : target.getOption()) {
                        insertOptionTypeOutputFileCS.setLong(1, outputFileId);
                        insertOptionTypeOutputFileCS.setString(2, optionType.getName());
                        insertOptionTypeOutputFileCS.setString(3, optionType.getValue());
                        insertOptionTypeOutputFileCS.execute();
                    }
                }
            }
            if (outputFileId == 0) { // no target
                insertOutputFileCS.setString(3, null);
                insertOutputFileCS.setString(4, null);
                insertOutputFileCS.setBoolean(5, false);
                insertOutputFileCS.setString(6, null);
                insertOutputFileCS.setBoolean(7, false);
                insertOutputFileCS.setBoolean(8, false);
                insertOutputFileCS.setBoolean(9, false);
                insertOutputFileCS.registerOutParameter(10, Types.BIGINT);
                insertOutputFileCS.execute();
            }
            logger.debug("insert into OutputFileTable done");
        } catch(SQLException se){
           throw se;            
        } finally {
            //finally block used to close resources
            try{
               if(insertOutputFileCS != null) 
                   insertOutputFileCS.close();
            } catch(SQLException se2) {
            } // nothing we can do
            try{
                if(insertOptionTypeOutputFileCS != null) 
                    insertOptionTypeOutputFileCS.close();
             } catch(SQLException se2) {
             } // nothing we can do
        }
    }

    public List<OutputFile> executeSelect(String activityId, Connection connection) throws SQLException {
        CallableStatement selectOutputFileCS = null;
        CallableStatement selectOptionTypeOutputFileCS = null;
        List<OutputFile> outputFileList = null;
        try {
            selectOutputFileCS = connection.prepareCall(selectOutputFileSP);
            selectOptionTypeOutputFileCS = connection.prepareCall(selectOptionTypeOutputFileSP);

            selectOutputFileCS.setString(1, activityId);
            if (selectOutputFileCS.execute()) {
                outputFileList = new ArrayList<OutputFile>(0);
                OutputFile outputFile = null;
                Target target = null;
                OptionType optionType = null;
                String fileNamePrec = null;
                ResultSet optionTypeResultSet = null;
                ResultSet resultSet = selectOutputFileCS.getResultSet();
                while (resultSet.next()) {
                    if (resultSet.getString(OutputFileTableInterface.NAME_LABEL) != fileNamePrec) {
                        fileNamePrec = resultSet.getString(OutputFileTableInterface.NAME_LABEL);
                        outputFile = new OutputFile();
                        outputFileList.add(outputFile);
                        outputFile.setName(resultSet.getString(OutputFileTableInterface.NAME_LABEL));
                    } 
                    if (resultSet.getString(OutputFileTableInterface.URI_LABEL) != null) {
                        target = new Target();
                        outputFile.getTarget().add(target);
                        if (resultSet.getString(OutputFileTableInterface.CREATION_FLAG_LABEL) != null){
                            target.setCreationFlag(CreationFlagEnumeration.fromValue(resultSet.getString(OutputFileTableInterface.CREATION_FLAG_LABEL)));
                        } 
                        target.setDelegationID(resultSet.getString(OutputFileTableInterface.DELEGATION_ID_LABEL));
                        target.setMandatory(resultSet.getBoolean(OutputFileTableInterface.MANDATORY_LABEL));
                        target.setURI(resultSet.getString(OutputFileTableInterface.URI_LABEL));
                        target.setUseIfCancel(resultSet.getBoolean(OutputFileTableInterface.USE_IF_CANCEL_LABEL));
                        target.setUseIfFailure(resultSet.getBoolean(OutputFileTableInterface.USE_IF_FAILURE_LABEL));
                        target.setUseIfSuccess(resultSet.getBoolean(OutputFileTableInterface.USE_IF_SUCCESS_LABEL));
                        selectOptionTypeOutputFileCS.setLong(1, resultSet.getLong(OutputFileTableInterface.ID_LABEL));
                        if (selectOptionTypeOutputFileCS.execute()) {
                            optionTypeResultSet = selectOptionTypeOutputFileCS.getResultSet();
                            while (optionTypeResultSet.next()) {
                                optionType = new OptionType();
                                optionType.setName(optionTypeResultSet.getString(OutputFileTableInterface.OPTIONTYPE_OUTPUTFILE_NAME_LABEL));
                                optionType.setValue(optionTypeResultSet.getString(OutputFileTableInterface.OPTIONTYPE_OUTPUTFILE_VALUE_LABEL));
                                target.getOption().add(optionType);
                            }    
                        }
                    }
                }
            } // while
            logger.debug("select OutputFile done");
        } catch(SQLException se) {
            throw se;            
        } finally {
            //finally block used to close resources
            try{
                if(selectOutputFileCS != null) 
                    selectOutputFileCS.close();
            } catch(SQLException se2) {
            } // nothing we can do
            try{
                if(selectOptionTypeOutputFileCS != null) 
                    selectOptionTypeOutputFileCS.close();
            } catch(SQLException se2) {
            } // nothing we can do
        }
        return outputFileList;
    }
}
