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
import org.glite.ce.creamapi.activitymanagement.db.table.InputFileTableInterface;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.InputFile;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.OptionType;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.Source;

public class InputFileTable implements InputFileTableInterface {
    private static final Logger logger = Logger.getLogger(InputFileTable.class.getName());
    private static final String insertInputFileSP = "{call insertInputFileTable (?, ?, ?, ?, ?, ?)}";
    private static final String insertOptionTypeInputFileSP = "{call insertOptionTypeInputFileTable (?, ?, ?)}";
    private static final String selectInputFileSP = "{call selectInputFileTable (?)}";
    private static final String selectOptionTypeInputFileSP = "{call selectOptionTypeInputFileTable (?)}";
    
    public void executeInsert(String activityId, InputFile inputFile, Connection connection) throws SQLException {  
        CallableStatement insertInputFileCS = null;
        CallableStatement insertOptionTypeInputFileCS = null;
        try {
            insertInputFileCS = connection.prepareCall(insertInputFileSP);
            insertOptionTypeInputFileCS = connection.prepareCall(insertOptionTypeInputFileSP);
            
            insertInputFileCS.setString(1, activityId);
            insertInputFileCS.setString(2, inputFile.getName());
            insertInputFileCS.setBoolean(3, inputFile.isIsExecutable());
            
            long inputFileId = 0;
            for (Source source : inputFile.getSource()) {
                insertInputFileCS.setString(4, source.getURI());
                insertInputFileCS.setString(5, source.getDelegationID());
                insertInputFileCS.registerOutParameter(6, Types.BIGINT);
                insertInputFileCS.execute();
                inputFileId = insertInputFileCS.getLong(6);
                if (source.getOption().size() != 0) {
                    for (OptionType optionType : source.getOption()) {
                        insertOptionTypeInputFileCS.setLong(1, inputFileId);
                        insertOptionTypeInputFileCS.setString(2, optionType.getName());
                        insertOptionTypeInputFileCS.setString(3, optionType.getValue());
                        insertOptionTypeInputFileCS.execute();
                    }
                }
            }
            if (inputFileId == 0) { // no sources
                insertInputFileCS.setString(4, null);
                insertInputFileCS.setString(5, null);
                insertInputFileCS.registerOutParameter(6, Types.BIGINT);
                insertInputFileCS.execute();
            }
            logger.debug("insert into InputFileTable done");
        } catch(SQLException se){
           throw se;            
        } finally {
            //finally block used to close resources
            try{
               if(insertInputFileCS != null) 
                   insertInputFileCS.close();
            } catch(SQLException se2) {
            } // nothing we can do
            try{
                if(insertOptionTypeInputFileCS != null) 
                    insertOptionTypeInputFileCS.close();
             } catch(SQLException se2) {
             } // nothing we can do
        }
    }

    public List<InputFile> executeSelect(String activityId, Connection connection) throws SQLException {
        CallableStatement selectInputFileCS = null;
        CallableStatement selectOptionTypeInputFileCS = null;
        List<InputFile> inputFileList = null;
        try {
            selectInputFileCS = connection.prepareCall(selectInputFileSP);
            selectOptionTypeInputFileCS = connection.prepareCall(selectOptionTypeInputFileSP);
            
            selectInputFileCS.setString(1, activityId);
            if (selectInputFileCS.execute()) {
                inputFileList = new ArrayList<InputFile>(0);
                InputFile inputFile = null;
                Source source = null;
                OptionType optionType = null;
                String fileNamePrec = null;
                ResultSet optionTypeResultSet = null;
                ResultSet resultSet = selectInputFileCS.getResultSet();
                while (resultSet.next()) {
                    if (resultSet.getString(InputFileTableInterface.NAME_LABEL) != fileNamePrec) {
                        fileNamePrec = resultSet.getString(InputFileTableInterface.NAME_LABEL);
                        inputFile = new InputFile();
                        inputFileList.add(inputFile);
                        inputFile.setName(resultSet.getString(InputFileTableInterface.NAME_LABEL));
                        inputFile.setIsExecutable(resultSet.getBoolean(InputFileTableInterface.ISEXECUTABLE_LABEL));
                    } 
                    if (resultSet.getString(InputFileTableInterface.URI_LABEL) != null) {
                        source = new Source();
                        inputFile.getSource().add(source);
                        source.setURI(resultSet.getString(InputFileTableInterface.URI_LABEL));
                        source.setDelegationID(resultSet.getString(InputFileTableInterface.DELEGATION_ID_LABEL));
                    
                        selectOptionTypeInputFileCS.setLong(1, resultSet.getLong(InputFileTableInterface.ID_LABEL));
                        if (selectOptionTypeInputFileCS.execute()) {
                            optionTypeResultSet = selectOptionTypeInputFileCS.getResultSet();
                            while (optionTypeResultSet.next()) {
                                optionType = new OptionType();
                                optionType.setName(optionTypeResultSet.getString(InputFileTableInterface.OPTIONTYPE_INPUTFILE_NAME_LABEL));
                                optionType.setValue(optionTypeResultSet.getString(InputFileTableInterface.OPTIONTYPE_INPUTFILE_VALUE_LABEL));
                                source.getOption().add(optionType);
                            }    
                        }
                    }
                } //while
            }
            logger.debug("select InputFile done");
        } catch(SQLException se) {
           throw se;            
        } finally {
            //finally block used to close resources
            try{
               if(selectInputFileCS != null) 
                   selectInputFileCS.close();
            } catch(SQLException se2) {
            } // nothing we can do
            try{
                if(selectOptionTypeInputFileCS != null) 
                    selectOptionTypeInputFileCS.close();
             } catch(SQLException se2) {
             } // nothing we can do
        }
        return inputFileList;
    }
}
