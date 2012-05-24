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
 * Authors: L. Zangrando, <zangrando@pd.infn.it>
 *
 */

package org.glite.ce.cream.activitymanagement.cmdexecutor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import org.apache.log4j.Logger;
import org.glite.ce.creamapi.activitymanagement.Activity;
import org.glite.ce.creamapi.activitymanagement.ActivityException;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.ActivityTypeEnumeration;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.Application;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.DataStaging;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.ExecutableType;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.InputFile;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.OptionType;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.OutputFile;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.Source;
import org.glite.ce.creamapi.activitymanagement.wrapper.adl.Target;

public class ActivityWrapper {
    private static final Logger logger = Logger.getLogger(ActivityWrapper.class.getName());
    private static final Hashtable<String, String> wrapperTemplateHashTable = new Hashtable<String, String>(0);

    public static final String DELEGATION_TIME_SLOT_DEFAULT_VALUE = "3600"; // sec.
    public static final String COPY_PROXY_MIN_RETRY_WAIT_DEFAULT_VALUE = "60"; // sec.
    public static final String COPY_RETRY_COUNT_ISB_DEFAULT_VALUE = "2";
    public static final String COPY_RETRY_FIRST_WAIT_ISB_DEFAULT_VALUE = "60"; // sec.
    public static final String COPY_RETRY_COUNT_OSB_DEFAULT_VALUE = "6";
    public static final String COPY_RETRY_FIRST_WAIT_OSB_DEFAULT_VALUE = "300"; // sec.
    public static final String TEMPLATE_NAME = "activitywrapper.tpl";

    public static String filenameNorm(String str) {
        if (str == null || str.length() == 0)
            return "";

        StringBuffer buff = new StringBuffer();
        int start = 0;
        int end = str.length();

        if (str.charAt(0) == '"' && str.charAt(end - 1) == '"') {
            start++;
            end--;
        }

        for (int k = start; k < end; k++) {
            if (str.charAt(k) == ' ')
                buff.append("\\ ");
            else
                buff.append(str.charAt(k));
        }
        return buff.toString();
    }

    public static String stringNorm(String str) {
        StringBuffer buff = new StringBuffer("\"");

        for (int k = 0; k < str.length(); k++) {
            if ((str.charAt(k) == '"') && ((k == 0) || ((k != 0) && (str.charAt(k - 1) != '\\')))) {
                buff.append("\\\"");
            } else if (str.charAt(k) == '$') {
                buff.append("\\$");
            } else {
                buff.append(str.charAt(k));
            }
        }
        buff.append("\"");
        return buff.toString();
    }

    public static String buildWrapper(Activity activity) throws ActivityException {
        Application application = activity.getApplication();
        if (application == null) {
            throw new ActivityException("Missing the application");
        }

        ExecutableType executable = application.getExecutable();
        if (executable == null) {
            throw new ActivityException("Missing the executable");
        }

        String executablePath = executable.getPath();
        if (executablePath == null || executablePath.equals("")) {
            throw new ActivityException("Missing the executable path");
        }

        if (!executablePath.startsWith("/") && !executablePath.startsWith("./")) {
            executablePath = "./" + executablePath;
        }

        String serviceURL = activity.getProperties().get(Activity.SERVICE_URL);
        if (serviceURL == null) {
            throw new ActivityException("Missing the service url");
        }

        String activityId = activity.getId();
        if (activityId == null) {
            throw new ActivityException("Missing the activityId");
        }

        String ceId = activity.getVolatileProperties().get(Activity.CE_ID);
        String activityIdURI = serviceURL.substring(0, serviceURL.indexOf("ce-cream-es")) + activityId;

        int nodes = 1; // TBD

        if (nodes == 0 && ActivityTypeEnumeration.PARALLELELEMENT.compareTo(activity.getActivityIdentification().getType()) == 0) {
            throw new ActivityException("Missing node number for mpich job");
        }

        String loggerDestURI = activity.getVolatileProperties().get(Activity.ACTIVITY_WRAPPER_NOTIFICATION_STATUS_URI);
        if (loggerDestURI == null || loggerDestURI.length() == 0) {
            loggerDestURI = "\"\"";
        }

        String vo = activity.getProperties().get(Activity.VIRTUAL_ORGANISATION);
        if (vo == null) {
            vo = "";
        }

        String ceHostName = activity.getVolatileProperties().get(Activity.CE_HOSTNAME);
        if (ceHostName == null) {
            ceHostName = "";
        }

        String copyRetryCountISB = activity.getVolatileProperties().get(Activity.COPY_RETRY_COUNT_ISB);
        try {
            Integer.parseInt(copyRetryCountISB);
        } catch (NumberFormatException nfe) {
            copyRetryCountISB = COPY_RETRY_COUNT_ISB_DEFAULT_VALUE;
            logger.warn("COPY_RETRY_COUNT_ISB_DEFAULT must be integer. So it'll be replaced with the default value:" + COPY_RETRY_COUNT_ISB_DEFAULT_VALUE);
        }

        String copyRetryFirstWaitISB = activity.getVolatileProperties().get(Activity.COPY_RETRY_FIRST_WAIT_ISB);
        try {
            Integer.parseInt(copyRetryFirstWaitISB);
        } catch (NumberFormatException nfe) {
            copyRetryFirstWaitISB = COPY_RETRY_FIRST_WAIT_ISB_DEFAULT_VALUE;
            logger.warn("COPY_RETRY_FIRST_WAIT_ISB_DEFAULT must be integer. So it'll be replaced with the default value: " + COPY_RETRY_FIRST_WAIT_ISB_DEFAULT_VALUE);
        }

        String copyRetryCountOSB = activity.getVolatileProperties().get(Activity.COPY_RETRY_COUNT_OSB);
        try {
            Integer.parseInt(copyRetryCountOSB);
        } catch (NumberFormatException nfe) {
            copyRetryCountOSB = COPY_RETRY_COUNT_OSB_DEFAULT_VALUE;
            logger.warn("COPY_RETRY_COUNT_OSB_DEFAULT must be integer. So it'll be replaced with the default value: " + COPY_RETRY_COUNT_OSB_DEFAULT_VALUE);
        }

        String copyRetryFirstWaitOSB = activity.getVolatileProperties().get(Activity.COPY_RETRY_FIRST_WAIT_OSB);
        try {
            Integer.parseInt(copyRetryFirstWaitOSB);
        } catch (NumberFormatException nfe) {
            copyRetryFirstWaitOSB = COPY_RETRY_FIRST_WAIT_OSB_DEFAULT_VALUE;
            logger.warn("COPY_FIRST_WAIT_OSB must be integer. So it'll be replaced with the default value: " + COPY_RETRY_FIRST_WAIT_OSB_DEFAULT_VALUE);
        }

        String copyProxyMinRetryWait = activity.getVolatileProperties().get(Activity.COPY_PROXY_MIN_RETRY_WAIT);
        try {
            Integer.parseInt(copyProxyMinRetryWait);
        } catch (NumberFormatException nfe) {
            copyProxyMinRetryWait = COPY_PROXY_MIN_RETRY_WAIT_DEFAULT_VALUE;
            logger.warn("COPY_PROXY_MIN_RETRY_WAIT_DEFAULT must be integer. So it'll be replaced with the default value: " + COPY_PROXY_MIN_RETRY_WAIT_DEFAULT_VALUE);
        }

        StringBuffer wrapper = new StringBuffer("#!/bin/sh -l\n");
        wrapper.append("__create_subdir=1\n");
        wrapper.append("export CE_ID=").append(ceId).append("\n");

        String delegationTimeSlot = null;
        String delegationFileNameSuffix = activity.getVolatileProperties().get(Activity.DELEGATION_FILE_NAME_SUFFIX);
        String delegationSandboxURI = activity.getProperties().get(Activity.DELEGATION_SANDBOX_URI);
        String delegationSandboxPath = activity.getProperties().get(Activity.DELEGATION_SANDBOX_PATH);
        
        wrapper.append("export ES_ACTIVITYID_URI=").append(activityIdURI).append("\n");
        wrapper.append("__vo=").append(vo).append("\n");
        wrapper.append("__esActivityId=").append(activityId).append("\n");
        wrapper.append("__executable=").append(executablePath).append("\n");
        if (executable.getFailIfExitCodeNotEqualTo() != null) {
            wrapper.append("__executableExitCode=").append(executable.getFailIfExitCodeNotEqualTo().longValue()).append("\n");
        }
        wrapper.append("__working_directory=").append(activityId);
        wrapper.append("\n__ce_hostname=").append(ceHostName).append("\n");

        StringBuffer cmdLine = new StringBuffer("\"");
        cmdLine.append(executablePath).append("\" ");

        if (executable.getArgument() != null) {
            for (String argument : executable.getArgument()) {
                cmdLine.append(argument).append(" ");
            }
        }

        cmdLine.append("$* ");

        String stdi = application.getInput();
        if (stdi != null && !stdi.equals("")) {
            cmdLine.append("< \"").append(stdi).append("\" ");
        }

        String stdo = application.getOutput();
        if (stdo != null && !stdo.equals("")) {
            cmdLine.append("> \"").append(stdo).append("\" ");
            wrapper.append("__stdout_file=\"").append(stdo).append("\"\n");
        } else {
            cmdLine.append("> /dev/null ");
        }

        String stde = application.getError();
        if (stde != null && !stde.equals("")) {
            cmdLine.append(stde.equals(stdo) ? "2>&1" : "2> \"" + stde + "\"");
            wrapper.append("__stderr_file=\"").append(stde).append("\"\n");
        } else {
            cmdLine.append("2> /dev/null");
        }

        wrapper.append("__cmd_line=").append(stringNorm(cmdLine.toString())).append("\n");
        wrapper.append("__logger_dest=").append(loggerDestURI).append("\n");
        wrapper.append("__nodes=").append(nodes).append("\n");
        wrapper.append("export __delegationTimeSlot=").append(delegationTimeSlot).append("\n");
        wrapper.append("export __copy_proxy_min_retry_wait=").append(copyProxyMinRetryWait).append("\n");
        wrapper.append("__copy_retry_count_isb=").append(copyRetryCountISB).append("\n");
        wrapper.append("__copy_retry_first_wait_isb=").append(copyRetryFirstWaitISB).append("\n");
        wrapper.append("__copy_retry_count_osb=").append(copyRetryCountOSB).append("\n");
        wrapper.append("__copy_retry_first_wait_osb=").append(copyRetryFirstWaitOSB).append("\n");

        // preExecutable
        List<ExecutableType> preExecutableList = application.getPreExecutable();
        if (preExecutableList != null && preExecutableList.size() > 0) {
            wrapper.append("declare -a __preExecutable_path\n\n");
            wrapper.append("declare -a __preExecutable_exitCode\n\n");
            wrapper.append("declare -a __preExecutable_arguments\n\n");
            String preExecutablePath = null;
            String preExecutableArgs = null;
            for (int i = 0; i < preExecutableList.size(); i++) {
                preExecutablePath = preExecutableList.get(i).getPath();
                if ((preExecutablePath == null) || ("".equals(preExecutablePath))) {
                    throw new ActivityException("At least a preExecutable path is empty.");
                }
                if (!preExecutablePath.startsWith("/")) {
                    preExecutablePath = "./" + preExecutablePath;
                }
                wrapper.append("__preExecutable_path[" + i + "]=").append(preExecutablePath).append("\n");

                preExecutableArgs = "";
                for (String argument : preExecutableList.get(i).getArgument()) {
                    preExecutableArgs += argument + " ";
                }
                wrapper.append("__preExecutable_arguments[" + i + "]=").append(stringNorm(preExecutableArgs.trim())).append("\n");

                wrapper.append("__preExecutable_exitCode[" + i + "]=");
                if (preExecutableList.get(i).getFailIfExitCodeNotEqualTo() != null) {
                    wrapper.append(preExecutableList.get(i).getFailIfExitCodeNotEqualTo()).append("\n");
                } else {
                    wrapper.append("XXX").append("\n"); // trick
                }
            }
        }

        // postExecutable
        List<ExecutableType> postExecutableList = application.getPostExecutable();
        if (postExecutableList != null && postExecutableList.size() > 0) {
            wrapper.append("declare -a __postExecutable_path\n\n");
            wrapper.append("declare -a __postExecutable_exitCode\n\n");
            wrapper.append("declare -a __postExecutable_arguments\n\n");
            String postExecutablePath = null;
            String postExecutableArgs = null;
            for (int i = 0; i < postExecutableList.size(); i++) {
                postExecutablePath = postExecutableList.get(i).getPath();
                if ((postExecutablePath == null) || ("".equals(postExecutablePath))) {
                    throw new ActivityException("At least a postExecutable path is empty.");
                }
                if (!postExecutablePath.startsWith("/")) {
                    postExecutablePath = "./" + postExecutablePath;
                }
                wrapper.append("__postExecutable_path[" + i + "]=").append(postExecutablePath).append("\n");

                postExecutableArgs = "";
                for (String argument : postExecutableList.get(i).getArgument()) {
                    postExecutableArgs += argument + " ";
                }
                wrapper.append("__postExecutable_arguments[" + i + "]=").append(stringNorm(postExecutableArgs.trim())).append("\n");

                wrapper.append("__postExecutable_exitCode[" + i + "]=");
                if (postExecutableList.get(i).getFailIfExitCodeNotEqualTo() != null) {
                    wrapper.append(postExecutableList.get(i).getFailIfExitCodeNotEqualTo()).append("\n");
                } else {
                    wrapper.append("XXX").append("\n"); // trick
                }
            }
        }

        // environment
        List<OptionType> optionTypeList = application.getEnvironment();
        if (optionTypeList != null) {
            wrapper.append("declare -a __environment\n\n");
            int counter = 0;
            for (OptionType optionType : optionTypeList) {
                wrapper.append("__environment[" + counter + "]=");
                wrapper.append(stringNorm(optionType.getName() + "=" + optionType.getValue()));
                wrapper.append("\n");
                counter++;
            }
        }

        DataStaging dataStaging = activity.getDataStaging();
        List<String> proxyFileList = new ArrayList<String>(0);
        boolean useProxyRenewal = false;

        if (dataStaging != null) {
            String delegationFileName = null;
            int lrmsInputFileIndex = 0;
            StringBuffer lrmsInputFile = new StringBuffer();
            StringBuffer lrmsInputFileIsExecutableAttribute = new StringBuffer();
            StringBuffer transferInput = new StringBuffer();

            int lrmsOutputFileIndex = 0;
            StringBuffer lrmsOutputFile = new StringBuffer();
            StringBuffer transferOutput = new StringBuffer();
            StringBuffer transferOutputRemaps = new StringBuffer();
            
            if (dataStaging.getInputFile().size() > 0) {
                int inputFileIndex = 0;
                String sourceURL = null;

                wrapper.append("declare -a __input_file_url\n");
                wrapper.append("declare -a __input_file_dest\n");
                wrapper.append("declare -a __input_transfer_cmd\n");
                wrapper.append("declare -a __input_file_isExecutable\n");
                wrapper.append("declare -a __input_proxy_file\n");
                
                for (InputFile inputFile : dataStaging.getInputFile()) {

                    if (inputFile.getSource().size() == 0) { // clientDataPush
                        lrmsInputFile.append("__lrms_input_file[").append(lrmsInputFileIndex).append("]=").append(stringNorm(inputFile.getName())).append("\n");
                        transferInput.append(activity.getProperties().get(Activity.SANDBOX_PATH)).append("/ISB/").append(inputFile.getName()).append(",");
                        lrmsInputFileIsExecutableAttribute.append("__lrms_input_file_isExecutable[").append(lrmsInputFileIndex).append("]=").append((inputFile.isIsExecutable().booleanValue() ? 1 : 0)).append("\n");
                        lrmsInputFileIndex++;
                    } else {
                        for (Source source : inputFile.getSource()) {
                            sourceURL = source.getURI();

                            if (sourceURL == null) {
                                throw new ActivityException("Error: the source URL is empty for the input file " + inputFile.getName());
                            }

                            if ((source.getDelegationID() == null) || ("".equals(source.getDelegationID()))) {
                                throw new ActivityException("Error: delegationId is empty for the input file " + inputFile.getName());
                            }

                            wrapper.append("__input_file_url[").append(inputFileIndex).append("]=").append(stringNorm(sourceURL)).append("\n");
                            wrapper.append("__input_file_dest[").append(inputFileIndex).append("]=").append(stringNorm(inputFile.getName())).append("\n");
                            wrapper.append("__input_file_isExecutable[").append(inputFileIndex).append("]=").append((inputFile.isIsExecutable().booleanValue() ? 1 : 0)).append("\n");
                            
                            if ((sourceURL.startsWith("gsiftp")) || (sourceURL.startsWith("file"))) {
                                wrapper.append("__input_transfer_cmd[").append(inputFileIndex).append("]=\"\\${globus_transfer_cmd}\"\n");
                            } else if (sourceURL.startsWith("https://")) {
                                wrapper.append("__input_transfer_cmd[").append(inputFileIndex).append("]=\"\\${https_transfer_cmd}\"\n");
                            } else {
                                throw new ActivityException("Error: unsupported protocol for source URL : " + sourceURL);
                            }

                            delegationFileName = (source.getDelegationID() + "_" + delegationFileNameSuffix).replaceAll("\\W", "_");
                            wrapper.append("__input_proxy_file[").append(inputFileIndex).append("]=").append(stringNorm(delegationFileName)).append("\n");                            
                            if (!proxyFileList.contains(delegationFileName)) {
                                proxyFileList.add(delegationFileName);
                                lrmsInputFile.append("__lrms_input_file[").append(lrmsInputFileIndex).append("]=").append(stringNorm(delegationFileName)).append("\n");
                                transferInput.append(delegationSandboxPath + delegationFileName).append(",");
                                lrmsInputFileIsExecutableAttribute.append("__lrms_input_file_isExecutable[").append(lrmsInputFileIndex).append("]=0").append("\n");
                                lrmsInputFileIndex++;
                            }
                            inputFileIndex++;
                        } // for source
                    }
                } // for inputFile
            }

            wrapper.append("\n");

            if (dataStaging.getOutputFile().size() > 0) {
                int outputFileIndex = 0;
                String targetURL = null;

                wrapper.append("declare -a __output_file\n");
                wrapper.append("declare -a __output_transfer_cmd\n");
                wrapper.append("declare -a __output_file_dest\n");
                wrapper.append("declare -a __output_file_flag\n");
                wrapper.append("declare -a __output_proxy_file\n");

                for (OutputFile outputFile : dataStaging.getOutputFile()) {
                    if (outputFile.getTarget().size() == 0) {
                        lrmsOutputFile.append("__lrms_output_file[").append(lrmsOutputFileIndex++).append("]=").append(stringNorm(outputFile.getName())).append("\n");
                        transferOutput.append(outputFile.getName()).append(",");
                        transferOutputRemaps.append(outputFile.getName()).append("=").append(activity.getProperties().get(Activity.SANDBOX_PATH)).append("/OSB/").append(outputFile.getName()).append(";");
                    } else {
                        StringBuffer outputFileFlag = null;
                        for (Target target : outputFile.getTarget()) {
                            targetURL = target.getURI();

                            if (targetURL == null) {
                                throw new ActivityException("Error: the source URL is empty for the output file " + outputFile.getName());
                            }

                            if ((target.getDelegationID() == null) || ("".equals(target.getDelegationID()))) {
                                throw new ActivityException("Error: delegationId is empty for the output file " + outputFile.getName());
                            }

                            String fName = filenameNorm(outputFile.getName());
                            if (fName.indexOf('/') != 0) {
                                fName = "${workdir}/" + fName;
                            }

                            wrapper.append("__output_file[").append(outputFileIndex).append("]=").append(stringNorm(fName)).append("\n");
                            wrapper.append("__output_file_dest[").append(outputFileIndex).append("]=").append(stringNorm(targetURL)).append("\n");
                            
                            outputFileFlag = new StringBuffer();
                            outputFileFlag.append((target.isMandatory() ? "1" : "0"));
                            outputFileFlag.append((target.isUseIfCancel() ? "1" : "0"));
                            outputFileFlag.append((target.isUseIfFailure() ? "1" : "0"));
                            outputFileFlag.append((target.isUseIfSuccess() ? "1" : "0"));
                            
                            switch (target.getCreationFlag()){
                            case OVERWRITE:
                                outputFileFlag.append("0");
                                break;
                            case APPEND:
                                outputFileFlag.append("1");
                                break;
                            case DONT_OVERWRITE:
                                outputFileFlag.append("2");
                            }
                            wrapper.append("__output_file_flag[").append(outputFileIndex).append("]=").append(outputFileFlag).append("\n");
                            
                            if (targetURL.startsWith("gsiftp")) {
                                wrapper.append("__output_transfer_cmd[").append(outputFileIndex).append("]=\"\\${globus_transfer_cmd}\"\n");
                            } else if (targetURL.startsWith("https")) {
                                wrapper.append("__output_transfer_cmd[").append(outputFileIndex).append("]=\"\\${https_transfer_cmd}\"\n");
                            } else {
                                throw new ActivityException("Error: unsupported protocol for target URL : " + targetURL);
                            }
                            delegationFileName = (target.getDelegationID() + "_" + delegationFileNameSuffix).replaceAll("\\W", "_");
                            wrapper.append("__output_proxy_file[").append(outputFileIndex).append("]=").append(stringNorm(delegationFileName)).append("\n");
                            if (!proxyFileList.contains(delegationFileName)) {
                                proxyFileList.add(delegationFileName);
                                lrmsInputFile.append("__lrms_input_file[").append(lrmsInputFileIndex).append("]=").append(stringNorm(delegationFileName)).append("\n");
                                transferInput.append(delegationSandboxPath + delegationFileName).append(",");
                                lrmsInputFileIsExecutableAttribute.append("__lrms_input_file_isExecutable[").append(lrmsInputFileIndex).append("]=0").append("\n");
                                lrmsInputFileIndex++;

                            }
                            outputFileIndex++;
                        } // for target
                    }
                } // for outputFile
            }
            
            if (lrmsInputFileIndex > 0) {
                wrapper.append("declare -a __lrms_input_file\n");
                wrapper.append(lrmsInputFile).append("\n");
                activity.getProperties().put(Activity.TRANSFER_INPUT, transferInput.substring(0, transferInput.length() - 1).toString());
                
                wrapper.append("declare -a __lrms_input_file_isExecutable\n");
                wrapper.append(lrmsInputFileIsExecutableAttribute).append("\n");
            }

            if (lrmsOutputFileIndex > 0) {
                wrapper.append("\ndeclare -a __lrms_output_file\n\n");
                wrapper.append(lrmsOutputFile).append("\n");
                activity.getProperties().put(Activity.TRANSFER_OUTPUT, transferOutput.substring(0, transferOutput.length() - 1).toString());
                activity.getProperties().put(Activity.TRANSFER_OUTPUT_REMAPS, transferOutputRemaps.substring(0, transferOutputRemaps.length() - 1).toString());
            }

        } // end dataStaging

        if (proxyFileList.size() == 0) {
            // no ProxyRenewal
            useProxyRenewal = false;
            logger.debug("No PROXY_RENEWAL for activityId = " + activityId);
            delegationTimeSlot = "-1";
        } else {
            useProxyRenewal = true;
            wrapper.append("declare -a __all_proxy_file\n");
            for (int i= 0; i<proxyFileList.size(); i++) {
                wrapper.append("__all_proxy_file[").append(i).append("]=").append(stringNorm(proxyFileList.get(i))).append("\n");                
            }

            delegationTimeSlot = activity.getVolatileProperties().get(Activity.DELEGATION_TIME_SLOT);
            try {
                Integer.parseInt(delegationTimeSlot);
            } catch (NumberFormatException nfe) {
                delegationTimeSlot = DELEGATION_TIME_SLOT_DEFAULT_VALUE;
                logger.warn("DELEGATION_TIME_SLOT_DEFAULT must be integer. So it'll be replaced with the default value: " + DELEGATION_TIME_SLOT_DEFAULT_VALUE);
            }
            wrapper.append("__delegationTimeSlot=").append(delegationTimeSlot).append("\n");
            wrapper.append("__delegationSandboxURI=").append(delegationSandboxURI).append("\n");
        }
        
        wrapper.append("\n__useProxyRenewal=").append(useProxyRenewal ? "1" : "0").append("\n\n");

        String templatePathName = activity.getVolatileProperties().get(Activity.TEMPLATE_PATH) + File.separator + TEMPLATE_NAME;

        if (wrapperTemplateHashTable.get(templatePathName) == null) {
            wrapperTemplateHashTable.put(templatePathName, getWrapperTemplate(templatePathName));
        }

        wrapper.append(wrapperTemplateHashTable.get(templatePathName));
        return wrapper.toString();
    }

    private static String getWrapperTemplate(String templatePathName) throws ActivityException {
        FileReader templateFileReader = null;

        try {
            templateFileReader = new FileReader(templatePathName);
        } catch (FileNotFoundException fnf) {
            throw new ActivityException("Cannot find the activity wrapper template from file: " + templatePathName);
        }

        StringBuffer wrapperTemplate = new StringBuffer();
        BufferedReader in = new BufferedReader(templateFileReader);

        try {
            String line = in.readLine();

            while (line != null) {
                wrapperTemplate.append(line + "\n");
                line = in.readLine();
            }
        } catch (IOException ioe) {
            throw new ActivityException("Cannot read the activity wrapper template from file: " + templatePathName);
        } finally {
            try {
                in.close();
                templateFileReader.close();
            } catch (IOException ioe) {
                // nothing.
            }
        }

        return wrapperTemplate.toString();
    }
}
