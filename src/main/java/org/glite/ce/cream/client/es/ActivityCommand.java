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

package org.glite.ce.cream.client.es;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.axis2.AxisFault;
import org.apache.commons.httpclient.protocol.Protocol;
import org.glite.ce.cream.client.CmdLineParser;
import org.glite.ce.creamapi.ws.es.activityinfo.ActivityInfoServiceStub;
import org.glite.ce.creamapi.ws.es.activitymanagement.ActivityManagementServiceStub;
import org.glite.ce.creamapi.ws.es.creation.ActivityCreationServiceStub;
import org.glite.ce.creamapi.ws.es.delegation.DelegationServiceStub;
import org.glite.ce.creamapi.ws.es.resourceinfo.ResourceInfoServiceStub;
import org.glite.security.delegation.GrDPX509Util;
import org.glite.security.delegation.GrDProxyDlgorOptions;
import org.glite.security.delegation.GrDProxyGenerator;
import org.glite.security.trustmanager.ContextWrapper;
import org.glite.security.trustmanager.axis2.AXIS2SocketFactory;

public abstract class ActivityCommand {
    public static final String ADL = "ADL";
    public static final String CANCEL_ACTIVITY = "CANCEL_ACTIVITY";
    public static final String DELEGATION_ID = "DELEGATION_ID";
    public static final String DESTROY = "DESTROY";
    public static final String EPR = "EPR";
//    public static final String FROM_DATE = "FROM_DATE";
    public static final String GET_ACTIVITY_INFO = "GET_ACTIVITY_INFO";
    public static final String GET_ACTIVITY_STATUS = "GET_ACTIVITY_STATUS";
    public static final String GET_INTERFACE_VERSION = "GET_INTERFACE_VERSION";
    public static final String GET_NEW_PROXY_REQUEST = "GET_NEW_PROXY_REQUEST";
    public static final String GET_PROXY_REQUEST = "GET_PROXY_REQUEST";
    public static final String GET_RESOURCE_INFO = "GET_RESOURCE_INFO";
    public static final String GET_TERMINATION_TIME = "GET_TERMINATION_TIME";
    public static final String GET_VERSION = "GET_VERSION";
//    public static final String LIMIT = "LIMIT";
    public static final String LIST_ACTIVITIES = "LIST_ACTIVITIES";
    // public static final String NOTIFY_MESSAGE_TYPE = "NOTIFY_MESSAGE_TYPE";
    public static final String NOTIFY_SERVICE = "NOTIFY_SERVICE";
    public static final String PAUSE_ACTIVITY = "PAUSE_ACTIVITY";
    public static final String PROXY = "PROXY";
    public static final String QUERY_RESOURCE_INFO = "QUERY_RESOURCE_INFO";
    public static final String RENEW_PROXY_REQUEST = "RENEW_PROXY_REQUEST";
    public static final String RESUME_ACTIVITY = "RESUME_ACTIVITY";
    public static final String STATUS = "STATUS";
//    public static final String TO_DATE = "TO_DATE";
    public static final String WIPE_ACTIVITY = "WIPE_ACTIVITY";

    private List<String> activityDescFileList = null;
    private List<String> attributeList = null;
    private String epr = null;
    private Calendar fromDate = null;
    private List<String> idList = null;
    private boolean isCancelActivity = false;
    private boolean isDestroy = false;
    private boolean isGetActivityInfo = false;
    private boolean isGetActivityStatus = false;
    private boolean isGetDelegationInfo = false;
    private boolean isGetInterfaceVersion = false;
    private boolean isGetNewProxyRequest = false;
    private boolean isGetProxyRequest = false;
    private boolean isGetResourceInfo = false;
    private boolean isGetTerminationTime = false;
    private boolean isGetVersion = false;
    private boolean isListActivities = false;
//    private boolean isNotifyService = false;
    private boolean isPauseActivity = false;
    private boolean isQueryResourceInfo = false;
    private boolean isRenew = false;
    private boolean isRenewProxyRequest = false;
    private boolean isResumeActivity = false;
    private boolean isWipeActivity = false;
    private int limit = 0;
    private String notifyMessageType = null;
    private List<String> options = null;

    private String proxy = null;
    private String query = null;
    private List<String> statusList = null;
    private Calendar toDate = null;

    public ActivityCommand(String[] args, List<String> options) throws IllegalArgumentException {
        if (options == null || options.size() == 0) {
            throw new IllegalArgumentException("options not specified!");
        }

        this.options = options;

        parseArguments(args, this.options);

        execute();
    }

    protected abstract void execute();

    public ActivityCreationServiceStub getActivityCreationServiceStub() throws AxisFault {
        if (epr == null) {
            throw new AxisFault("epr not specified!");
        }

        if (epr.startsWith("https")) {
            setSSLProperties();
        }

        return new ActivityCreationServiceStub(epr); // + "/ce-cream-es/services/ActivityCreationService");
    }

    public List<String> getActivityDescFileList() {
        if (activityDescFileList == null) {
            activityDescFileList = new ArrayList<String>(0);
        }
        return activityDescFileList;
    }

    public ActivityInfoServiceStub getActivityInfoServiceStub() throws AxisFault {
        if (epr == null) {
            throw new AxisFault("epr not specified!");
        }

        if (epr.startsWith("https")) {
            setSSLProperties();
        }

        return new ActivityInfoServiceStub(epr); // + "/ce-cream-es/services/ActivityInfoService");
    }

    public ActivityManagementServiceStub getActivityManagementServiceStub() throws AxisFault {
        if (epr == null) {
            throw new AxisFault("epr not specified!");
        }

        if (epr.startsWith("https")) {
            setSSLProperties();
        }

        return new ActivityManagementServiceStub(epr); // + "/ce-cream-es/services/ActivityManagementService");
    }

    public DelegationServiceStub getDelegationServiceStub() throws AxisFault {
        if (epr == null) {
            throw new AxisFault("epr not specified!");
        }

        if (epr.startsWith("https")) {
            setSSLProperties();
        }

        return new DelegationServiceStub(epr); // + "/ce-cream-es/services/DelegationService");
    }

    public String getEpr() {
        return epr;
    }

    public Calendar getFromDate() {
        return fromDate;
    }

    public String[] getIdArray() {
        String[] array = null;
        if (idList != null) {
            array = new String[idList.size()];
            array = idList.toArray(array);
        }
        return array;
    }

    public List<String> getIdList() {
        if (idList == null) {
            idList = new ArrayList<String>(0);
        }
        return idList;
    }

    public List<String> getAttributeList() {
        if (attributeList == null) {
            attributeList = new ArrayList<String>(0);
        }
        return attributeList;
    }
    
    public int getLimit() {
        return limit;
    }

    public String getNotifyMessageType() {
        return notifyMessageType;
    }

    public String getQuery() {
        return query;
    }

    public ResourceInfoServiceStub getResourceInfoServiceStub() throws AxisFault {
        if (epr == null) {
            throw new AxisFault("epr not specified!");
        }

        if (epr.startsWith("https")) {
            setSSLProperties();
        }

        return new ResourceInfoServiceStub(epr); // + "/ce-cream-es/services/ResourceInfoService");
    }

    public List<String> getStatusList() {
        if (statusList == null) {
            statusList = new ArrayList<String>(0);
        }
        return statusList;
    }

    public Calendar getToDate() {
        return toDate;
    }

    public boolean isCancelActivity() {
        return isCancelActivity;
    }

    public boolean isDestroy() {
        return isDestroy;
    }

    public boolean isGetActivityInfo() {
        return isGetActivityInfo;
    }

    public boolean isGetActivityStatus() {
        return isGetActivityStatus;
    }

    public boolean isGetDelegationInfo() {
        return isGetDelegationInfo;
    }

    public boolean isGetInterfaceVersion() {
        return isGetInterfaceVersion;
    }

    public boolean isGetNewProxyRequest() {
        return isGetNewProxyRequest;
    }

    public boolean isGetProxyRequest() {
        return isGetProxyRequest;
    }

    public boolean isGetResourceInfo() {
        return isGetResourceInfo;
    }

    public boolean isGetTerminationTime() {
        return isGetTerminationTime;
    }

    public boolean isGetVersion() {
        return isGetVersion;
    }

    public boolean isListActivities() {
        return isListActivities;
    }

    public boolean isNotifyService() {
        return notifyMessageType != null;
    }

    public boolean isPauseActivity() {
        return isPauseActivity;
    }

    public boolean isQueryResourceInfo() {
        return isQueryResourceInfo;
    }

    public boolean isRenew() {
        return isRenew;
    }

    public boolean isRenewProxyRequest() {
        return isRenewProxyRequest;
    }

    public boolean isResumeActivity() {
        return isResumeActivity;
    }

    public boolean isWipeActivity() {
        return isWipeActivity;
    }

    private void parseArguments(String[] args, List<String> options) {
        CmdLineParser parser = new CmdLineParser();

        CmdLineParser.Option getHelpOpt = parser.addBooleanOption('h', "help");
        CmdLineParser.Option eprOpt = null;
        CmdLineParser.Option statusOpt = null;
        CmdLineParser.Option fromOpt = null;
        CmdLineParser.Option toOpt = null;
        CmdLineParser.Option limitOpt = null;
        CmdLineParser.Option proxyOpt = null;
        CmdLineParser.Option renewOpt = null;
        CmdLineParser.Option activityInfoOpt = null;
        CmdLineParser.Option getVersionOpt = null;
        CmdLineParser.Option getInterfaceVersionOpt = null;
        CmdLineParser.Option getTerminationTimeOpt = null;
        CmdLineParser.Option getNewProxyRequestOpt = null;
        CmdLineParser.Option getProxyRequestOpt = null;
        CmdLineParser.Option renewProxyRequestOpt = null;
        CmdLineParser.Option destroyOpt = null;
        CmdLineParser.Option cancelOpt = null;
        CmdLineParser.Option pauseOpt = null;
        CmdLineParser.Option resumeOpt = null;
        CmdLineParser.Option wipeOpt = null;
        CmdLineParser.Option getStatusOpt = null;
        CmdLineParser.Option notifyServiceOpt = null;
        // CmdLineParser.Option notifyMessageTypeOpt = null;
        CmdLineParser.Option listActivitiesOpt = null;
        CmdLineParser.Option getResourceInfoOpt = null;
        CmdLineParser.Option queryResourceInfoOpt = null;
        CmdLineParser.Option attributeListOpt = null;


        if (options.contains(PROXY)) {
            proxyOpt = parser.addStringOption('x', "proxy");
        }

        if (options.contains(EPR)) {
            eprOpt = parser.addStringOption('e', "epr");
        }

        if (options.contains(STATUS)) {
            statusOpt = parser.addStringOption('s', "status");
        }

//        if (options.contains(FROM_DATE)) {
//            fromOpt = parser.addStringOption('f', "fromDate");
//        }
//
//        if (options.contains(TO_DATE)) {
//            toOpt = parser.addStringOption('t', "toDate");
//        }
//
//        if (options.contains(LIMIT)) {
//            limitOpt = parser.addIntegerOption("limit");
//        }

        if (options.contains(GET_ACTIVITY_INFO)) {
            activityInfoOpt = parser.addBooleanOption('i', "info");
            attributeListOpt = parser.addStringOption('a', "attributes");
        }

        if (options.contains(GET_VERSION)) {
            getVersionOpt = parser.addBooleanOption('v', "version");
        }

        if (options.contains(GET_INTERFACE_VERSION)) {
            getInterfaceVersionOpt = parser.addBooleanOption('i', "interfaceversion");
        }

        if (options.contains(GET_TERMINATION_TIME)) {
            getTerminationTimeOpt = parser.addBooleanOption('t', "termination");
        }

        if (options.contains(GET_PROXY_REQUEST)) {
            getProxyRequestOpt = parser.addBooleanOption('g', "getreq");
        }

        if (options.contains(GET_NEW_PROXY_REQUEST)) {
            getNewProxyRequestOpt = parser.addBooleanOption('n', "newreq");
        }

        if (options.contains(RENEW_PROXY_REQUEST)) {
            renewProxyRequestOpt = parser.addBooleanOption('r', "renewreq");
        }

        if (options.contains(DESTROY)) {
            destroyOpt = parser.addBooleanOption('d', "destroy");
        }

        if (options.contains(GET_RESOURCE_INFO)) {
            getResourceInfoOpt = parser.addBooleanOption('i', "info");
        }

        if (options.contains(QUERY_RESOURCE_INFO)) {
            queryResourceInfoOpt = parser.addStringOption('q', "query");
        }

        if (options.contains(LIST_ACTIVITIES)) {
            listActivitiesOpt = parser.addBooleanOption('l', "listActivities");
            fromOpt = parser.addStringOption('f', "fromDate");
            toOpt = parser.addStringOption('t', "toDate");
            limitOpt = parser.addIntegerOption("limit");
        }

        if (options.contains(NOTIFY_SERVICE)) {
            notifyServiceOpt = parser.addStringOption('n', "notify");
        }

        if (options.contains(CANCEL_ACTIVITY)) {
            cancelOpt = parser.addBooleanOption('c', "cancel");
        }

        if (options.contains(PAUSE_ACTIVITY)) {
            pauseOpt = parser.addBooleanOption('p', "pause");
        }

        if (options.contains(RESUME_ACTIVITY)) {
            resumeOpt = parser.addBooleanOption('r', "resume");
        }

        if (options.contains(WIPE_ACTIVITY)) {
            wipeOpt = parser.addBooleanOption('w', "wipe");
        }

        if (options.contains(GET_ACTIVITY_STATUS)) {
            getStatusOpt = parser.addBooleanOption('s', "getStatus");
        }

        // if (options.contains(NOTIFY_MESSAGE_TYPE)) {
        // notifyMessageTypeOpt = parser.addStringOption('m', "message");
        // }

        try {
            parser.parse(args);
        } catch (CmdLineParser.OptionException e) {
            System.err.println(e.getMessage());

            printUsage();
        }

        Boolean getHelp = (Boolean) parser.getOptionValue(getHelpOpt, Boolean.FALSE);

        if (getHelp.booleanValue()) {
            printUsage();
        }

        if (proxyOpt != null) {
            proxy = (String) parser.getOptionValue(proxyOpt);
        }

        if (eprOpt != null) {
            epr = (String) parser.getOptionValue(eprOpt);

            if (epr == null) {
                printUsage();
            }
        }

        if (queryResourceInfoOpt != null) {
            isQueryResourceInfo = true;
            setQuery((String) parser.getOptionValue(queryResourceInfoOpt));
        }

        if (statusOpt != null) {
            String statuses = (String) parser.getOptionValue(statusOpt, null);
            if (statuses != null) {
                StringTokenizer st = new StringTokenizer(statuses, ":");

                if (st.countTokens() > 0) {
                    statusList = new ArrayList<String>(st.countTokens());

                    while (st.hasMoreTokens()) {
                        statusList.add(st.nextToken());
                    }
                }
            }
        }

//        if (fromOpt != null) {
//            String fromDateStr = (String) parser.getOptionValue(fromOpt, null);
//            SimpleDateFormat df = new SimpleDateFormat("dd/MM/yyyy-HH:mm:ss");
//
//            if (fromDateStr != null) {
//                try {
//                    fromDate = new GregorianCalendar();
//                    fromDate.setTime(df.parse(fromDateStr));
//                } catch (ParseException e) {
//                    fromDate = null;
//                    System.err.println("Invalid --from date " + fromDateStr + "; ignored");
//                }
//            }
//        }
//
//        if (toOpt != null) {
//            String toDateStr = (String) parser.getOptionValue(toOpt, null);
//            SimpleDateFormat df = new SimpleDateFormat("dd/MM/yyyy-HH:mm:ss");
//
//            if (toDateStr != null) {
//                try {
//                    toDate = new GregorianCalendar();
//                    toDate.setTime(df.parse(toDateStr));
//                } catch (ParseException e) {
//                    toDate = null;
//                    System.err.println("Invalid --end date " + toDateStr + "; ignored");
//                }
//            }
//        }

//        if (limitOpt != null) {
//            limit = ((Integer) parser.getOptionValue(limitOpt, -1)).intValue();
//        }

        if (renewOpt != null) {
            isRenew = ((Boolean) parser.getOptionValue(renewOpt, Boolean.FALSE)).booleanValue();
        }

        if (activityInfoOpt != null) {
            isGetActivityInfo = ((Boolean) parser.getOptionValue(activityInfoOpt, Boolean.FALSE)).booleanValue();
            String attributes = (String) parser.getOptionValue(attributeListOpt);
            
            if (attributes != null) {
                StringTokenizer st = new StringTokenizer(attributes, ":");
                attributeList = new ArrayList<String>(0);
                
                while (st.hasMoreTokens()) {
                    attributeList.add(st.nextToken());
                }
            }
        }

        if (listActivitiesOpt != null) {
            isListActivities = ((Boolean) parser.getOptionValue(listActivitiesOpt, Boolean.FALSE)).booleanValue();
            limit = ((Integer) parser.getOptionValue(limitOpt, -1)).intValue();
            
            String dateStr = (String) parser.getOptionValue(toOpt, null);
            SimpleDateFormat df = new SimpleDateFormat("dd/MM/yyyy-HH:mm:ss");

            if (dateStr != null) {
                try {
                    toDate = new GregorianCalendar();
                    toDate.setTime(df.parse(dateStr));
                } catch (ParseException e) {
                    toDate = null;
                    System.err.println("Invalid --end date " + dateStr + "; ignored");
                }
            }
            
            dateStr = (String) parser.getOptionValue(fromOpt, null);

            if (dateStr != null) {
                try {
                    fromDate = new GregorianCalendar();
                    fromDate.setTime(df.parse(dateStr));
                } catch (ParseException e) {
                    fromDate = null;
                    System.err.println("Invalid --from date " + dateStr + "; ignored");
                }
            }
        }

        if (notifyServiceOpt != null) {
            notifyMessageType = (String) parser.getOptionValue(notifyServiceOpt);
        }

        if (cancelOpt != null) {
            isCancelActivity = ((Boolean) parser.getOptionValue(cancelOpt, Boolean.FALSE)).booleanValue();
        }

        if (pauseOpt != null) {
            isPauseActivity = ((Boolean) parser.getOptionValue(pauseOpt, Boolean.FALSE)).booleanValue();
        }

        if (resumeOpt != null) {
            isResumeActivity = ((Boolean) parser.getOptionValue(resumeOpt, Boolean.FALSE)).booleanValue();
        }

        if (wipeOpt != null) {
            isWipeActivity = ((Boolean) parser.getOptionValue(wipeOpt, Boolean.FALSE)).booleanValue();
        }

        if (getStatusOpt != null) {
            isGetActivityStatus = ((Boolean) parser.getOptionValue(getStatusOpt, Boolean.FALSE)).booleanValue();
        }

        if (getResourceInfoOpt != null) {
            isGetResourceInfo = ((Boolean) parser.getOptionValue(getResourceInfoOpt, Boolean.FALSE)).booleanValue();
        }

        // if (notifyMessageTypeOpt != null) {
        // notifyMessageType = (String)
        // parser.getOptionValue(notifyMessageTypeOpt);
        // }

        if (getVersionOpt != null) {
            isGetVersion = (((Boolean) parser.getOptionValue(getVersionOpt, Boolean.FALSE)).booleanValue());
        }

        if (getInterfaceVersionOpt != null) {
            isGetInterfaceVersion = (((Boolean) parser.getOptionValue(getInterfaceVersionOpt, Boolean.FALSE)).booleanValue());
        }

        if (getTerminationTimeOpt != null) {
            isGetTerminationTime = (((Boolean) parser.getOptionValue(getTerminationTimeOpt, Boolean.FALSE)).booleanValue());
        }

        if (getNewProxyRequestOpt != null) {
            isGetNewProxyRequest = (((Boolean) parser.getOptionValue(getNewProxyRequestOpt, Boolean.FALSE)).booleanValue());
        }

        if (getProxyRequestOpt != null) {
            isGetProxyRequest = (((Boolean) parser.getOptionValue(getProxyRequestOpt, Boolean.FALSE)).booleanValue());
        }

        if (renewProxyRequestOpt != null) {
            isRenewProxyRequest = (((Boolean) parser.getOptionValue(renewProxyRequestOpt, Boolean.FALSE)).booleanValue());
        }

        if (destroyOpt != null) {
            isDestroy = (((Boolean) parser.getOptionValue(destroyOpt, Boolean.FALSE)).booleanValue());
        }

        String[] opt = parser.getRemainingArgs();

        if (opt.length > 0) {
            idList = new ArrayList<String>(opt.length);

            for (int i = 0; i < opt.length; i++) {
                idList.add(opt[i]);
            }
        }
    }

    protected void printUsage() {
        System.err.print("usage: " + getClass().getName() + " -e|--epr <endpoint> [-h|--help]");

        if (options == null) {
            System.err.println();
        } else {
            for (int i = 0; i < options.size(); i++) {
                if (options.get(i).equals(ADL)) {
                    System.err.print(" file.adl");
                } else if (options.get(i).equals(PROXY)) {
                    System.err.print(" [-x|--proxy <filePath>]");
                } else if (options.get(i).equals(STATUS)) {
                    System.err.print(" [-s|--status]");
                } else if (options.get(i).equals(DELEGATION_ID)) {
                    System.err.print(" [-d|--delegId <id>]");
                    // } else if (options.get(i).equals(FROM_DATE)) {
                    // System.err.print(" [-f|--fromDate <'dd/MM/yyyy HH:mm:ss'>]");
                    // } else if (options.get(i).equals(TO_DATE)) {
                    // System.err.print(" [-t|--toDate <'dd/MM/yyyy HH:mm:ss'>]");
                } else if (options.get(i).equals(GET_PROXY_REQUEST)) {
                    System.err.print(" [-g|--getreq id]");
                } else if (options.get(i).equals(GET_NEW_PROXY_REQUEST)) {
                    System.err.print(" [-n|--newreq]");
                } else if (options.get(i).equals(RENEW_PROXY_REQUEST)) {
                    System.err.print(" [-r|--renewreq id]");
                } else if (options.get(i).equals(DESTROY)) {
                    System.err.print(" [-d|--destroy id]");
                } else if (options.get(i).equals(GET_VERSION)) {
                    System.err.print(" [-v|--version]");
                } else if (options.get(i).equals(GET_INTERFACE_VERSION)) {
                    System.err.print(" [-i|--interfaceversion");
                } else if (options.get(i).equals(GET_ACTIVITY_INFO)) {
                    System.err.print(" [-i|--info -a|--attributes attr1:attr2:... <id1 id2 ...>]");
                } else if (options.get(i).equals(GET_RESOURCE_INFO)) {
                    System.err.print(" [-i|--info]");
                } else if (options.get(i).equals(QUERY_RESOURCE_INFO)) {
                    System.err.print(" [-q| --query xpath_query]");
                } else if (options.get(i).equals(NOTIFY_SERVICE)) {
                    System.err.print(" [-n|--notify {dataPushDone | dataPullDone} <id1 id2 ...>]");
                } else if (options.get(i).equals(CANCEL_ACTIVITY)) {
                    System.err.print(" [-c|--cancel <id1 id2 ...>]");
                } else if (options.get(i).equals(PAUSE_ACTIVITY)) {
                    System.err.print(" [-p|--pause <id1 id2 ...>]");
                } else if (options.get(i).equals(RESUME_ACTIVITY)) {
                    System.err.print(" [-r|--resume <id1 id2 ...>]");
                } else if (options.get(i).equals(WIPE_ACTIVITY)) {
                    System.err.print(" [-w|--wipe <id1 id2 ...>]");
                } else if (options.get(i).equals(GET_ACTIVITY_STATUS)) {
                    System.err.print(" [-s|--getStatus <id1 id2 ...>]");
                } else if (options.get(i).equals(LIST_ACTIVITIES)) {
                    System.err.print(" [-l|--listActivities -f|--fromDate dd/MM/yyyy-HH:mm:ss -t|--toDate dd/MM/yyyy-HH:mm:ss --limit x <status1:attr1:attr2:.. status2 ...>]");
                }
            }

            System.err.println();
        }

        System.exit(0);
    }

    public void setCancelActivity(boolean isCancelActivity) {
        this.isCancelActivity = isCancelActivity;
    }

    public void setEpr(String epr) {
        this.epr = epr;
    }

    public void setFromDate(Calendar fromDate) {
        this.fromDate = fromDate;
    }

    public void setIsActivityInfo(boolean isGetActivityInfo) {
        this.isGetActivityInfo = isGetActivityInfo;
    }

    public void setIsGetActivityStatus(boolean isGetActivityStatus) {
        this.isGetActivityStatus = isGetActivityStatus;
    }

    public void setIsGetDelegationInfo(boolean isGetDelegationInfo) {
        this.isGetDelegationInfo = isGetDelegationInfo;
    }

//    public void setIsNotifyService(boolean isNotifyService) {
//        this.isNotifyService = isNotifyService;
//    }

    public void setIsPauseActivity(boolean isPauseActivity) {
        this.isPauseActivity = isPauseActivity;
    }

    public void setIsRenew(boolean isRenew) {
        this.isRenew = isRenew;
    }

    public void setisRResumeActivity(boolean isResumeActivity) {
        this.isResumeActivity = isResumeActivity;
    }

    // public void setNotifyMessageType(String notifyMessageType) {
    // this.notifyMessageType = notifyMessageType;
    // }

    public void setQuery(String query) {
        this.query = query;
    }

    private void setSSLProperties() throws AxisFault {
        Protocol.registerProtocol("https", new Protocol("https", new AXIS2SocketFactory(), 8443));

        Properties sslConfig = new Properties();
        sslConfig.put(ContextWrapper.SSL_PROTOCOL, "SSLv3");
        sslConfig.put(ContextWrapper.CA_FILES, "/etc/grid-security/certificates/*.0");
        sslConfig.put(ContextWrapper.CRL_ENABLED, "true");
        sslConfig.put(ContextWrapper.CRL_FILES, "/etc/grid-security/certificates/*.r0");
        sslConfig.put(ContextWrapper.CRL_UPDATE_INTERVAL, "0s");

        if (proxy != null) {
            sslConfig.put(ContextWrapper.CREDENTIALS_PROXY_FILE, proxy);
        } else {
            String confFileName = System.getProperty("user.home") + "/.glite/dlgor.properties";
            GrDProxyDlgorOptions dlgorOpt;
            try {
                dlgorOpt = new GrDProxyDlgorOptions(confFileName);
            } catch (IOException e) {
                throw new AxisFault(e.getMessage());
            }

            String proxyFilename = dlgorOpt.getDlgorProxyFile();

            if (proxyFilename != null) {
                sslConfig.put(ContextWrapper.CREDENTIALS_PROXY_FILE, proxyFilename);
            } else {
                String certFilename = dlgorOpt.getDlgorCertFile();
                if (certFilename == null || "".equals(certFilename)) {
                    throw new AxisFault("Missing user credentials: issuerCertFile not found in " + confFileName);
                }

                String keyFilename = dlgorOpt.getDlgorKeyFile();
                if (certFilename == null || "".equals(certFilename)) {
                    throw new AxisFault("Missing user credentials: issuerKeyFile not found in " + confFileName);
                }

                String passwd = dlgorOpt.getDlgorPass();
                passwd = passwd == null ? "" : passwd;

                sslConfig.put(ContextWrapper.CREDENTIALS_CERT_FILE, certFilename);
                sslConfig.put(ContextWrapper.CREDENTIALS_KEY_FILE, keyFilename);
                sslConfig.put(ContextWrapper.CREDENTIALS_KEY_PASSWD, passwd);
            }
        }

        AXIS2SocketFactory.setCurrentProperties(sslConfig);
    }

    public void setStatus(List<String> statusList) {
        this.statusList = statusList;
    }

    public void setToDate(Calendar toDate) {
        this.toDate = toDate;
    }

    public void setWipeActivity(boolean isWipeActivity) {
        this.isWipeActivity = isWipeActivity;
    }

    protected String signRequest(String certReq) throws IOException {
        String strX509CertChain = null;
        String proxyFile = proxy;

        if (proxyFile == null) {
            String confFileName = System.getProperty("user.home") + "/.glite/dlgor.properties";
            GrDProxyDlgorOptions dlgorOpt = new GrDProxyDlgorOptions(confFileName);
            proxyFile = dlgorOpt.getDlgorCertFile();
        }

        try {
            GrDProxyGenerator proxyGenerator = new GrDProxyGenerator();

            byte[] x509Cert = proxyGenerator.x509MakeProxyCert(certReq.getBytes(), GrDPX509Util.getFilesBytes(new File(proxyFile)), "null");

            strX509CertChain = new String(x509Cert);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return strX509CertChain;
    }
}
