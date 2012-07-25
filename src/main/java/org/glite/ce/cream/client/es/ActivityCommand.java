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

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.security.InvalidKeyException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.SignatureException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
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
import org.bouncycastle.jce.PKCS10CertificationRequest;
import org.bouncycastle.openssl.PEMReader;
import org.glite.ce.cream.client.CmdLineParser;
import org.glite.ce.creamapi.ws.es.activityinfo.ActivityInfoServiceStub;
import org.glite.ce.creamapi.ws.es.activitymanagement.ActivityManagementServiceStub;
import org.glite.ce.creamapi.ws.es.creation.CreationServiceStub;
import org.glite.ce.creamapi.ws.es.delegation.DelegationServiceStub;

import eu.emi.security.authn.x509.impl.CertificateUtils;
import eu.emi.security.authn.x509.impl.PEMCredential;
import eu.emi.security.authn.x509.proxy.ProxyGenerator;
import eu.emi.security.authn.x509.proxy.ProxyRequestOptions;
import eu.emi.security.canl.axis2.CANLAXIS2SocketFactory;

public abstract class ActivityCommand {
    public static final String ADL = "ADL";
    public static final String EPR = "EPR";
    public static final String DELEGATION_ID = "DELEGATION_ID";
    public static final String FROM_DATE = "FROM_DATE";
    public static final String TO_DATE = "TO_DATE";
    public static final String PROXY = "PROXY";
    public static final String RENEW_DELEGATION = "RENEW_DELEGATION";
    public static final String GET_ACTIVITY_INFO = "GET_ACTIVITY_INFO";
    public static final String GET_DELEGATION_INFO = "GET_DELEGATION_INFO";
    public static final String CANCEL_ACTIVITY = "CANCEL_ACTIVITY";
    public static final String PAUSE_ACTIVITY = "PAUSE_ACTIVITY";
    public static final String RESUME_ACTIVITY = "RESUME_ACTIVITY";
    public static final String GET_ACTIVITY_STATUS = "GET_ACTIVITY_STATUS";
    public static final String STATUS = "STATUS";
    public static final String WIPE_ACTIVITY = "WIPE_ACTIVITY";
    public static final String LIMIT = "LIMIT";
    public static final String LIST_ACTIVITIES = "LIST_ACTIVITIES";
    public static final String NOTIFY_SERVICE = "NOTIFY_SERVICE";
    public static final String NOTIFY_MESSAGE_TYPE = "NOTIFY_MESSAGE_TYPE";
 
    private int limit = 0;
    private String proxy = null;
    private String epr = null;
    private String notifyMessageType = null;
    private Calendar fromDate = null;
    private Calendar toDate = null;
    private boolean isRenew = false;
    private boolean isGetActivityInfo = false;
    private boolean isGetActivityStatus = false;
    private boolean isGetDelegationInfo = false;
    private boolean isListActivities = false;
    private boolean isNotifyService = false;
    private boolean isCancelActivity = false;
    private boolean isPauseActivity = false;
    private boolean isResumeActivity = false;
    private boolean isWipeActivity = false;

    private List<String> idList = null;
    private List<String> activityDescFileList = null;
    private List<String> statusList = null;
    private List<String> options = null;

    public ActivityCommand(String[] args, List<String> options) throws IllegalArgumentException {
        if (args == null) {
            throw new IllegalArgumentException("arguments not specified!");
        }

        if (options == null) {
            throw new IllegalArgumentException("options not specified!");
        }

        this.options = options;

        parseArguments(args, this.options);

        execute();
    }

    protected abstract void execute();

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

        return new ActivityInfoServiceStub(epr + "/ce-cream-es/services/ActivityInfoService");
    }
    
    public ActivityManagementServiceStub getActivityManagementServiceStub() throws AxisFault {
        if (epr == null) {
            throw new AxisFault("epr not specified!");
        }

        if (epr.startsWith("https")) {
            setSSLProperties();
        }

        return new ActivityManagementServiceStub(epr + "/ce-cream-es/services/ActivityManagementService");
    }

    public CreationServiceStub getCreationServiceStub() throws AxisFault {
        if (epr == null) {
            throw new AxisFault("epr not specified!");
        }

        if (epr.startsWith("https")) {
            setSSLProperties();
        }

        return new CreationServiceStub(epr + "/ce-cream-es/services/CreationService");
    }

    public DelegationServiceStub getDelegationServiceStub() throws AxisFault {
        if (epr == null) {
            throw new AxisFault("epr not specified!");
        }

        if (epr.startsWith("https")) {
            setSSLProperties();
        }

        return new DelegationServiceStub(epr + "/ce-cream-es/services/DelegationService");
    }

    public String getEpr() {
        return epr;
    }

    public Calendar getFromDate() {
        return fromDate;
    }

    public List<String> getIdList() {
        if (idList == null) {
            idList = new ArrayList<String>(0);
        }
        return idList;
    }
    
    public String[] getIdArray() {
        String[] array = null;
        if (idList != null) {
            array = new String[idList.size()];
            array = idList.toArray(array);
        }
        return array;
    }
    
    public int getLimit() {
        return limit;
    }

    public String getNotifyMessageType() {
        return notifyMessageType;
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

    public boolean isGetActivityInfo() {
        return isGetActivityInfo;
    }

    public boolean isGetActivityStatus() {
        return isGetActivityStatus;
    }

    public boolean isGetDelegationInfo() {
        return isGetDelegationInfo;
    }

    public boolean isListActivities() {
        return isListActivities;
    }
    
    public boolean isNotifyService() {
        return isNotifyService;
    }

    public boolean isPauseActivity() {
        return isPauseActivity;
    }

    public boolean isRenew() {
        return isRenew;
    }

    public boolean isResumeActivity() {
        return isResumeActivity;
    }

    public boolean isWipeActivity() {
        return isWipeActivity;
    }

    private void parseArguments(String[] args, List<String> options) {
        try {
              parseArguments2(args, options);
            } catch(Throwable t) {
                t.printStackTrace();
            }
    }

    private void parseArguments2(String[] args, List<String> options) {
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
        CmdLineParser.Option delegationInfoOpt = null;
        CmdLineParser.Option cancelOpt = null;
        CmdLineParser.Option pauseOpt = null;
        CmdLineParser.Option resumeOpt = null;
        CmdLineParser.Option wipeOpt = null;
        CmdLineParser.Option getStatusOpt = null;
        CmdLineParser.Option notifyServiceOpt = null;
        CmdLineParser.Option notifyMessageTypeOpt = null;
        CmdLineParser.Option listActivitiesOpt = null;

        if (options.contains(PROXY)) {
            proxyOpt = parser.addStringOption('x', "proxy");
        }

        if (options.contains(EPR)) {
            eprOpt = parser.addStringOption('e', "epr");
        }

        if (options.contains(STATUS)) {
            statusOpt = parser.addStringOption('s', "status");
        }

        if (options.contains(FROM_DATE)) {
            fromOpt = parser.addStringOption('f', "fromDate");
        }

        if (options.contains(TO_DATE)) {
            toOpt = parser.addStringOption('t', "toDate");
        }

        if (options.contains(LIMIT)) {
            limitOpt = parser.addIntegerOption("limit");
        }
        
        if (options.contains(RENEW_DELEGATION)) {
            renewOpt = parser.addBooleanOption('r', "renew");
        }

        if (options.contains(GET_ACTIVITY_INFO)) {
            activityInfoOpt = parser.addBooleanOption('i', "info");
        }

        if (options.contains(GET_DELEGATION_INFO)) {
            delegationInfoOpt = parser.addBooleanOption('i', "info");
        }
        
        if (options.contains(LIST_ACTIVITIES)) {
            listActivitiesOpt = parser.addBooleanOption('l', "listActivities");
        }
        
        if (options.contains(NOTIFY_SERVICE)) {
            notifyServiceOpt = parser.addBooleanOption('n', "notify");
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

        if (options.contains(NOTIFY_MESSAGE_TYPE)) {
            notifyMessageTypeOpt = parser.addStringOption('m', "message");
        }

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

        if (fromOpt != null) {
            String fromDateStr = (String) parser.getOptionValue(fromOpt, null);
            SimpleDateFormat df = new SimpleDateFormat("dd/MM/yyyy-HH:mm:ss");

            if (fromDateStr != null) {
                try {
                    fromDate = new GregorianCalendar();
                    fromDate.setTime(df.parse(fromDateStr));
                } catch (ParseException e) {
                    fromDate = null;
                    System.err.println("Invalid --from date " + fromDateStr + "; ignored");
                }
            }
        }

        if (toOpt != null) {
            String toDateStr = (String) parser.getOptionValue(toOpt, null);
            SimpleDateFormat df = new SimpleDateFormat("dd/MM/yyyy-HH:mm:ss");

            if (toDateStr != null) {
                try {
                    toDate = new GregorianCalendar();
                    toDate.setTime(df.parse(toDateStr));
                } catch (ParseException e) {
                    toDate = null;
                    System.err.println("Invalid --end date " + toDateStr + "; ignored");
                }
            }
        }

        if (limitOpt != null) {
            limit = ((Integer) parser.getOptionValue(limitOpt, -1)).intValue();
        }
        
        if (renewOpt != null) {
            isRenew = ((Boolean) parser.getOptionValue(renewOpt, Boolean.FALSE)).booleanValue();
        }

        if (activityInfoOpt != null) {
            isGetActivityInfo = ((Boolean) parser.getOptionValue(activityInfoOpt, Boolean.FALSE)).booleanValue();
        }
        
        if (delegationInfoOpt != null) {
            isGetDelegationInfo = (((Boolean) parser.getOptionValue(delegationInfoOpt, Boolean.FALSE)).booleanValue());
        }
        
        if (listActivitiesOpt != null) {
            isListActivities = ((Boolean) parser.getOptionValue(listActivitiesOpt, Boolean.FALSE)).booleanValue();
        }
        
        if (notifyServiceOpt != null) {
            isNotifyService = ((Boolean) parser.getOptionValue(notifyServiceOpt, Boolean.FALSE)).booleanValue();
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

        if (notifyMessageTypeOpt != null) {
            notifyMessageType = (String) parser.getOptionValue(notifyMessageTypeOpt);
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
//                } else if (options.get(i).equals(FROM_DATE)) {
//                    System.err.print(" [-f|--fromDate <'dd/MM/yyyy HH:mm:ss'>]");
//                } else if (options.get(i).equals(TO_DATE)) {
//                    System.err.print(" [-t|--toDate <'dd/MM/yyyy HH:mm:ss'>]");
                } else if (options.get(i).equals(RENEW_DELEGATION)) {
                    System.err.print(" [-r|--renew <id1 id2 ...>]");
                } else if (options.get(i).equals(GET_ACTIVITY_INFO)) {
                    System.err.print(" [-i|--info <id1 id2 ...>]");
                }  else if (options.get(i).equals(GET_DELEGATION_INFO)) {
                    System.err.print(" [-i|--info <id1 id2 ...>]");
                } else if (options.get(i).equals(NOTIFY_SERVICE)) {
                    System.err.print(" [-n|--notify -m|--message {dataPushDone | dataPullDone} <id1 id2 ...>]");
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
                    System.err.print(" [-l|--listActivities -f|--fromDate dd/MM/yyyy-HH:mm:ss -t|--toDate dd/MM/yyyy-HH:mm:ss --limit x <status1 status2 ...>]");
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

    public void setIsGetDelegationInfo(boolean isGetDelegationInfo) {
        this.isGetDelegationInfo = isGetDelegationInfo;
    }

    public void setIsActivityInfo(boolean isGetActivityInfo) {
        this.isGetActivityInfo = isGetActivityInfo;
    }

    public void setIsGetActivityStatus(boolean isGetActivityStatus) {
        this.isGetActivityStatus = isGetActivityStatus;
    }

    public void setIsNotifyService(boolean isNotifyService) {
        this.isNotifyService = isNotifyService;
    }

    public void setIsPauseActivity(boolean isPauseActivity) {
        this.isPauseActivity = isPauseActivity;
    }

    public void setIsRenew(boolean isRenew) {
        this.isRenew = isRenew;
    }

    public void setisRResumeActivity(boolean isResumeActivity) {
        this.isResumeActivity = isResumeActivity;
    }

    public void setNotifyMessageType(String notifyMessageType) {
        this.notifyMessageType = notifyMessageType;
    }

    private void setSSLProperties() throws AxisFault {
        Protocol.registerProtocol("https", new Protocol("https", new CANLAXIS2SocketFactory(), 8443));

        Properties sslConfig = new Properties();
        sslConfig.put("truststore", "/etc/grid-security/certificates");
        sslConfig.put("crlcheckingmode", "ifvalid");

        if (proxy != null) {
            sslConfig.put("proxy", proxy);
        } else {
            
            String confFileName = System.getProperty("user.home") + "/.glite/dlgor.properties";
            Properties dlgorOpt = null;
            try {
                dlgorOpt = this.loadProperties(confFileName);
            } catch (IOException e) {
                throw new AxisFault(e.getMessage());
            }

            String proxyFilename = dlgorOpt.getProperty("issuerProxyFile");

            if (proxyFilename != null) {
                sslConfig.put("proxy", proxyFilename);
            } else {
                String certFilename = dlgorOpt.getProperty("issuerCertFile");
                if (certFilename == null || "".equals(certFilename)) {
                    throw new AxisFault("Missing user credentials: issuerCertFile not found in " + confFileName);
                }

                String keyFilename = dlgorOpt.getProperty("issuerKeyFile");
                if (keyFilename == null || "".equals(keyFilename)) {
                    throw new AxisFault("Missing user credentials: issuerKeyFile not found in " + confFileName);
                }

                String passwd = dlgorOpt.getProperty("issuerPass");
                passwd = passwd == null ? "" : passwd;

                sslConfig.put("cert", certFilename);
                sslConfig.put("key", keyFilename);
                sslConfig.put("password", passwd);
            }
        }

        CANLAXIS2SocketFactory.setCurrentProperties(sslConfig);
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

    protected String signRequest(String certReq, String delegationID) 
            throws IOException, KeyStoreException, CertificateException,
            InvalidKeyException, SignatureException, 
            NoSuchAlgorithmException, NoSuchProviderException {
        
        String confFileName = System.getProperty("user.home") + "/.glite/dlgor.properties";
        Properties dlgorOpt = this.loadProperties(confFileName);
        
        X509Certificate[] parentChain = null;
        PrivateKey pKey = null;
        
        String proxyFilename = dlgorOpt.getProperty("issuerProxyFile", "");
        String certFilename = dlgorOpt.getProperty("issuerCertFile", "");
        String keyFilename = dlgorOpt.getProperty("issuerKeyFile", "");
        String passwd = dlgorOpt.getProperty("issuerPass", "");
        
        if (proxyFilename.length() == 0) {
            
            if (certFilename.length() == 0) {
                throw new AxisFault("Missing user credentials: issuerCertFile not found in " + confFileName);
            }
            
            if (keyFilename.length() == 0) {
                throw new AxisFault("Missing user credentials: issuerKeyFile not found in " + confFileName);
            }
            
            char[] tmppwd = null;
            if (passwd.length() != 0) {
                tmppwd = passwd.toCharArray();
            }
            
            FileInputStream inStream = null;
            try {
                inStream = new FileInputStream(keyFilename);
                pKey = CertificateUtils.loadPrivateKey(inStream, CertificateUtils.Encoding.PEM, tmppwd);
            } finally {
                if (inStream != null) {
                    inStream.close();
                }
            }
                        
            inStream = null;
            try {
                inStream = new FileInputStream(certFilename);
                parentChain = CertificateUtils.loadCertificateChain(inStream, CertificateUtils.Encoding.PEM);
            } finally {
                if (inStream != null) {
                    inStream.close();
                }
            }
            
        }else{
            
            FileInputStream inStream = null;
            try {
                
                inStream = new FileInputStream(proxyFilename);
                PEMCredential credentials = new PEMCredential(inStream, null);
                pKey = credentials.getKey();
                parentChain = credentials.getCertificateChain();
                
            } finally {
                if (inStream != null) {
                    inStream.close();
                }
            }
            
        }
            
        
        PEMReader pemReader = new PEMReader(new StringReader(certReq));
        PKCS10CertificationRequest proxytReq = (PKCS10CertificationRequest) pemReader.readObject();
        ProxyRequestOptions csrOpt = new ProxyRequestOptions(parentChain, proxytReq);
        
        X509Certificate[] certChain = ProxyGenerator.generate(csrOpt, pKey);
        
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        for (X509Certificate tmpcert : certChain) {
            CertificateUtils.saveCertificate(outStream, tmpcert, CertificateUtils.Encoding.PEM);
        }
        
        return outStream.toString();

    }
    
    private Properties loadProperties(String filename)  throws  IOException {
        Properties dlgorOpt = new Properties();
        
        FileInputStream inStream = null;
        try {
            inStream = new FileInputStream(filename);
            dlgorOpt.load(inStream);
        } finally {
            if (inStream != null) {
                    inStream.close();
            }
        }
        
        return dlgorOpt;

    }

}
