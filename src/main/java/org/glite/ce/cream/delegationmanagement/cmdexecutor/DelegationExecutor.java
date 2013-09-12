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

package org.glite.ce.cream.delegationmanagement.cmdexecutor;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.PublicKey;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.text.FieldPosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.TimeZone;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.bouncycastle.jce.PKCS10CertificationRequest;
import org.bouncycastle.openssl.PEMReader;
import org.bouncycastle.openssl.PEMWriter;
import org.bouncycastle.util.encoders.Hex;
import org.glite.ce.commonj.authz.VOMSResultCollector;
import org.glite.ce.commonj.authz.axis2.AuthorizationModule;
import org.glite.ce.commonj.db.DatasourceManager;
import org.glite.ce.commonj.utils.CEUtils;
import org.glite.ce.cream.configuration.ServiceConfig;
import org.glite.ce.cream.cmdmanagement.CommandManager;
import org.glite.ce.cream.delegationmanagement.DelegationManager;
import org.glite.ce.cream.delegationmanagement.DelegationPurger;
import org.glite.ce.creamapi.cmdmanagement.AbstractCommandExecutor;
import org.glite.ce.creamapi.cmdmanagement.Command;
import org.glite.ce.creamapi.cmdmanagement.CommandException;
import org.glite.ce.creamapi.cmdmanagement.CommandExecutorException;
import org.glite.ce.creamapi.delegationmanagement.Delegation;
import org.glite.ce.creamapi.delegationmanagement.DelegationCommand;
import org.glite.ce.creamapi.delegationmanagement.DelegationManagerInterface;
import org.glite.ce.creamapi.delegationmanagement.DelegationRequest;
import org.glite.ce.creamapi.jobmanagement.db.DBInfoManager;
import org.italiangrid.voms.VOMSAttribute;
import org.italiangrid.voms.VOMSValidators;
import org.italiangrid.voms.ac.VOMSACValidator;

import eu.emi.security.authn.x509.ValidationError;
import eu.emi.security.authn.x509.ValidationResult;
import eu.emi.security.authn.x509.impl.CertificateUtils;
import eu.emi.security.authn.x509.proxy.ProxyCSR;
import eu.emi.security.authn.x509.proxy.ProxyCSRGenerator;
import eu.emi.security.authn.x509.proxy.ProxyCertificateOptions;
import eu.emi.security.authn.x509.proxy.ProxyChainInfo;
import eu.emi.security.authn.x509.proxy.ProxyChainType;
import eu.emi.security.authn.x509.proxy.ProxyUtils;

public class DelegationExecutor
    extends AbstractCommandExecutor {
    private static final Logger logger = Logger.getLogger(DelegationExecutor.class.getName());

    private static final Random delegationIdGenerator = new Random();

    private static String delegationSuffix = null;

    private static MessageDigest sDigester = null;

    private boolean initialized = false;

    /** Key size being used. */
    private int keySize = 2048;

    public static final String DELEGATION_PURGE_RATE = "DELEGATION_PURGE_RATE";

    public static final String CREAM_SANDBOX_DIR = "CREAM_SANDBOX_DIR";

    public static final String CREAM_COPY_PROXY_TO_SANDBOX_BIN_PATH = "CREAM_COPY_PROXY_TO_SANDBOX_BIN_PATH";

    public static final String CREAM_PURGE_PROXY_FROM_SANDBOX_BIN_PATH = "CREAM_PURGE_PROXY_FROM_SANDBOX_BIN_PATH";

    public DelegationExecutor() throws CommandExecutorException {
        super("DelegationExecutor", DelegationCommand.DELEGATION_MANAGEMENT);

        final List<String> commands = new ArrayList<String>(9);
        commands.add(DelegationCommand.DESTROY_DELEGATION);
        commands.add(DelegationCommand.GET_DATABASE_VERSION);
        commands.add(DelegationCommand.GET_DELEGATION);
        commands.add(DelegationCommand.GET_NEW_DELEGATION_REQUEST);
        commands.add(DelegationCommand.GET_DELEGATION_REQUEST);
        commands.add(DelegationCommand.GET_SERVICE_MEDATADA);
        commands.add(DelegationCommand.GET_TERMINATION_TIME);
        commands.add(DelegationCommand.PUT_DELEGATION);
        commands.add(DelegationCommand.RENEW_DELEGATION_REQUEST);

        setCommands(commands);

        addParameter(CREAM_SANDBOX_DIR, "/var/cream_sandbox");
        addParameter(CREAM_COPY_PROXY_TO_SANDBOX_BIN_PATH, "/usr/bin/glite-cream-copyProxyToSandboxDir.sh");
        addParameter(CREAM_PURGE_PROXY_FROM_SANDBOX_BIN_PATH, "/usr/bin/glite-ce-cream-purge-proxy");
        addParameter(DELEGATION_PURGE_RATE, "720");

        dataSourceName = DelegationManagerInterface.DELEGATION_DATASOURCE_NAME;
    }

    private String createAndStoreCertificateRequest(X509Certificate parentCert, String delegationId, String dn,
            String localUser, List<VOMSAttribute> vomsAttributes)
        throws CommandException {
        logger.debug("BEGIN createAndStoreCertificateRequest");

        ProxyCertificateOptions prOpts = new ProxyCertificateOptions(new X509Certificate[] { parentCert });
        prOpts.setKeyLength(keySize);

        ProxyCSR pCSRContainer = null;
        String privateKey = null;
        PublicKey publicKey = null;
        String certificateRequest = null;
        try {

            pCSRContainer = ProxyCSRGenerator.generate(prOpts);
            PKCS10CertificationRequest pRequest = pCSRContainer.getCSR();
            publicKey = pRequest.getPublicKey();

            ByteArrayOutputStream outStream = new ByteArrayOutputStream();
            CertificateUtils.savePrivateKey(outStream, pCSRContainer.getPrivateKey(), CertificateUtils.Encoding.PEM,
                    null, null);
            outStream.close();
            privateKey = outStream.toString();

            StringWriter strWriter = new StringWriter();
            PEMWriter pemWriter = new PEMWriter(strWriter);
            pemWriter.writeObject(pRequest);
            pemWriter.close();
            certificateRequest = strWriter.toString();

            logger.debug("Public key is: " + publicKey.toString());
            logger.debug("Private key is: " + privateKey);
            logger.debug("Certificate request is: " + certificateRequest);

        } catch (Exception ex) {
            throw new CommandException("Error while generating the certificate request [delegId=" + delegationId
                    + "; dn=" + dn + "; localUser=" + localUser + "]: " + ex.getMessage());
        }

        String reqId = null;
        try {
            reqId = delegationId + '+' + this.generateSessionID(publicKey);
            logger.debug("DelegationRequestId (delegationId + sessionId): " + reqId);
        } catch (GeneralSecurityException e) {
            throw new CommandException("Error while generating the sessionId [delegId=" + delegationId + "; dn=" + dn
                    + "; localUser=" + localUser + "]: " + e.getMessage());
        }

        ArrayList<String> vomsAttributeList = new ArrayList<String>(0);

        for (VOMSAttribute vomsAttr : vomsAttributes) {
            vomsAttributeList.addAll(vomsAttr.getFQANs());
        }

        DelegationRequest delegationRequest = null;
        try {
            boolean found = true;
            // Search for an existing entry in storage for this delegation ID
            delegationRequest = DelegationManager.getInstance().getDelegationRequest(reqId, dn, localUser);

            if (delegationRequest == null) {
                delegationRequest = new DelegationRequest(reqId);
                found = false;
            }

            delegationRequest.setDN(dn);
            delegationRequest.setVOMSAttributes(vomsAttributeList);
            delegationRequest.setCertificateRequest(certificateRequest);
            delegationRequest.setPublicKey(publicKey.toString());
            delegationRequest.setPrivateKey(privateKey);
            delegationRequest.setLocalUser(localUser);

            if (found) {
                DelegationManager.getInstance().update(delegationRequest);
            } else {
                DelegationManager.getInstance().insert(delegationRequest);
            }
        } catch (Exception e) {
            throw new CommandException("Failure on storage interaction [delegId=" + delegationId + "; dn=" + dn
                    + "; localUser=" + localUser + "]: " + e.getMessage());
        }

        logger.debug("END createAndStoreCertificateRequest");
        return certificateRequest;
    }

    public void destroy() {
        logger.info("destroy invoked!");

        super.destroy();

        try {
            DelegationManager.getInstance().terminate();
        } catch (Throwable t) {
            logger.error("cannot get instance of DelegationManager: " + t.getMessage());
        }

        logger.info("destroyed!");
    }

    /**
     * @see org.glite.security.delegation.service.Delegation#destroy(java.lang.String)
     */
    private void destroyDelegation(Command command)
        throws CommandException {
        logger.debug("BEGIN destroy");

        Delegation delegation = getDelegation(command);

        if (delegation == null) {
            throw new CommandException("delegation [delegId="
                    + getParameterValueAsString(command, DelegationCommand.DELEGATION_ID) + "; dn="
                    + getParameterValueAsString(command, DelegationCommand.USER_DN_RFC2253) + "; localUser="
                    + getParameterValueAsString(command, DelegationCommand.LOCAL_USER) + "] not found!");
        }

        // String delegationId = delegation.getId();
        // String dn = delegation.getDN();
        // String userId = delegation.getUserId();
        String localUser = delegation.getLocalUser();
        // String localUserGroup = delegation.getLocalUserGroup();

        logger.debug("removing the delegation from sandbox " + delegation.toString());

        Process proc = null;
        String[] cmd = new String[] { "sudo", "-S", "-n", "-u", localUser,
                getParameterValueAsString(CREAM_PURGE_PROXY_FROM_SANDBOX_BIN_PATH), delegation.getFullPath() };

        try {
            proc = Runtime.getRuntime().exec(cmd);

            logger.debug("removed the delegation from sandbox " + delegation.toString());
        } catch (Throwable e) {
            if (proc != null) {
                proc.destroy();
            }
        } finally {
            if (proc != null) {
                try {
                    proc.waitFor();
                } catch (InterruptedException ioe) {
                    throw new CommandException(ioe.getMessage());
                }

                StringBuffer errorMessage = null;

                if (proc.exitValue() != 0) {
                    BufferedReader readErr = new BufferedReader(new InputStreamReader(proc.getErrorStream()));

                    errorMessage = new StringBuffer();
                    String inputLine = null;

                    try {
                        while ((inputLine = readErr.readLine()) != null) {
                            errorMessage.append(inputLine);
                        }
                    } catch (IOException ioe) {
                        logger.error(ioe.getMessage());
                    } finally {
                        try {
                            readErr.close();
                        } catch (IOException e) {
                        }
                    }

                    if (errorMessage.length() > 0) {
                        errorMessage.append("\n");
                    }
                }

                try {
                    proc.getInputStream().close();
                } catch (IOException ioe) {
                }

                try {
                    proc.getErrorStream().close();
                } catch (IOException ioe) {
                }

                try {
                    proc.getOutputStream().close();
                } catch (IOException ioe) {
                }

                if (errorMessage != null && errorMessage.length() > 0) {
                    logger.warn("failure on removing the delegation " + delegation.toString() + " from sandbox: "
                            + errorMessage.toString());
                    // throw new CommandException(errorMessage.toString());
                }
            }
        }

        try {
            DelegationManager.getInstance().delete(delegation);
        } catch (Throwable t) {
            throw new CommandException("Failure on deleting the delegation " + delegation.toString() + ": " + t.getMessage());
        }

        try {
            logger.info("cancelling all related active jobs");

            Command jobCancelCmd = new Command("JOB_CANCEL", "JOB_MANAGEMENT");
            jobCancelCmd.setCommandGroupId("COMPOUND");
            jobCancelCmd.setAsynchronous(true);
            jobCancelCmd.setUserId(delegation.getUserId());
            jobCancelCmd.setDescription("job cancelled because the related delegation has expired");
            jobCancelCmd.addParameter("USER_DN", delegation.getDN());
            jobCancelCmd.addParameter("USER_FQAN", delegation.getFQAN());
            jobCancelCmd.addParameter("LOCAL_USER", delegation.getLocalUser());
            jobCancelCmd.addParameter("LOCAL_USER_GROUP", delegation.getLocalUserGroup());
            jobCancelCmd.addParameter("DELEGATION_PROXY_ID", delegation.getId());

            CommandManager.getInstance().execute(jobCancelCmd);

            logger.info("cancelled all related active jobs");
        } catch (Throwable t) {
            logger.error("failure on invoking the jobCancel: " + t.getMessage());
        }

        logger.debug("END destroy");
    }

    public void execute(Command command)
        throws CommandExecutorException, CommandException {
        logger.debug("BEGIN execute");

        if (!initialized) {
            throw new CommandExecutorException(getName() + " not initialized!");
        }

        if (command == null) {
            throw new IllegalArgumentException("command not defined!");
        }

        if (!command.getCategory().equalsIgnoreCase(getCategory())) {
            throw new CommandException("command category mismatch: found \"" + command.getCategory() + "\" required \""
                    + getCategory() + "\"");
        }

        if (command.containsParameterKey(DelegationCommand.USER_DN_RFC2253)) {
            command.addParameter(DelegationCommand.USER_DN_RFC2253,
                    normalize(command.getParameterAsString(DelegationCommand.USER_DN_RFC2253)));
        }

        try {
            if (command.getName().equalsIgnoreCase(DelegationCommand.DESTROY_DELEGATION)) {
                destroyDelegation(command);

            } else if (command.getName().equalsIgnoreCase(DelegationCommand.PUT_DELEGATION)) {
                putDelegation(command);

            } else if (command.getName().equalsIgnoreCase(DelegationCommand.GET_DELEGATION)) {
                Delegation delegation = getDelegation(command, true);
                if (delegation == null) {
                    throw new CommandException("delegation [delegId="
                            + getParameterValueAsString(command, DelegationCommand.DELEGATION_ID) + "; dn="
                            + getParameterValueAsString(command, DelegationCommand.USER_DN_RFC2253) + "; localUser="
                            + getParameterValueAsString(command, DelegationCommand.LOCAL_USER) + "] not found!");
                }
            } else if (command.getName().equalsIgnoreCase(DelegationCommand.GET_DELEGATION_REQUEST)) {
                getDelegationRequest(command);

            } else if (command.getName().equalsIgnoreCase(DelegationCommand.GET_NEW_DELEGATION_REQUEST)) {
                getNewDelegationRequest(command);

            } else if (command.getName().equalsIgnoreCase(DelegationCommand.RENEW_DELEGATION_REQUEST)) {
                renewDelegationRequest(command);

            } else if (command.getName().equalsIgnoreCase(DelegationCommand.GET_TERMINATION_TIME)) {
                getTerminationTime(command);

            } else if (command.getName().equalsIgnoreCase(DelegationCommand.GET_DATABASE_VERSION)) {
                getDatabaseVersion(command);

            } else if (command.getName().equalsIgnoreCase(DelegationCommand.GET_SERVICE_MEDATADA)) {

            } else {
                logger.error("command \"" + command.getName() + "\" not found!");
                throw new CommandExecutorException("command \"" + command.getName() + "\" not found!");
            }
        } catch (CommandException ex) {
            logger.error("Execution of the command \"" + command.getName() + "\" failed: " + ex.getMessage());
            throw ex;
        }

        logger.debug("END execute");
    }

    public void execute(List<Command> commandList)
        throws CommandExecutorException, CommandException {
        if (commandList == null) {
            return;
        }

        for (Command command : commandList) {
            execute(command);
        }
    }

    private String getDatabaseVersion(Command command)
        throws CommandException {
        logger.debug("BEGIN getDatabaseVersion");

        String version = "N/A";

        try {
            version = DBInfoManager.getDBVersion(dataSourceName);
        } catch (Exception e) {
            throw new CommandException("Failure on storage interaction: " + e.getMessage());
        }

        command.getResult().addParameter("DATABASE_VERSION", version);
        logger.debug("END getDatabaseVersion");

        return version;
    }

    private Delegation getDelegation(Command command) throws CommandException {
        return getDelegation(command, false);
    }

    private Delegation getDelegation(Command command, boolean includeCertificate) throws CommandException {
        logger.debug("BEGIN getDelegation");

        String delegationId = getParameterValueAsString(command, DelegationCommand.DELEGATION_ID);
        String userDN = getParameterValueAsString(command, DelegationCommand.USER_DN_RFC2253);
        String localUser = getParameterValueAsString(command, DelegationCommand.LOCAL_USER);

        Delegation delegation = null;

        try {
            // Search for an existing entry in storage for this delegation ID
            delegation = DelegationManager.getInstance().getDelegation(delegationId, userDN, localUser, includeCertificate);
        } catch (Exception e) {
            throw new CommandException("Failure on storage interaction [delegId=" + delegationId + "; dn=" + userDN
                    + "; localUser=" + localUser + "]: " + e.getMessage());
        }

        if (delegation != null) {
            delegation.setFileName(makeDelegationFileName(delegationId));
            delegation.setPath(makeDelegationPath(delegation));
            command.getResult().addParameter(DelegationCommand.DELEGATION, delegation);
        }

        logger.debug("END getDelegation");

        return delegation;
    }

    private void getNewDelegationRequest(Command command)
        throws CommandException {
        logger.debug("BEGIN getNewDelegationRequest");

        String userDN = getParameterValueAsString(command, DelegationCommand.USER_DN_RFC2253);
        String originalString = userDN;
        @SuppressWarnings("unchecked")
        List<VOMSAttribute> vomsAttributes = (List<VOMSAttribute>) getParameterValue(command,
                DelegationCommand.VOMS_ATTRIBUTES);

        // Generate a delegation id from the client DN and VOMS attributes
        for (VOMSAttribute vomsAttr : vomsAttributes) {
            for (String attr : vomsAttr.getFQANs()) {
                originalString += attr;
            }
        }

        byte[] hashDigest = null;

        synchronized (sDigester) {
            hashDigest = sDigester.digest(originalString.getBytes());
        }

        byte[] resultDigest = new byte[20];
        // Returns 'n' most significant bytes of byte array
        for (int i = 0; i < 20; ++i) {
            resultDigest[i] = hashDigest[i];
        }

        GregorianCalendar now = new GregorianCalendar();

        command.addParameter(DelegationCommand.DELEGATION_ID,
                new String(Hex.encode(resultDigest)) + now.getTimeInMillis());

        getDelegationRequest(command);

        logger.debug("END getNewDelegationRequest");
    }

    private Object getParameterValue(Command command, String key)
        throws CommandException {
        if (command == null) {
            throw new CommandException("command not specified!");
        }

        if (key == null) {
            throw new CommandException("paramenter key not specified!");
        }

        Object value = command.getParameter(key);

        // Check for a null Delegation
        if (value == null) {
            throw new CommandException("parameter \"" + key + "\" not specified!");
        }

        return value;
    }

    private String getParameterValueAsString(Command command, String key)
        throws CommandException {
        if (command == null) {
            throw new CommandException("command not specified!");
        }

        if (key == null) {
            throw new CommandException("paramenter key not specified!");
        }

        Object value = command.getParameter(key);

        // Check for a null Delegation
        if (value == null) {
            throw new CommandException("parameter \"" + key + "\" not specified!");
        }

        if (!(value instanceof String)) {
            throw new CommandException("the value of the parameter \"" + key
                    + "\" is not an instance of the String type!");
        }

        return (String) value;
    }

    private String getDelegationRequest(Command command)
        throws CommandException {
        logger.debug("BEGIN getDelegationRequest");

        Delegation delegation = null;
        String delegationId = null;

        if (command.containsParameterKey(DelegationCommand.DELEGATION_ID)) {
            delegationId = command.getParameterAsString(DelegationCommand.DELEGATION_ID);
            delegation = getDelegation(command);

            // Throw error in case there was already a credential with the given
            // id
            if (delegation != null) {
                throw new CommandException("delegation [delegId=" + delegation.getId() + "; dn=" + delegation.getDN()
                        + "; localUser=" + delegation.getLocalUser()
                        + "] already exists! please invoke renewDelegationReq()");
            }
        } else {
            delegationId = "";

            synchronized (delegationIdGenerator) {
                delegationId += delegationIdGenerator.nextDouble();
                delegationId = delegationId.substring(2);
                // delegationId = delegationId.substring(delegationId.length() -
                // 15);
            }
        }

        String userDN = getParameterValueAsString(command, DelegationCommand.USER_DN_RFC2253);
        String localUser = getParameterValueAsString(command, DelegationCommand.LOCAL_USER);
        X509Certificate userCertificate = (X509Certificate) getParameterValue(command,
                DelegationCommand.USER_CERTIFICATE);
        @SuppressWarnings("unchecked")
        List<VOMSAttribute> vomsAttributes = (List<VOMSAttribute>) getParameterValue(command,
                DelegationCommand.VOMS_ATTRIBUTES);
        String certificateRequest = createAndStoreCertificateRequest(userCertificate, delegationId, userDN, localUser,
                vomsAttributes);

        command.getResult().addParameter(DelegationCommand.CERTIFICATE_REQUEST, certificateRequest);
        command.getResult().addParameter(DelegationCommand.DELEGATION_ID, delegationId);

        logger.debug("END getDelegationRequest");

        return certificateRequest;
    }

    private Calendar getTerminationTime(Command command)
        throws CommandException {
        logger.debug("BEGIN getTerminationTime");

        Delegation delegation = getDelegation(command);

        if (delegation == null) {
            throw new CommandException("delegation [delegId="
                    + getParameterValueAsString(command, DelegationCommand.DELEGATION_ID) + "; dn="
                    + getParameterValueAsString(command, DelegationCommand.USER_DN_RFC2253) + "; localUser="
                    + getParameterValueAsString(command, DelegationCommand.LOCAL_USER) + "] not found!");
        }

        // Build a calendar object with the proper time
        Calendar time = Calendar.getInstance();
        time.setTime(delegation.getExpirationTime());

        command.getResult().addParameter(DelegationCommand.TERMINATION_TIME, time);

        logger.debug("END getTerminationTime");

        return time;
    }

    public void initExecutor()
        throws CommandExecutorException {
        logger.debug("BEGIN initExecutor");

        if (!initialized) {
            logger.info("initalizing the " + getName() + " executor...");

            ServiceConfig serviceConfig = ServiceConfig.getConfiguration();
            if (serviceConfig == null) {
                throw new CommandExecutorException("Configuration error: cannot initialize the ServiceConfig");
            }

            HashMap<String, DataSource> dataSources = serviceConfig.getDataSources();
            if (dataSources == null) {
                throw new CommandExecutorException("Datasource is empty!");
            }

            if (dataSources.containsKey(dataSourceName)) {
                if (DatasourceManager.addDataSource(dataSourceName, dataSources.get(dataSourceName))) {
                    logger.info("new dataSource \"" + dataSourceName + "\" added to the DatasourceManager");
                } else {
                    logger.info("the dataSource \"" + dataSourceName + "\" already exist!");
                }
            } else {
                throw new CommandExecutorException("Datasource \"" + dataSourceName + "\" not found!");
            }

            try {
                DelegationManager.getInstance();
            } catch (Throwable t) {
                throw new CommandExecutorException("initialization error: cannot get instance of DelegationManager: "
                        + t.getMessage());
            }

            try {
                sDigester = MessageDigest.getInstance("SHA-1");
            } catch (Throwable t) {
                throw new CommandExecutorException("initialization error: message digester implementation not found: "
                        + t.getMessage());
            }

            try {
                delegationSuffix = DelegationManager.getInstance().getDelegationSuffix();
            } catch (Throwable t) {
                throw new CommandExecutorException("cannot get instance of DelegationManager: " + t.getMessage());
            }

            if (delegationSuffix == null || delegationSuffix.equals("")) {
                throw new CommandExecutorException("delegationSuffix not defined!");
            }

            logger.info("delegationSuffix = " + delegationSuffix);

            if (!containsParameterKey(CREAM_SANDBOX_DIR)) {
                throw new CommandExecutorException("parameter CREAM_SANDBOX_DIR not defined!");
            }

            if (!containsParameterKey(CREAM_COPY_PROXY_TO_SANDBOX_BIN_PATH)) {
                throw new CommandExecutorException("parameter CREAM_COPY_PROXY_TO_SANDBOX_BIN_PATH not defined!");
            }

            if (!containsParameterKey(CREAM_PURGE_PROXY_FROM_SANDBOX_BIN_PATH)) {
                throw new CommandExecutorException("parameter CREAM_PURGE_PROXY_FROM_SANDBOX_BIN_PATH not defined!");
            }

            if (containsParameterKey(DELEGATION_PURGE_RATE)) {
                int purgeRateInMinutes = 720;

                try {
                    purgeRateInMinutes = Integer.parseInt(getParameterValueAsString(DELEGATION_PURGE_RATE));
                    DelegationPurger.getInstance().setRate(purgeRateInMinutes);
                    logger.debug("found new value for DELEGATION_PURGE_RATE: " + purgeRateInMinutes + " min.");
                } catch (Throwable t) {
                    logger.warn("Configuration warning: wrong value for DELEGATION_PURGE_RATE parameter => using default "
                            + purgeRateInMinutes);
                }
            }

            initialized = true;
            logger.info(getName() + " executor initialized!");
        }

        logger.debug("END initExecutor");
    }

    private String makeDelegationFileName(String delegationId)
        throws CommandException {
        if (delegationId == null) {
            throw new CommandException("delegationId not specified!");
        }

        return normalize(delegationId + "_" + delegationSuffix);
    }

    private String makeDelegationPath(Delegation delegation)
        throws CommandException {
        if (delegation == null) {
            throw new CommandException("delegation not specified!");
        }

        String cream_sandbox_dir = getParameterValueAsString(CREAM_SANDBOX_DIR);
        if (cream_sandbox_dir == null) {
            throw new CommandException("parameter CREAM_SANDBOX_DIR not defined!");
        }

        return cream_sandbox_dir + File.separator + delegation.getLocalUserGroup() + File.separator
                + delegation.getUserId() + "_" + delegation.getLocalUser() + File.separator + "proxy" + File.separator;
    }

    private String normalize(String s) {
        if (s != null) {
            return s.replaceAll("\\W", "_");
        }
        return null;
    }

    private void putDelegation(Command command)
        throws CommandException {
        logger.debug("BEGIN putDelegation");

        String delegationId = getParameterValueAsString(command, DelegationCommand.DELEGATION_ID);
        String deleg = getParameterValueAsString(command, DelegationCommand.DELEGATION);
        String userDN = getParameterValueAsString(command, DelegationCommand.USER_DN_RFC2253);
        String localUser = getParameterValueAsString(command, DelegationCommand.LOCAL_USER);
        String localUserGroup = getParameterValueAsString(command, DelegationCommand.LOCAL_USER_GROUP);

        String delegInfoStr = "[delegId=" + delegationId + "; dn=" + userDN + "; localUser=" + localUser + "]";

        X509Certificate[] certChain = null;
        try {

            BufferedInputStream pemStream = new BufferedInputStream(new ByteArrayInputStream(deleg.getBytes()));
            certChain = CertificateUtils.loadCertificateChain(pemStream, CertificateUtils.Encoding.PEM);

            if (certChain.length == 0) {
                throw new IOException("Chain has size 0");
            } else {
                logger.debug("Given proxy certificate loaded successfully.");
            }

        } catch (IOException ex) {
            throw new CommandException("Failed to load certificate chain " + delegInfoStr + ": " + ex.getMessage());
        }

        ValidationResult vRes = AuthorizationModule.validator.validate(certChain);
        if (!vRes.isValid()) {

            StringBuffer buff = new StringBuffer("Proxy certificate is not valid\n");
            for (ValidationError vErr : vRes.getErrors()) {
                buff.append(vErr.getMessage()).append("\n");
            }

            String tmps = buff.toString();
            throw new CommandException("Validation failed " + delegInfoStr + ": " + tmps);

        }

        ProxyChainInfo pChainInfo = null;
        boolean isRFCproxy = false;

        try {

            pChainInfo = new ProxyChainInfo(certChain);

            isRFCproxy = pChainInfo.getProxyType().equals(ProxyChainType.RFC3820);

        } catch (CertificateException ex) {
            throw new CommandException("Proxy parsing error " + delegInfoStr + ": " + ex.getMessage());
        }

        String subjectDN = certChain[0].getIssuerX500Principal().getName();
        String issuerDN = certChain[0].getSubjectX500Principal().getName();

        if (logger.isDebugEnabled()) {
            logger.debug("subject DN: " + subjectDN);
            logger.debug("issuer DN: " + issuerDN);
            logger.debug("chain length is: " + certChain.length);
            logger.debug("last cert is:" + certChain[certChain.length - 1]);

            for (int n = 0; n < certChain.length; n++) {
                logger.debug("cert [" + n + "] is from " + certChain[n].getSubjectX500Principal().getName());
            }
        }

        if (subjectDN == null || issuerDN == null) {
            throw new CommandException("Failed to get DN (subject or issuer) out of delegation " + delegInfoStr
                    + ": it came null");
        }

        String clientDN;
        try {
            X509Certificate eeCert = ProxyUtils.getEndUserCertificate(certChain);
            clientDN = eeCert.getSubjectX500Principal().getName();
        } catch (Exception ex) {
            throw new CommandException("No user certificate found in the delegation chain " + delegInfoStr + ": "
                    + ex.getMessage());
        }

        if (clientDN == null) {
            throw new CommandException("Failed to get client DN " + delegInfoStr + ": it came null");
        }

        String authDN = CEUtils.getUserDN_RFC2253();
        if (!clientDN.equals(authDN)) {
            throw new CommandException("Invalid delegation issuer: '" + clientDN + "' " + authDN);
        }

        String reqId = delegationId;
        try {
            reqId = delegationId + '+' + this.generateSessionID(certChain[0].getPublicKey());
            logger.debug("reqId (delegationId + sessionId): " + reqId);
        } catch (GeneralSecurityException e) {
            throw new CommandException("Failed to generate the session ID " + delegInfoStr + ": " + e.getMessage());
        }

        DelegationRequest delegationRequest = null;
        try {
            // Search for an existing entry in storage for this delegation ID
            delegationRequest = DelegationManager.getInstance().getDelegationRequest(reqId, userDN, localUser);
        } catch (Exception e) {
            throw new CommandException("Failure on storage interaction " + delegInfoStr + ": " + e.getMessage());
        }

        // Check if the delegation request existed
        if (delegationRequest == null) {
            throw new CommandException("Delegation request not found! " + delegInfoStr);
        }
        logger.debug("Got delegation request from cache " + delegInfoStr);

        // the public key of the cached certificate request has to
        // match the public key of the proxy certificate, otherwise
        // this is an answer to a different request
        PEMReader pemReader = new PEMReader(new StringReader(delegationRequest.getCertificateRequest()));
        PKCS10CertificationRequest req;
        try {
            req = (PKCS10CertificationRequest) pemReader.readObject();
        } catch (IOException e1) {
            throw new CommandException("Could not load the original certificate request from cache " + delegInfoStr
                    + ": " + e1.getMessage());
        }

        if (req == null) {
            throw new CommandException("Could not load the original certificate request from cache " + delegInfoStr);
        }

        PublicKey publicKey = null;
        try {
            publicKey = req.getPublicKey();
        } catch (Exception e) {
            throw new CommandException("cannot get the public key " + delegInfoStr + ": " + e.getMessage());
        }

        if (!publicKey.equals(certChain[0].getPublicKey())) {
            logger.error("The delegation and the original request's public key do not match [delegation public key: '"
                    + certChain[0].getPublicKey() + "'; request public key: '" + publicKey + "']");
            throw new CommandException("The delegation and the original request's public key do not match "
                    + delegInfoStr);
        }

        // Add the private key to the proxy certificate chain and check it was
        // ok
        // Don't use the CertUtil routines as single writer is faster.
        String completeProxy = null;
        StringWriter writer = new StringWriter();
        PEMWriter pemWriter = new PEMWriter(writer);

        try {
            pemWriter.writeObject(certChain[0]);
            // make sure the writers are in sync.
            pemWriter.flush();
            // write the private key string.
            writer.write(delegationRequest.getPrivateKey());
            // make sure the writers are still in sync.
            writer.flush();
            // pemWriter.writeObject(PrivateKeyReader.read(new
            // BufferedReader(new
            // StringReader(delegationRequest.getPrivateKey()))));
            // pemWriter.flush();

            // add rest of the certs.
            for (int i = 1; i < certChain.length; i++) {
                pemWriter.writeObject(certChain[i]);
            }

            pemWriter.flush();
            completeProxy = writer.toString();
            pemWriter.close();
        } catch (IOException e) {
            throw new CommandException("Could not properly process given delegation " + delegInfoStr + ": "
                    + e.getMessage());
        }

        if (completeProxy == null) {
            throw new CommandException("Could not properly process given delegation " + delegInfoStr);
        }

        SimpleDateFormat dateFormat = new SimpleDateFormat();
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        FieldPosition fp = new FieldPosition(SimpleDateFormat.YEAR_FIELD);

        StringBuffer buff = new StringBuffer("[ isRFC=\"");
        buff.append(isRFCproxy);
        buff.append("\"; valid from=\"");
        dateFormat.format(certChain[0].getNotBefore(), buff, fp);
        buff.append(" (GMT)\"; valid to=\"");
        dateFormat.format(certChain[0].getNotAfter(), buff, fp);
        buff.append(" (GMT)\"; holder DN=\"").append(clientDN);
        buff.append("\"; holder AC issuer=\"").append(issuerDN);

        HashMap<String, List<String>> proxyVOAttrs = new HashMap<String, List<String>>(0);
        List<String> vomsAttrituteList = new ArrayList<String>(0);

        VOMSResultCollector collector = new VOMSResultCollector();
        VOMSACValidator validator = VOMSValidators.newValidator(AuthorizationModule.vomsStore,
                AuthorizationModule.validator, collector);
        List<VOMSAttribute> vomsList = validator.validate(certChain);

        if (collector.size() > 0) {
            StringBuffer eBuff = new StringBuffer("Cannot validate proxy ");
            eBuff.append(delegationId).append("\n").append(collector.toString());
            throw new CommandException(eBuff.toString());
        }

        for (VOMSAttribute vomsAttr : vomsList) {
            buff.append("\"; VO=\"").append(vomsAttr.getVO());
            buff.append("\"; AC issuer=\"").append(vomsAttr.getIssuer());
            buff.append("\"; VOMS attributes={ ");

            for (String attr : vomsAttr.getFQANs()) {
                buff.append(attr).append(", ");
                vomsAttrituteList.add(attr);
            }

            buff.replace(buff.length() - 2, buff.length() - 1, " }");

            proxyVOAttrs.put(vomsAttr.getVO(), vomsAttr.getFQANs());
        }

        buff.append("]");

        // Save the delegation into the storage (copying the rest from the info taken from the cache)
        Delegation delegation = null;

        try {
            delegation = DelegationManager.getInstance().getDelegation(delegationId, userDN, localUser, false);
        } catch (Exception e) {
            throw new CommandException("Failure on storage interaction " + delegInfoStr + ": " + e.getMessage());
        }

        boolean found = true;

        if (delegation == null) {
            delegation = new Delegation(delegationId);
            found = false;
        }

        if (!proxyVOAttrs.values().isEmpty()) {
            List<String> fqanList = proxyVOAttrs.values().iterator().next();
            if (fqanList != null && fqanList.size() > 0) {
                delegation.setFQAN(fqanList.get(0).toString());
            }
        }

        if (!proxyVOAttrs.keySet().isEmpty()) {
            delegation.setVO(proxyVOAttrs.keySet().iterator().next());
        }

        if (!vomsAttrituteList.isEmpty()) {
            delegation.setVOMSAttributes(vomsAttrituteList);
        }

        delegation.setRFC(isRFCproxy);
        delegation.setDN(userDN);
        delegation.setCertificate(completeProxy);
        delegation.setStartTime(certChain[0].getNotBefore());
        delegation.setExpirationTime(certChain[0].getNotAfter());
        delegation.setLastUpdateTime(Calendar.getInstance().getTime());
        delegation.setLocalUser(localUser);
        delegation.setLocalUserGroup(localUserGroup);
        delegation.setInfo(buff.toString());
        delegation.setFileName(makeDelegationFileName(delegationId));
        delegation.setPath(makeDelegationPath(delegation));

        try {
            storeLimitedDelegationProxy(delegation);

            logger.info("New delegation created " + delegation.toString());
        } catch (CommandException e) {
            logger.error("Cannot store the limited delegation locally " + delegation.toString() + ": " + e.getMessage());
            throw e;
        }

        try {
            if (found) {
                DelegationManager.getInstance().update(delegation);
            } else {
                DelegationManager.getInstance().insert(delegation);
            }
        } catch (Throwable t) {
            logger.error(t.getMessage());
            throw new CommandException("Failure on storage interaction " + delegation.toString() + ": "
                    + t.getMessage());
        }

        logger.debug("Delegation finished successfully.");

        // Remove the credential from storage cache
        try {
            DelegationManager.getInstance().delete(delegationRequest);
        } catch (Exception e) {
            logger.warn("Failed to remove credential from storage " + delegation.toString() + ": " + e.getMessage());
        }

        command.getResult().addParameter(DelegationCommand.DELEGATION, delegation);
        logger.debug("END putDelegation");
    }

    private String renewDelegationRequest(Command command)
        throws CommandException {
        logger.debug("BEGIN renewDelegationRequest");

        Delegation delegation = getDelegation(command);

        if (delegation == null) {
            throw new CommandException("delegation [delegId="
                    + getParameterValueAsString(command, DelegationCommand.DELEGATION_ID) + "; dn="
                    + getParameterValueAsString(command, DelegationCommand.USER_DN_RFC2253) + "; localUser="
                    + getParameterValueAsString(command, DelegationCommand.LOCAL_USER) + "] not found!");
        }

        X509Certificate userCertificate = (X509Certificate) getParameterValue(command,
                DelegationCommand.USER_CERTIFICATE);
        @SuppressWarnings("unchecked")
        List<VOMSAttribute> vomsAttributes = (List<VOMSAttribute>) getParameterValue(command,
                DelegationCommand.VOMS_ATTRIBUTES);

        String certificateRequest = createAndStoreCertificateRequest(userCertificate, delegation.getId(),
                delegation.getDN(), delegation.getLocalUser(), vomsAttributes);

        command.getResult().addParameter(DelegationCommand.CERTIFICATE_REQUEST, certificateRequest);
        command.getResult().addParameter(DelegationCommand.DELEGATION_ID, delegation.getId());

        logger.debug("END renewDelegationRequest");

        return certificateRequest;
    }

    private String storeLimitedDelegationProxy(Delegation delegation)
        throws CommandException {
        logger.debug("BEGIN storeLimitedDelegationProxy " + delegation.toString());

        String[] cmd = new String[] { "sudo", "-S", "-n", "-u", delegation.getLocalUser(),
                getParameterValueAsString(CREAM_COPY_PROXY_TO_SANDBOX_BIN_PATH), delegation.getFileName(),
                delegation.getPath(), delegation.isRFC() ? "1" : "0" };

        Process proc = null;
        BufferedOutputStream os = null;

        try {
            proc = Runtime.getRuntime().exec(cmd);
            os = new BufferedOutputStream(proc.getOutputStream());
            os.write(delegation.getCertificate().getBytes());
            os.flush();
            os.close();
            os = null;
        } catch (IOException e) {
            logger.error("IOException caught: " + e.getMessage());
            if (proc != null) {
                proc.destroy();
            }
            throw new CommandException(e.getMessage());
        } catch (Throwable e) {
            if (proc != null) {
                proc.destroy();
            }
        } finally {
            if (proc != null) {
                try {
                    proc.waitFor();
                } catch (InterruptedException ioe) {
                    throw new CommandException(ioe.getMessage());
                }

                StringBuffer errorMessage = null;

                if (proc.exitValue() != 0) {
                    logger.error("proc.exitValue() != 0");
                    BufferedReader readErr = new BufferedReader(new InputStreamReader(proc.getErrorStream()));

                    errorMessage = new StringBuffer();
                    String inputLine = null;

                    try {
                        while ((inputLine = readErr.readLine()) != null) {
                            errorMessage.append(inputLine);
                        }
                    } catch (IOException ioe) {
                        logger.error(ioe.getMessage());
                    } finally {
                        try {
                            readErr.close();
                        } catch (IOException e) {
                        }
                    }

                    if (errorMessage.length() > 0) {
                        errorMessage.append("\n");
                    }
                }

                try {
                    proc.getInputStream().close();
                } catch (IOException ioe) {
                }

                try {
                    proc.getErrorStream().close();
                } catch (IOException ioe) {
                }

                try {
                    proc.getOutputStream().close();
                } catch (IOException ioe) {
                }

                if (errorMessage != null && errorMessage.length() > 0) {
                    throw new CommandException("storeLimitedDelegationProxy error " + delegation.toString() + ": "
                            + errorMessage);
                }
            }
        }

        logger.debug("END storeLimitedDelegationProxy " + delegation.toString());

        return delegation.getFullPath();
    }

    /*
     * This code has been extracted from
     * org.glite.security.delegation.GrDPX509Util
     */
    private String generateSessionID(PublicKey pk)
        throws java.security.NoSuchAlgorithmException {

        MessageDigest digester = MessageDigest.getInstance("SHA-256");
        byte[] oldDigest = digester.digest(pk.getEncoded());
        byte[] newDigest = new byte[20];
        for (int i = 0; i < 20; ++i) {
            newDigest[i] = oldDigest[i];
        }
        return new String(Hex.encode(newDigest));
    }

}
