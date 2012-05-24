package org.glite.ce.cream.ws.es;

import java.util.GregorianCalendar;

import org.apache.axis2.AxisFault;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.context.ServiceContext;
import org.apache.axis2.description.AxisService;
import org.apache.axis2.description.Parameter;
import org.apache.axis2.engine.AxisConfiguration;
import org.apache.axis2.service.Lifecycle;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.glite.ce.commonj.utils.CEUtils;
import org.glite.ce.cream.cmdmanagement.CommandManager;
import org.glite.ce.cream.configuration.ServiceConfig;
import org.glite.ce.creamapi.cmdmanagement.CommandException;
import org.glite.ce.creamapi.cmdmanagement.CommandManagerException;
import org.glite.ce.creamapi.cmdmanagement.CommandManagerInterface;
import org.glite.ce.creamapi.delegationmanagement.Delegation;
import org.glite.ce.creamapi.delegationmanagement.DelegationCommand;
import org.glite.ce.creamapi.ws.es.delegation.AccessControlFault;
import org.glite.ce.creamapi.ws.es.delegation.DelegationServiceSkeletonInterface;
import org.glite.ce.creamapi.ws.es.delegation.InternalBaseFault;
import org.glite.ce.creamapi.ws.es.delegation.UnknownDelegationIDFault;
import org.glite.ce.creamapi.ws.es.delegation.types.GetDelegationInfo;
import org.glite.ce.creamapi.ws.es.delegation.types.GetDelegationInfoResponse;
import org.glite.ce.creamapi.ws.es.delegation.types.InitDelegation;
import org.glite.ce.creamapi.ws.es.delegation.types.InitDelegationResponse;
import org.glite.ce.creamapi.ws.es.delegation.types.PutDelegation;
import org.glite.ce.creamapi.ws.es.delegation.types.PutDelegationResponse;

public class DelegationService implements DelegationServiceSkeletonInterface, Lifecycle {
    private static final Logger logger = Logger.getLogger(DelegationService.class.getName());
    private static final int INITIALIZATION_TBD = 0;
    private static final int INITIALIZATION_OK = 1;
    private static final int INITIALIZATION_ERROR = 2;
    private static int initialization = INITIALIZATION_TBD;
    public static final String SAML = "SAML";
    public static final String RFC3820 = "RFC3820";

    public DelegationService() {
    }

    private void checkInitialization() throws InternalBaseFault {
        if (initialization == INITIALIZATION_ERROR) {
            throw new InternalBaseFault("DelegationService not available: configuration failed!");
        }
    }

    public void destroy(ServiceContext context) {
        logger.info("destroy invoked!");

        try {
            CommandManager.getInstance().terminate();
        } catch (Throwable t) {
            logger.error("cannot get instance of CommandManager: " + t.getMessage());
        }

        logger.info("destroyed!");
    }

    public GetDelegationInfoResponse getDelegationInfo(GetDelegationInfo delegationInfo) throws AccessControlFault, InternalBaseFault, UnknownDelegationIDFault {
        checkInitialization();

        logger.debug("BEGIN getDelegationInfo");

        if (delegationInfo == null || delegationInfo.getDelegationID() == null) {
            throw new UnknownDelegationIDFault("delegationId not specified!");
        }

        DelegationCommand command = new DelegationCommand(DelegationCommand.GET_DELEGATION);
        command.addParameter(DelegationCommand.DELEGATION_ID, delegationInfo.getDelegationID());
        command.addParameter(DelegationCommand.USER_DN, CEUtils.getUserDN_RFC2253());
        command.addParameter(DelegationCommand.LOCAL_USER, CEUtils.getLocalUser());

        try {
            CommandManager.getInstance().execute(command);
        } catch (CommandException e) {
            logger.debug("getDelegationInfo error: " + e.getMessage());
            throw new UnknownDelegationIDFault(e.getMessage());
        } catch (CommandManagerException e) {
            logger.debug("getDelegationInfo error: " + e.getMessage());
            throw new InternalBaseFault(e.getMessage());
        }

        Delegation delegation = (Delegation) command.getResult().getParameter(DelegationCommand.DELEGATION);

        if (delegation == null) {
            throw new UnknownDelegationIDFault("delegationId " + delegationInfo.getDelegationID() + " not found!");
        }

        GetDelegationInfoResponse response = new GetDelegationInfoResponse();
        
        GregorianCalendar time = new GregorianCalendar();
        time.setTime(delegation.getExpirationTime());

        response.setLifetime(time);

        int index = -1;
        String info = delegation.getInfo();
        logger.info(delegation.getInfo());

        if (info != null) {
            index = info.indexOf("holder DN=\"");

            if (index != -1) {
                response.setSubject(info.substring(index+11, info.indexOf("\";", index)));
            }

            index = info.indexOf("holder AC issuer=\"");

            if (index != -1) {
                response.setIssuer(info.substring(index+18, info.indexOf("\";", index)));
            }
        }

        logger.debug("END getDelegationInfo");

        return response;
    }

    public void init(ServiceContext serviceContext) throws AxisFault {
        if (initialization == INITIALIZATION_ERROR) {
            throw new AxisFault("DelegationService not available: configuration failed!");
        }

        if (initialization == INITIALIZATION_OK) {
            return;
        }

        ConfigurationContext configurationContext = serviceContext.getConfigurationContext();

        if (configurationContext == null) {
            initialization = INITIALIZATION_ERROR;
            logger.error("Configuration error: ConfigurationContext not found!");
            return;
        }

        AxisConfiguration axisConfiguration = configurationContext.getAxisConfiguration();
        if (axisConfiguration == null) {
            initialization = INITIALIZATION_ERROR;
            logger.error("Configuration error: AxisConfiguration not found!");
            return;
        }

        AxisService axisService = axisConfiguration.getService("DelegationService");
        if (axisService == null) {
            initialization = INITIALIZATION_ERROR;
            logger.error("Configuration error: AxisService 'DelegationService' not found!");
            return;
        }

        Parameter parameter = axisService.getParameter("serviceLogConfigurationFile");
        if (parameter != null) {
            LogManager.resetConfiguration();
            PropertyConfigurator.configure((String) parameter.getValue());
        }

        ServiceConfig serviceConfig = ServiceConfig.getConfiguration();
        if (serviceConfig == null) {
            initialization = INITIALIZATION_ERROR;
            logger.error("Configuration error: cannot initialize the ServiceConfig");
            return;
        }

        logger.info("starting the DelegationService initialization...");

        CommandManagerInterface commandManager = null;
        try {
            commandManager = CommandManager.getInstance();
        } catch (Throwable t) {
            initialization = INITIALIZATION_ERROR;
            logger.error("Configuration error: cannot get instance of CommandManager: " + t.getMessage());
            return;
        }

        if (!commandManager.checkCommandExecutor("DelegationExecutor", DelegationCommand.DELEGATION_MANAGEMENT)) {
            initialization = INITIALIZATION_ERROR;
            logger.error("Configuration error: DelegationExecutor not loaded!");
            return;
        }

        initialization = INITIALIZATION_OK;
        logger.info("DelegationService initialization done!");
        logger.info("DelegationService started!");
    }

    public InitDelegationResponse initDelegation(InitDelegation delegation) throws AccessControlFault, InternalBaseFault {
        checkInitialization();

        logger.debug("BEGIN initDelegation");

        if (delegation == null) {
            throw new InternalBaseFault("delegation not specified!");
        }

        if (delegation.getCredentialType() == null) {
            throw new InternalBaseFault("CredentialType not specified!");
        }

        if (SAML.equalsIgnoreCase(delegation.getCredentialType()) || !RFC3820.equalsIgnoreCase(delegation.getCredentialType())) {
            throw new InternalBaseFault("CredentialType \"" + delegation.getCredentialType() + "\" not supported!");
        }

        DelegationCommand command = null;

        if (delegation.getRenewalID() == null) {
            command = new DelegationCommand(DelegationCommand.GET_DELEGATION_REQUEST);
        } else {
            command = new DelegationCommand(DelegationCommand.RENEW_DELEGATION_REQUEST);
            command.addParameter(DelegationCommand.DELEGATION_ID, delegation.getRenewalID());
        }

        // command.addParameter("LIFETIME",
        // delegation.getInitDelegationLifetime()); TBD
        command.addParameter(DelegationCommand.USER_DN, CEUtils.getUserDN_RFC2253());
        command.addParameter(DelegationCommand.USER_CERTIFICATE, CEUtils.getUserCert());
        command.addParameter(DelegationCommand.VOMS_ATTRIBUTES, CEUtils.getVOMSAttributes());
        command.addParameter(DelegationCommand.LOCAL_USER, CEUtils.getLocalUser());

        try {
            CommandManager.getInstance().execute(command);
        } catch (CommandException e) {
            logger.debug("getDelegationInfo error: " + e.getMessage());
            throw new InternalBaseFault(e.getMessage());
        } catch (CommandManagerException e) {
            logger.debug("getDelegationInfo error: " + e.getMessage());
            throw new InternalBaseFault(e.getMessage());
        }

        InitDelegationResponse response = new InitDelegationResponse();
        response.setCSR(command.getResult().getParameterAsString("CERTIFICATE_REQUEST"));
        response.setDelegationID(command.getResult().getParameterAsString(DelegationCommand.DELEGATION_ID));

        logger.debug("END initDelegation");

        return response;
    }

    public PutDelegationResponse putDelegation(PutDelegation delegation) throws AccessControlFault, InternalBaseFault, UnknownDelegationIDFault {
        checkInitialization();

        logger.debug("BEGIN putDelegation");

        if (delegation == null) {
            throw new UnknownDelegationIDFault("delegation not specified!");
        }

        DelegationCommand command = new DelegationCommand(DelegationCommand.PUT_DELEGATION);
        command.addParameter(DelegationCommand.DELEGATION_ID, delegation.getDelegationID());
        command.addParameter(DelegationCommand.DELEGATION, delegation.getCredential());
        command.addParameter(DelegationCommand.USER_DN, CEUtils.getUserDN_RFC2253());
        command.addParameter(DelegationCommand.USER_CERTIFICATE, CEUtils.getUserCert());
        command.addParameter(DelegationCommand.LOCAL_USER, CEUtils.getLocalUser());
        command.addParameter(DelegationCommand.LOCAL_USER_GROUP, CEUtils.getLocalUserGroup());

        try {
            CommandManager.getInstance().execute(command);
        } catch (CommandException e) {
            logger.debug("getDelegationInfo error: " + e.getMessage());
            throw new UnknownDelegationIDFault(e.getMessage());
        } catch (CommandManagerException e) {
            logger.debug("getDelegationInfo error: " + e.getMessage());
            throw new InternalBaseFault(e.getMessage());
        }

        logger.info("END putDelegation");

        PutDelegationResponse response = new PutDelegationResponse();
        response.setPutDelegationResponse("SUCCESS");
 
        return response;
    }
}
