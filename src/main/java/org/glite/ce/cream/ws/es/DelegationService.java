package org.glite.ce.cream.ws.es;

import java.util.Calendar;

import org.apache.axis2.AxisFault;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.context.ServiceContext;
import org.apache.axis2.description.AxisModule;
import org.apache.axis2.description.AxisOperation;
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
import org.glite.ce.creamapi.cmdmanagement.CommandManagerInterface;
import org.glite.ce.creamapi.delegationmanagement.DelegationCommand;
import org.glite.ce.creamapi.ws.es.delegation.DelegationExceptionException;
import org.glite.ce.creamapi.ws.es.delegation.DelegationServiceSkeletonInterface;
import org.glite.ce.creamapi.ws.es.delegation.GetNewProxyReqResponse;
import org.glite.ce.creamapi.ws.es.delegation.GetTerminationTimeResponse;

public class DelegationService implements DelegationServiceSkeletonInterface, Lifecycle {
    private static final Logger logger = Logger.getLogger(DelegationService.class.getName());
    private static final int INITIALIZATION_TBD = 0;
    private static final int INITIALIZATION_OK = 1;
    private static final int INITIALIZATION_ERROR = 2;
    private static int initialization = INITIALIZATION_TBD;
    private static String serviceVersion = "N/A";
    private static String serviceInterfaceVersion = "N/A";

    public DelegationService() {
    }

    private void checkInitialization() throws DelegationExceptionException {
        if (initialization == INITIALIZATION_ERROR) {
            throw new DelegationExceptionException("DelegationService not available: configuration failed!");
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

    // public GetDelegationInfoResponse getDelegationInfo(GetDelegationInfo
    // delegationInfo) throws AccessControlFault, InternalBaseFault,
    // UnknownDelegationIDFault {
    // checkInitialization();
    //
    // logger.debug("BEGIN getDelegationInfo");
    //
    // if (delegationInfo == null || delegationInfo.getDelegationID() == null) {
    // throw new UnknownDelegationIDFault("delegationId not specified!");
    // }
    //
    // DelegationCommand command = new
    // DelegationCommand(DelegationCommand.GET_DELEGATION);
    // command.addParameter(DelegationCommand.DELEGATION_ID,
    // delegationInfo.getDelegationID());
    // command.addParameter(DelegationCommand.USER_DN_RFC2253,
    // CEUtils.getUserDN_RFC2253());
    // command.addParameter(DelegationCommand.LOCAL_USER,
    // CEUtils.getLocalUser());
    //
    // try {
    // CommandManager.getInstance().execute(command);
    // } catch (CommandException e) {
    // logger.debug("getDelegationInfo error: " + e.getMessage());
    // throw new UnknownDelegationIDFault(e.getMessage());
    // } catch (CommandManagerException e) {
    // logger.debug("getDelegationInfo error: " + e.getMessage());
    // throw new InternalBaseFault(e.getMessage());
    // }
    //
    // Delegation delegation = (Delegation)
    // command.getResult().getParameter(DelegationCommand.DELEGATION);
    //
    // if (delegation == null) {
    // throw new UnknownDelegationIDFault("delegationId " +
    // delegationInfo.getDelegationID() + " not found!");
    // }
    //
    // GetDelegationInfoResponse response = new GetDelegationInfoResponse();
    //
    // GregorianCalendar time = new GregorianCalendar();
    // time.setTime(delegation.getExpirationTime());
    //
    // response.setLifetime(time);
    //
    // int index = -1;
    // String info = delegation.getInfo();
    // logger.info(delegation.getInfo());
    //
    // if (info != null) {
    // index = info.indexOf("holder DN=\"");
    //
    // if (index != -1) {
    // response.setSubject(info.substring(index+11, info.indexOf("\";",
    // index)));
    // }
    //
    // index = info.indexOf("holder AC issuer=\"");
    //
    // if (index != -1) {
    // response.setIssuer(info.substring(index+18, info.indexOf("\";", index)));
    // }
    // }
    //
    // logger.debug("END getDelegationInfo");
    //
    // return response;
    // }
    //
    public void init(ServiceContext serviceContext) throws AxisFault {
        if (initialization == INITIALIZATION_ERROR) {
            throw new AxisFault("DelegationService not available: configuration failed!");
        }

        if (initialization == INITIALIZATION_OK) {
            return;
        }

        logger.info("starting the CREAMDelegationService initialization...");

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
        } else {
            /*
             * TODO remove workaround for destroy operation
             */
            AxisOperation destroyOp = axisService.getOperationByAction("destroyRequest");
            AxisModule authzModule = axisConfiguration.getModule("cream-authorization");
            if (destroyOp != null && authzModule != null) {
                logger.debug("forced registration of destroy");
                axisService.mapActionToOperation("destroy", destroyOp);
                axisService.mapActionToOperation("http://www.gridsite.org/namespaces/delegation-2/Delegation/destroyRequest", destroyOp);
                destroyOp.engageModule(authzModule);
            } else {
                logger.debug("Cannot apply workaround for destroy operation");
            }
        }

        Parameter parameter = axisService.getParameter("delegationVersion");
        if (parameter != null) {
            serviceVersion = (String) parameter.getValue();
        }

        parameter = axisService.getParameter("delegationInterfaceVersion");
        if (parameter != null) {
            serviceInterfaceVersion = (String) parameter.getValue();
        }

        parameter = axisService.getParameter("serviceLogConfigurationFile");
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

    //
    // public InitDelegationResponse initDelegation(InitDelegation delegation)
    // throws AccessControlFault, InternalBaseFault {
    // checkInitialization();
    //
    // logger.debug("BEGIN initDelegation");
    //
    // if (delegation == null) {
    // throw new InternalBaseFault("delegation not specified!");
    // }
    //
    // if (delegation.getCredentialType() == null) {
    // throw new InternalBaseFault("CredentialType not specified!");
    // }
    //
    // if (SAML.equalsIgnoreCase(delegation.getCredentialType()) ||
    // !RFC3820.equalsIgnoreCase(delegation.getCredentialType())) {
    // throw new InternalBaseFault("CredentialType \"" +
    // delegation.getCredentialType() + "\" not supported!");
    // }
    //
    // DelegationCommand command = null;
    //
    // if (delegation.getRenewalID() == null) {
    // command = new
    // DelegationCommand(DelegationCommand.GET_DELEGATION_REQUEST);
    // } else {
    // command = new
    // DelegationCommand(DelegationCommand.RENEW_DELEGATION_REQUEST);
    // command.addParameter(DelegationCommand.DELEGATION_ID,
    // delegation.getRenewalID());
    // }
    //
    // // command.addParameter("LIFETIME",
    // // delegation.getInitDelegationLifetime()); TBD
    // command.addParameter(DelegationCommand.USER_DN_RFC2253,
    // CEUtils.getUserDN_RFC2253());
    // command.addParameter(DelegationCommand.USER_CERTIFICATE,
    // CEUtils.getUserCert());
    // command.addParameter(DelegationCommand.VOMS_ATTRIBUTES,
    // CEUtils.getVOMSAttributes());
    // command.addParameter(DelegationCommand.LOCAL_USER,
    // CEUtils.getLocalUser());
    //
    // try {
    // CommandManager.getInstance().execute(command);
    // } catch (CommandException e) {
    // logger.debug("getDelegationInfo error: " + e.getMessage());
    // throw new InternalBaseFault(e.getMessage());
    // } catch (CommandManagerException e) {
    // logger.debug("getDelegationInfo error: " + e.getMessage());
    // throw new InternalBaseFault(e.getMessage());
    // }
    //
    // InitDelegationResponse response = new InitDelegationResponse();
    // response.setCSR(command.getResult().getParameterAsString("CERTIFICATE_REQUEST"));
    // response.setDelegationID(command.getResult().getParameterAsString(DelegationCommand.DELEGATION_ID));
    //
    // logger.debug("END initDelegation");
    //
    // return response;
    // }
    //
    // public PutDelegationResponse putDelegation(PutDelegation delegation)
    // throws AccessControlFault, InternalBaseFault, UnknownDelegationIDFault {
    // checkInitialization();
    //
    // logger.debug("BEGIN putDelegation");
    //
    // if (delegation == null) {
    // throw new UnknownDelegationIDFault("delegation not specified!");
    // }
    //
    // DelegationCommand command = new
    // DelegationCommand(DelegationCommand.PUT_DELEGATION);
    // command.addParameter(DelegationCommand.DELEGATION_ID,
    // delegation.getDelegationID());
    // command.addParameter(DelegationCommand.DELEGATION,
    // delegation.getCredential());
    // command.addParameter(DelegationCommand.USER_DN_RFC2253,
    // CEUtils.getUserDN_RFC2253());
    // command.addParameter(DelegationCommand.USER_CERTIFICATE,
    // CEUtils.getUserCert());
    // command.addParameter(DelegationCommand.LOCAL_USER,
    // CEUtils.getLocalUser());
    // command.addParameter(DelegationCommand.LOCAL_USER_GROUP,
    // CEUtils.getLocalUserGroup());
    //
    // try {
    // CommandManager.getInstance().execute(command);
    // } catch (CommandException e) {
    // logger.debug("getDelegationInfo error: " + e.getMessage());
    // throw new UnknownDelegationIDFault(e.getMessage());
    // } catch (CommandManagerException e) {
    // logger.debug("getDelegationInfo error: " + e.getMessage());
    // throw new InternalBaseFault(e.getMessage());
    // }
    //
    // logger.info("END putDelegation");
    //
    // PutDelegationResponse response = new PutDelegationResponse();
    // response.setPutDelegationResponse("SUCCESS");
    //
    // return response;
    // }

    public void destroy(String delegId) throws DelegationExceptionException {
        checkInitialization();

        if (delegId == null) {
            throw new DelegationExceptionException("delegationId not specified!");
        }

        logger.info("BEGIN destroy");

        DelegationCommand command = new DelegationCommand(DelegationCommand.DESTROY_DELEGATION);
        command.addParameter(DelegationCommand.DELEGATION_ID, delegId);
        command.addParameter(DelegationCommand.USER_DN_RFC2253, CEUtils.getUserDN_RFC2253());
        command.addParameter(DelegationCommand.LOCAL_USER, CEUtils.getLocalUser());

        try {
            CommandManager.getInstance().execute(command);
        } catch (Throwable t) {
            logger.debug("destroy error: " + t.getMessage());
            throw new DelegationExceptionException(t.getMessage());
        }

        logger.info("END destroy");

    }

    public String getInterfaceVersion() throws DelegationExceptionException {
        checkInitialization();

        return serviceInterfaceVersion;
    }

    public GetNewProxyReqResponse getNewProxyReq() throws DelegationExceptionException {
        checkInitialization();

        logger.info("BEGIN getNewProxyRequest");

        DelegationCommand command = new DelegationCommand(DelegationCommand.GET_NEW_DELEGATION_REQUEST);
        command.addParameter(DelegationCommand.USER_DN_RFC2253, CEUtils.getUserDN_RFC2253());
        command.addParameter(DelegationCommand.USER_CERTIFICATE, CEUtils.getUserCert());
        command.addParameter(DelegationCommand.VOMS_ATTRIBUTES, CEUtils.getVOMSAttributes());
        command.addParameter(DelegationCommand.LOCAL_USER, CEUtils.getLocalUser());

        try {
            CommandManager.getInstance().execute(command);
        } catch (Throwable t) {
            logger.debug("getNewProxyRequest error: " + t.getMessage());
            throw new DelegationExceptionException(t.getMessage());
        }

        // Create and return the proxy request object
        GetNewProxyReqResponse newProxyReq = new GetNewProxyReqResponse();
        newProxyReq.setDelegationID(command.getResult().getParameterAsString(DelegationCommand.DELEGATION_ID));
        newProxyReq.setProxyRequest(command.getResult().getParameterAsString(DelegationCommand.CERTIFICATE_REQUEST));

        logger.info("END getNewProxyRequest");

        return newProxyReq;
    }

    public String getProxyReq(String delegId) throws DelegationExceptionException {
        checkInitialization();

        if (delegId == null) {
            throw new DelegationExceptionException("delegationId not specified!");
        }

        logger.debug("BEGIN getProxyReq");

        DelegationCommand command = new DelegationCommand(DelegationCommand.GET_DELEGATION_REQUEST);
        command.addParameter(DelegationCommand.DELEGATION_ID, delegId);
        command.addParameter(DelegationCommand.USER_DN_RFC2253, CEUtils.getUserDN_RFC2253());
        command.addParameter(DelegationCommand.LOCAL_USER, CEUtils.getLocalUser());
        command.addParameter(DelegationCommand.VOMS_ATTRIBUTES, CEUtils.getVOMSAttributes());
        command.addParameter(DelegationCommand.USER_CERTIFICATE, CEUtils.getUserCert());

        try {
            CommandManager.getInstance().execute(command);
        } catch (Throwable t) {
            logger.debug("getProxyReq error: " + t.getMessage());
            throw new DelegationExceptionException(t.getMessage());
        }

        String certificateRequest = command.getResult().getParameterAsString(DelegationCommand.CERTIFICATE_REQUEST);

        logger.debug("END getProxyReq");

        return certificateRequest;
    }

    public String getServiceMetadata(String req) throws DelegationExceptionException {
        checkInitialization();

        throw new DelegationExceptionException("operation not implemented!");
    }

    public Calendar getTerminationTime(String delegId) throws DelegationExceptionException {
        checkInitialization();

        if (delegId == null) {
            throw new DelegationExceptionException("delegationId not specified!");
        }

        logger.debug("BEGIN getTerminationTime");

        DelegationCommand command = new DelegationCommand(DelegationCommand.GET_TERMINATION_TIME);
        command.addParameter(DelegationCommand.DELEGATION_ID, delegId);
        command.addParameter(DelegationCommand.USER_DN_RFC2253, CEUtils.getUserDN_RFC2253());
        command.addParameter(DelegationCommand.LOCAL_USER, CEUtils.getLocalUser());

        try {
            CommandManager.getInstance().execute(command);
        } catch (Throwable t) {
            logger.debug("getTerminationTime error: " + t.getMessage());
            throw new DelegationExceptionException(t.getMessage());
        }

        Calendar time = (Calendar) command.getResult().getParameter(DelegationCommand.TERMINATION_TIME);

        logger.debug("END getTerminationTime");
        GetTerminationTimeResponse response = new GetTerminationTimeResponse();
        response.setGetTerminationTimeReturn(time);

        return time;
    }

    public String getVersion() throws DelegationExceptionException {
        checkInitialization();

        return serviceVersion;
    }

    public void putProxy(String delegId, String proxy) throws DelegationExceptionException {
        checkInitialization();

        if (delegId == null || proxy == null) {
            throw new DelegationExceptionException("delegationId not specified!");
        }

        logger.info("BEGIN putProxy");

        DelegationCommand command = new DelegationCommand(DelegationCommand.PUT_DELEGATION);
        command.addParameter(DelegationCommand.DELEGATION_ID, delegId);
        command.addParameter(DelegationCommand.DELEGATION, proxy);
        command.addParameter(DelegationCommand.USER_DN_RFC2253, CEUtils.getUserDN_RFC2253());
        command.addParameter(DelegationCommand.USER_CERTIFICATE, CEUtils.getUserCert());
        command.addParameter(DelegationCommand.LOCAL_USER, CEUtils.getLocalUser());
        command.addParameter(DelegationCommand.LOCAL_USER_GROUP, CEUtils.getLocalUserGroup());

        try {
            CommandManager.getInstance().execute(command);
        } catch (Throwable t) {
            logger.debug("putProxy error: " + t.getMessage());
            throw new DelegationExceptionException(t.getMessage());
        }

//        Delegation deleg = (Delegation) command.getResult().getParameter(DelegationCommand.DELEGATION);
//        if (deleg != null) {
//            JobCmd proxyRenewCmd = new JobCmd(JobCommandConstant.PROXY_RENEW);
//            proxyRenewCmd.setAsynchronous(true);
//            proxyRenewCmd.setUserId(CEUtils.getUserId());
//            proxyRenewCmd.setCommandGroupId(CEUtils.getLocalUser());
//            proxyRenewCmd.setDelegationProxyId(req.getDelegationID());
//            proxyRenewCmd.addParameter("DELEGATION_PROXY_INFO", deleg.getInfo());
//
//            try {
//                CommandManager.getInstance().execute(proxyRenewCmd);
//            } catch (Throwable t) {
//                logger.debug("putProxy error: " + t.getMessage());
//                throw new DelegationExceptionException(t.getMessage());
//            }
//        }

        logger.info("END putProxy");
    }

    public String renewProxyReq(String delegId) throws DelegationExceptionException {
        checkInitialization();

        if (delegId == null) {
            throw new DelegationExceptionException("delegationId not specified!");
        }

        logger.debug("BEGIN renewProxyReq");

        DelegationCommand command = new DelegationCommand(DelegationCommand.RENEW_DELEGATION_REQUEST);
        command.addParameter(DelegationCommand.DELEGATION_ID, delegId);
        command.addParameter(DelegationCommand.USER_DN_RFC2253, CEUtils.getUserDN_RFC2253());
        command.addParameter(DelegationCommand.USER_CERTIFICATE, CEUtils.getUserCert());
        command.addParameter(DelegationCommand.VOMS_ATTRIBUTES, CEUtils.getVOMSAttributes());
        command.addParameter(DelegationCommand.LOCAL_USER, CEUtils.getLocalUser());

        try {
            CommandManager.getInstance().execute(command);
        } catch (Throwable t) {
            logger.debug("renewProxyReq error: " + t.getMessage());
            throw new DelegationExceptionException(t.getMessage());
        }

        return command.getResult().getParameterAsString(DelegationCommand.CERTIFICATE_REQUEST);
    }
}
