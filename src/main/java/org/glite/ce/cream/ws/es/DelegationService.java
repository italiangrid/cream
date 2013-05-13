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
import org.glite.ce.creamapi.delegationmanagement.Delegation;
import org.glite.ce.creamapi.delegationmanagement.DelegationCommand;
import org.glite.ce.creamapi.jobmanagement.cmdexecutor.JobCommandConstant;
import org.glite.ce.creamapi.ws.es.delegation.DelegationException;
import org.glite.ce.creamapi.ws.es.delegation.DelegationExceptionException;
import org.glite.ce.creamapi.ws.es.delegation.DelegationServiceSkeletonInterface;
import org.glite.ce.creamapi.ws.es.delegation.Destroy;
import org.glite.ce.creamapi.ws.es.delegation.GetInterfaceVersion;
import org.glite.ce.creamapi.ws.es.delegation.GetInterfaceVersionResponse;
import org.glite.ce.creamapi.ws.es.delegation.GetNewProxyReq;
import org.glite.ce.creamapi.ws.es.delegation.GetNewProxyReqResponse;
import org.glite.ce.creamapi.ws.es.delegation.GetProxyReq;
import org.glite.ce.creamapi.ws.es.delegation.GetProxyReqResponse;
import org.glite.ce.creamapi.ws.es.delegation.GetServiceMetadata;
import org.glite.ce.creamapi.ws.es.delegation.GetServiceMetadataResponse;
import org.glite.ce.creamapi.ws.es.delegation.GetTerminationTime;
import org.glite.ce.creamapi.ws.es.delegation.GetTerminationTimeResponse;
import org.glite.ce.creamapi.ws.es.delegation.GetVersion;
import org.glite.ce.creamapi.ws.es.delegation.GetVersionResponse;
import org.glite.ce.creamapi.ws.es.delegation.PutProxy;
import org.glite.ce.creamapi.ws.es.delegation.RenewProxyReq;
import org.glite.ce.creamapi.ws.es.delegation.RenewProxyReqResponse;

public class DelegationService implements DelegationServiceSkeletonInterface, Lifecycle {
    private static final Logger logger = Logger.getLogger(DelegationService.class.getName());
    private static final int INITIALIZATION_TBD = 0;
    private static final int INITIALIZATION_OK = 1;
    private static final int INITIALIZATION_ERROR = 2;
    private static int initialization = INITIALIZATION_TBD;
    private static String serviceVersion = "N/A";
    private static String serviceInterfaceVersion = "N/A";
    private static String delegationDatabaseVersion = "N/A";

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

    public void init(ServiceContext serviceContext) throws AxisFault {
        if (initialization == INITIALIZATION_ERROR) {
            throw new AxisFault("DelegationService not available: configuration failed!");
        }

        if (initialization == INITIALIZATION_OK) {
            return;
        }

        logger.info("starting the DelegationService initialization...");

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

        parameter = axisService.getParameter("delegationesdbDatabaseVersion");
        if (parameter != null) {
            delegationDatabaseVersion = (String) parameter.getValue();
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

        // check delegationdb version
        DelegationCommand command = new DelegationCommand(DelegationCommand.GET_DATABASE_VERSION);

        try {
            CommandManager.getInstance().execute(command);
        } catch (Throwable t) {
            logger.debug("cannot retrieve the database version from the database: " + t.getMessage());

            initialization = INITIALIZATION_ERROR;
            return;
        }

        String databaseVersionFromDatabase = command.getResult().getParameterAsString("DATABASE_VERSION");

        if (databaseVersionFromDatabase == null) {
            logger.error("Error: cannot retrieve the database version from the database " + "(Requested version is " + delegationDatabaseVersion +
                    ") because either the database isn't reachable " + "or the database version isn't correct");

            initialization = INITIALIZATION_ERROR;
            return;
        }
  
        if (!databaseVersionFromDatabase.equals(delegationDatabaseVersion)) {
            logger.error("The database version (db version = " + databaseVersionFromDatabase +
                    ") is not compliant with the one requested by cream service (db version = " + delegationDatabaseVersion + ")");

            initialization = INITIALIZATION_ERROR;
            return;
        }

        logger.info("database version is correct");

        initialization = INITIALIZATION_OK;

        logger.info("DelegationService initialization done!");
        logger.info("DelegationService started!");
    }

    private DelegationExceptionException makeDelegationException(String msg) {
        DelegationException fault = new DelegationException();
        fault.setMsg(msg);

        DelegationExceptionException delegationException = new DelegationExceptionException();
        delegationException.setFaultMessage(fault);
        
        return delegationException;
    }
    
    public void destroy(Destroy req) throws DelegationExceptionException {
        checkInitialization();

        if (req == null || req.getDelegationID() == null) {
            throw new DelegationExceptionException("delegationId not specified!");
        }

        logger.info("BEGIN destroy");

        DelegationCommand command = new DelegationCommand(DelegationCommand.DESTROY_DELEGATION);
        command.addParameter(DelegationCommand.DELEGATION_ID, req.getDelegationID());
        command.addParameter(DelegationCommand.USER_DN_RFC2253, CEUtils.getUserDN_RFC2253());
        command.addParameter(DelegationCommand.LOCAL_USER, CEUtils.getLocalUser());

        try {
            CommandManager.getInstance().execute(command);
        } catch (Throwable t) {
            logger.debug("destroy error: " + t.getMessage());
            throw makeDelegationException(t.getMessage());
        }

        logger.info("END destroy");

    }

    public GetInterfaceVersionResponse getInterfaceVersion(GetInterfaceVersion req) throws DelegationExceptionException {
        checkInitialization();

        logger.debug("BEGIN getInterfaceVersion");

        GetInterfaceVersionResponse response = new GetInterfaceVersionResponse();
        response.setGetInterfaceVersionReturn(serviceInterfaceVersion);

        logger.debug("END getInterfaceVersion");
        return response;
    }

    public GetNewProxyReqResponse getNewProxyReq(GetNewProxyReq req) throws DelegationExceptionException {
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
            throw makeDelegationException(t.getMessage());
        }

        // Create and return the proxy request object
        GetNewProxyReqResponse newProxyReq = new GetNewProxyReqResponse();
        newProxyReq.setDelegationID(command.getResult().getParameterAsString(DelegationCommand.DELEGATION_ID));
        newProxyReq.setProxyRequest(command.getResult().getParameterAsString(DelegationCommand.CERTIFICATE_REQUEST));

        logger.info("END getNewProxyRequest");

        return newProxyReq;
    }

    public GetProxyReqResponse getProxyReq(GetProxyReq req) throws DelegationExceptionException {
        checkInitialization();

        if (req == null || req.getDelegationID() == null) {
            throw new DelegationExceptionException("delegationId not specified!");
        }

        logger.debug("BEGIN getProxyReq");

        DelegationCommand command = new DelegationCommand(DelegationCommand.GET_DELEGATION_REQUEST);
        command.addParameter(DelegationCommand.DELEGATION_ID, req.getDelegationID());
        command.addParameter(DelegationCommand.USER_DN_RFC2253, CEUtils.getUserDN_RFC2253());
        command.addParameter(DelegationCommand.LOCAL_USER, CEUtils.getLocalUser());
        command.addParameter(DelegationCommand.VOMS_ATTRIBUTES, CEUtils.getVOMSAttributes());
        command.addParameter(DelegationCommand.USER_CERTIFICATE, CEUtils.getUserCert());

        try {
            CommandManager.getInstance().execute(command);
        } catch (Throwable t) {
            logger.debug("getProxyReq error: " + t.getMessage());
            throw makeDelegationException(t.getMessage());
        }

        String certificateRequest = command.getResult().getParameterAsString(DelegationCommand.CERTIFICATE_REQUEST);

        logger.debug("END getProxyReq");

        GetProxyReqResponse response = new GetProxyReqResponse();
        response.setGetProxyReqReturn(certificateRequest);

        return response;
    }

    public GetServiceMetadataResponse getServiceMetadata(GetServiceMetadata req) throws DelegationExceptionException {
        checkInitialization();

        throw makeDelegationException("operation not implemented!");
    }

    public GetTerminationTimeResponse getTerminationTime(GetTerminationTime req) throws DelegationExceptionException {
        checkInitialization();

        if (req == null || req.getDelegationID() == null) {
            throw new DelegationExceptionException("delegationId not specified!");
        }

        logger.debug("BEGIN getTerminationTime");

        DelegationCommand command = new DelegationCommand(DelegationCommand.GET_TERMINATION_TIME);
        command.addParameter(DelegationCommand.DELEGATION_ID, req.getDelegationID());
        command.addParameter(DelegationCommand.USER_DN_RFC2253, CEUtils.getUserDN_RFC2253());
        command.addParameter(DelegationCommand.LOCAL_USER, CEUtils.getLocalUser());

        try {
            CommandManager.getInstance().execute(command);
        } catch (Throwable t) {
            logger.debug("getTerminationTime error: " + t.getMessage());
            throw makeDelegationException(t.getMessage());
        }

        Calendar time = (Calendar) command.getResult().getParameter(DelegationCommand.TERMINATION_TIME);

        logger.debug("END getTerminationTime");
        GetTerminationTimeResponse response = new GetTerminationTimeResponse();
        response.setGetTerminationTimeReturn(time);

        return response;
    }

    public GetVersionResponse getVersion(GetVersion req) throws DelegationExceptionException {
        checkInitialization();

        logger.debug("BEGIN getVersion");
        GetVersionResponse response = new GetVersionResponse();
        response.setGetVersionReturn(serviceVersion);

        logger.debug("END getVersion");
        return response;
    }

    public void putProxy(PutProxy req) throws DelegationExceptionException {
        checkInitialization();

        if (req == null || req.getDelegationID() == null) {
            throw makeDelegationException("delegationId not specified!");
        }

        logger.info("BEGIN putProxy");

        DelegationCommand command = new DelegationCommand(DelegationCommand.PUT_DELEGATION);
        command.addParameter(DelegationCommand.DELEGATION_ID, req.getDelegationID());
        command.addParameter(DelegationCommand.DELEGATION, req.getProxy());
        command.addParameter(DelegationCommand.USER_DN_RFC2253, CEUtils.getUserDN_RFC2253());
        command.addParameter(DelegationCommand.USER_CERTIFICATE, CEUtils.getUserCert());
        command.addParameter(DelegationCommand.LOCAL_USER, CEUtils.getLocalUser());
        command.addParameter(DelegationCommand.LOCAL_USER_GROUP, CEUtils.getLocalUserGroup());

        try {
            CommandManager.getInstance().execute(command);
        } catch (Throwable t) {
            logger.debug("putProxy error: " + t.getMessage());
            throw makeDelegationException(t.getMessage());
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

    public RenewProxyReqResponse renewProxyReq(RenewProxyReq req) throws DelegationExceptionException {
        checkInitialization();

        if (req == null || req.getDelegationID() == null) {
            throw new DelegationExceptionException("delegationId not specified!");
        }

        logger.debug("BEGIN renewProxyReq");

        DelegationCommand command = new DelegationCommand(DelegationCommand.RENEW_DELEGATION_REQUEST);
        command.addParameter(DelegationCommand.DELEGATION_ID, req.getDelegationID());
        command.addParameter(DelegationCommand.USER_DN_RFC2253, CEUtils.getUserDN_RFC2253());
        command.addParameter(DelegationCommand.USER_CERTIFICATE, CEUtils.getUserCert());
        command.addParameter(DelegationCommand.VOMS_ATTRIBUTES, CEUtils.getVOMSAttributes());
        command.addParameter(DelegationCommand.LOCAL_USER, CEUtils.getLocalUser());

        try {
            CommandManager.getInstance().execute(command);
        } catch (Throwable t) {
            logger.debug("renewProxyReq error: " + t.getMessage());
            throw makeDelegationException(t.getMessage());
        }

        String request = command.getResult().getParameterAsString(DelegationCommand.CERTIFICATE_REQUEST);

        logger.debug("END renewProxyReq");
        RenewProxyReqResponse response = new RenewProxyReqResponse();
        response.setRenewProxyReqReturn(request);

        return response;
    }
}
