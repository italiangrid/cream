package org.glite.ce.cream.ws.delegation;

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
import org.apache.log4j.Logger;
import org.glite.ce.commonj.utils.CEUtils;
import org.glite.ce.cream.cmdmanagement.CommandManager;
import org.glite.ce.cream.configuration.ServiceConfig;
import org.glite.ce.cream.jobmanagement.command.JobCmd;
import org.glite.ce.creamapi.delegationmanagement.Delegation;
import org.glite.ce.creamapi.delegationmanagement.DelegationCommand;
import org.glite.ce.creamapi.jobmanagement.cmdexecutor.JobCommandConstant;
import org.glite.ce.security.delegation.DelegationException_Fault;
import org.glite.ce.security.delegation.DelegationServiceSkeletonInterface;
import org.glite.ce.security.delegation.NewProxyReq;

public class CREAMDelegationService implements DelegationServiceSkeletonInterface, Lifecycle {
    private static final Logger logger = Logger.getLogger(CREAMDelegationService.class.getName());
    private static final int INITIALIZATION_TBD = 0;
    private static final int INITIALIZATION_OK = 1;
    private static final int INITIALIZATION_ERROR = 2;
    private static int initialization = INITIALIZATION_TBD;
    private static String serviceVersion = "N/A";
    private static String serviceInterfaceVersion = "N/A";

    /**
     * Initializes the class implementing the delegation logic.
     */
    public CREAMDelegationService() throws InstantiationException {
    }

    private void checkInitialization() throws DelegationException_Fault {
        if (initialization == INITIALIZATION_ERROR) {
            throw new DelegationException_Fault("CREAMDelegationService not available: configuration failed!");
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

    /**
     * @see org.glite.security.delegation.service.Delegation#destroy(java.lang.String)
     */
    public void destroy(String delegationId) throws DelegationException_Fault {
        checkInitialization();

        logger.info("BEGIN destroy");

        DelegationCommand command = new DelegationCommand(DelegationCommand.DESTROY_DELEGATION);
        command.addParameter(DelegationCommand.DELEGATION_ID, delegationId);
        command.addParameter(DelegationCommand.USER_DN, CEUtils.getUserDN_RFC2253());
        command.addParameter(DelegationCommand.LOCAL_USER, CEUtils.getLocalUser());

        try {
            CommandManager.getInstance().execute(command);
        } catch (Throwable t) {
            logger.debug("destroy error: " + t.getMessage());
            throw new DelegationException_Fault(t.getMessage());
        }

        logger.info("END destroy");
    }

    public String getInterfaceVersion() throws DelegationException_Fault {
        checkInitialization();

        try {
            return serviceInterfaceVersion;
        } catch (RuntimeException rt) {
            logger.error("Caught RuntimeException: ", rt);
            throw new DelegationException_Fault("Internal server error: " + rt.getMessage());
        }
    }

    /**
     * @see org.glite.security.delegation.service.Delegation#getNewProxyReq()
     */
    public NewProxyReq getNewProxyReq() throws DelegationException_Fault {
        checkInitialization();

        logger.info("BEGIN getNewProxyReq");

        DelegationCommand command = new DelegationCommand(DelegationCommand.GET_NEW_DELEGATION_REQUEST);
        command.addParameter(DelegationCommand.USER_DN, CEUtils.getUserDN_RFC2253());
        command.addParameter(DelegationCommand.USER_CERTIFICATE, CEUtils.getUserCert());
        command.addParameter(DelegationCommand.VOMS_ATTRIBUTES, CEUtils.getVOMSAttributes());
        command.addParameter(DelegationCommand.LOCAL_USER, CEUtils.getLocalUser());

        try {
            CommandManager.getInstance().execute(command);
        } catch (Throwable t) {
            logger.debug("getNewProxyReq error: " + t.getMessage());
            throw new DelegationException_Fault(t.getMessage());
        }

        // Create and return the proxy request object
        NewProxyReq newProxyReq = new NewProxyReq();
        newProxyReq.setDelegationID(command.getResult().getParameterAsString(DelegationCommand.DELEGATION_ID));
        newProxyReq.setProxyRequest(command.getResult().getParameterAsString(DelegationCommand.CERTIFICATE_REQUEST));

        logger.info("END getNewProxyReq");

        return newProxyReq;
    }

    public String getProxyReq(String delegationId) throws DelegationException_Fault {
        checkInitialization();

        logger.debug("BEGIN getProxyReq");

        DelegationCommand command = new DelegationCommand(DelegationCommand.GET_DELEGATION_REQUEST);
        command.addParameter(DelegationCommand.DELEGATION_ID, delegationId);
        command.addParameter(DelegationCommand.USER_DN, CEUtils.getUserDN_RFC2253());
        command.addParameter(DelegationCommand.LOCAL_USER, CEUtils.getLocalUser());
        command.addParameter(DelegationCommand.VOMS_ATTRIBUTES, CEUtils.getVOMSAttributes());
        command.addParameter(DelegationCommand.USER_CERTIFICATE, CEUtils.getUserCert());

        try {
            CommandManager.getInstance().execute(command);
        } catch (Throwable t) {
            logger.debug("getProxyReq error: " + t.getMessage());
            throw new DelegationException_Fault(t.getMessage());
        }

        String certificateRequest = command.getResult().getParameterAsString(DelegationCommand.CERTIFICATE_REQUEST);

        logger.debug("END getProxyReq");

        return certificateRequest;
    }

    public String getServiceMetadata(String key) throws DelegationException_Fault {
        checkInitialization();

        try {
            return key;
        } catch (RuntimeException rt) {
            logger.debug("getServiceMetadata error: " + rt.getMessage());
            throw new DelegationException_Fault("Internal server error: " + rt.getMessage());
        }
    }

    /**
     * @see org.glite.security.delegation.service.Delegation#getTerminationTime(java.lang.String)
     */
    public Calendar getTerminationTime(String delegationId) throws DelegationException_Fault {
        checkInitialization();

        logger.debug("BEGIN getTerminationTime");

        DelegationCommand command = new DelegationCommand(DelegationCommand.GET_TERMINATION_TIME);
        command.addParameter(DelegationCommand.DELEGATION_ID, delegationId);
        command.addParameter(DelegationCommand.USER_DN, CEUtils.getUserDN_RFC2253());
        command.addParameter(DelegationCommand.LOCAL_USER, CEUtils.getLocalUser());

        try {
            CommandManager.getInstance().execute(command);
        } catch (Throwable t) {
            logger.debug("getTerminationTime error: " + t.getMessage());
            throw new DelegationException_Fault(t.getMessage());
        }

        Calendar time = (Calendar) command.getResult().getParameter(DelegationCommand.TERMINATION_TIME);

        logger.debug("END getTerminationTime");

        return time;
    }

    public String getVersion() throws DelegationException_Fault {
        checkInitialization();

        try {
            return serviceVersion;
        } catch (RuntimeException rt) {
            logger.debug("getVersion error: " + rt.getMessage());
            throw new DelegationException_Fault("Internal server error: " + rt.getMessage());
        }
    }

    public void init(ServiceContext serviceContext) throws AxisFault {
        if (initialization == INITIALIZATION_ERROR) {
            throw new AxisFault("CREAMDelegationService not available: configuration failed!");
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

        AxisService axisService = axisConfiguration.getService("gridsite-delegation");
        if (axisService == null) {
            initialization = INITIALIZATION_ERROR;
            logger.error("Configuration error: AxisService 'gridsite-delegation' not found!");
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

        ServiceConfig serviceConfig = ServiceConfig.getConfiguration();
        if (serviceConfig == null) {
            initialization = INITIALIZATION_ERROR;
            logger.error("Configuration error: cannot initialize the ServiceConfig");
            return;
        }

        boolean executorExists = false;

        try {
            CommandManager commandManager = CommandManager.getInstance();
            executorExists = commandManager.checkCommandExecutor("DelegationExecutor", DelegationCommand.DELEGATION_MANAGEMENT);
        } catch (Throwable t) {
            initialization = INITIALIZATION_ERROR;
            logger.error("Configuration error: cannot get instance of CommandManager: " + t.getMessage());
            return;
        }

        if (!executorExists) {
            initialization = INITIALIZATION_ERROR;
            logger.error("Configuration error: DelegationExecutor not loaded!");
            return;
        }

        initialization = INITIALIZATION_OK;
        logger.info("CREAMDelegationService initialization done!");
        logger.info("CREAMDelegationService started!");
    }

    public void putProxy(String delegationId, String delegation) throws DelegationException_Fault {
        checkInitialization();

        logger.info("BEGIN putProxy");

        DelegationCommand command = new DelegationCommand(DelegationCommand.PUT_DELEGATION);
        command.addParameter(DelegationCommand.DELEGATION_ID, delegationId);
        command.addParameter(DelegationCommand.DELEGATION, delegation);
        command.addParameter(DelegationCommand.USER_DN, CEUtils.getUserDN_RFC2253());
        command.addParameter(DelegationCommand.USER_CERTIFICATE, CEUtils.getUserCert());
        command.addParameter(DelegationCommand.LOCAL_USER, CEUtils.getLocalUser());
        command.addParameter(DelegationCommand.LOCAL_USER_GROUP, CEUtils.getLocalUserGroup());

        try {
            CommandManager.getInstance().execute(command);
        } catch (Throwable t) {
            logger.debug("putProxy error: " + t.getMessage());
            throw new DelegationException_Fault(t.getMessage());
        }

        Delegation deleg = (Delegation)command.getResult().getParameter(DelegationCommand.DELEGATION);
        if (deleg != null) {
            JobCmd proxyRenewCmd = new JobCmd(JobCommandConstant.PROXY_RENEW);
            proxyRenewCmd.setAsynchronous(true);
            proxyRenewCmd.setUserId(CEUtils.getUserId());
            proxyRenewCmd.setCommandGroupId(CEUtils.getLocalUser());
            proxyRenewCmd.setDelegationProxyId(delegationId);
            proxyRenewCmd.addParameter("DELEGATION_PROXY_INFO", deleg.getInfo());

            try {
                CommandManager.getInstance().execute(proxyRenewCmd);
            } catch (Throwable t) {
                logger.debug("putProxy error: " + t.getMessage());
                throw new DelegationException_Fault(t.getMessage());
            }
        }

        logger.info("END putProxy");
    }

    public String renewProxyReq(String delegationId) throws DelegationException_Fault {
        checkInitialization();

        logger.debug("BEGIN renewProxyReq");

        DelegationCommand command = new DelegationCommand(DelegationCommand.RENEW_DELEGATION_REQUEST);
        command.addParameter(DelegationCommand.DELEGATION_ID, delegationId);
        command.addParameter(DelegationCommand.USER_DN, CEUtils.getUserDN_RFC2253());
        command.addParameter(DelegationCommand.USER_CERTIFICATE, CEUtils.getUserCert());
        command.addParameter(DelegationCommand.VOMS_ATTRIBUTES, CEUtils.getVOMSAttributes());
        command.addParameter(DelegationCommand.LOCAL_USER, CEUtils.getLocalUser());

        try {
            CommandManager.getInstance().execute(command);
        } catch (Throwable t) {
            logger.debug("renewProxyReq error: " + t.getMessage());
            throw new DelegationException_Fault(t.getMessage());
        }

        String request = command.getResult().getParameterAsString(DelegationCommand.CERTIFICATE_REQUEST);

        logger.debug("END renewProxyReq");

        return request;
    }
}
