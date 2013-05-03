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

package org.glite.ce.cream.ws.es;

/**
 *  CreateActivitySkeleton java skeleton for the axisService
 */
import java.net.InetAddress;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;

import org.apache.axiom.om.OMAttribute;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMNamespace;
import org.apache.axiom.om.impl.OMNamespaceImpl;
import org.apache.axiom.om.impl.llom.OMAttributeImpl;
import org.apache.axis2.AxisFault;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.context.ServiceContext;
import org.apache.axis2.databinding.types.URI;
import org.apache.axis2.databinding.types.URI.MalformedURIException;
import org.apache.axis2.description.AxisService;
import org.apache.axis2.description.Parameter;
import org.apache.axis2.engine.AxisConfiguration;
import org.apache.axis2.service.Lifecycle;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.glite.ce.cream.activitymanagement.ActivityCmd;
import org.glite.ce.cream.activitymanagement.ActivityCmd.ActivityCommandField;
import org.glite.ce.cream.activitymanagement.ActivityCmd.ActivityCommandName;
import org.glite.ce.cream.cmdmanagement.CommandManager;
import org.glite.ce.cream.configuration.ServiceConfig;
import org.glite.ce.creamapi.cmdmanagement.CommandManagerInterface;
import org.glite.ce.creamapi.ws.es.glue.Capability_t;
import org.glite.ce.creamapi.ws.es.glue.ComputingService_t;
import org.glite.ce.creamapi.ws.es.glue.EndpointHealthState_t;
import org.glite.ce.creamapi.ws.es.glue.EndpointTechnology_t;
import org.glite.ce.creamapi.ws.es.glue.Endpoint_t;
import org.glite.ce.creamapi.ws.es.glue.InterfaceName_t;
import org.glite.ce.creamapi.ws.es.glue.QualityLevel_t;
import org.glite.ce.creamapi.ws.es.glue.ServiceType_t;
import org.glite.ce.creamapi.ws.es.glue.Service_t;
import org.glite.ce.creamapi.ws.es.glue.ServingState_t;
import org.glite.ce.creamapi.ws.es.resourceinfo.AccessControlFault;
import org.glite.ce.creamapi.ws.es.resourceinfo.InternalBaseFault;
import org.glite.ce.creamapi.ws.es.resourceinfo.InternalResourceInfoFault;
import org.glite.ce.creamapi.ws.es.resourceinfo.NotSupportedQueryDialectFault;
import org.glite.ce.creamapi.ws.es.resourceinfo.NotValidQueryStatementFault;
import org.glite.ce.creamapi.ws.es.resourceinfo.ResourceInfoNotFoundFault;
import org.glite.ce.creamapi.ws.es.resourceinfo.ResourceInfoServiceSkeletonInterface;
import org.glite.ce.creamapi.ws.es.resourceinfo.types.DefaultQueryDialectsEnumType;
import org.glite.ce.creamapi.ws.es.resourceinfo.types.GetResourceInfo;
import org.glite.ce.creamapi.ws.es.resourceinfo.types.GetResourceInfoResponse;
import org.glite.ce.creamapi.ws.es.resourceinfo.types.QueryResourceInfo;
import org.glite.ce.creamapi.ws.es.resourceinfo.types.QueryResourceInfoItem_type0;
import org.glite.ce.creamapi.ws.es.resourceinfo.types.QueryResourceInfoResponse;
import org.glite.ce.creamapi.ws.es.resourceinfo.types.Services_type0;

public class ResourceInfoService implements ResourceInfoServiceSkeletonInterface, Lifecycle {
    private static final Logger logger = Logger.getLogger(ResourceInfoService.class.getName());
    private static final int INITIALIZATION_TBD = 0;
    private static final int INITIALIZATION_OK = 1;
    private static final int INITIALIZATION_ERROR = 2;
    private static int initialization = INITIALIZATION_TBD;

    private void checkInitialization() throws InternalBaseFault {
        if (initialization == INITIALIZATION_ERROR) {
            throw new InternalBaseFault("ResourceInfoService not available: configuration failed!");
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
            throw new AxisFault("CreationService not available: configuration failed!");
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

        AxisService axisService = axisConfiguration.getService("ResourceInfoService");
        if (axisService == null) {
            initialization = INITIALIZATION_ERROR;
            logger.error("Configuration error: AxisService 'ResourceInfoService' not found!");
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

        logger.info("starting the ResourceInfoService initialization...");

        CommandManagerInterface commandManager = null;
        try {
            commandManager = CommandManager.getInstance();
        } catch (Throwable t) {
            initialization = INITIALIZATION_ERROR;
            logger.error("Configuration error: cannot get instance of CommandManager: " + t.getMessage());
            return;
        }

        if (!commandManager.checkCommandExecutor("ActivityExecutor", ActivityCmd.ACTIVITY_MANAGEMENT)) {
            initialization = INITIALIZATION_ERROR;
            logger.error("Configuration error: ActivityExecutor not loaded!");
            return;
        }

        initialization = INITIALIZATION_OK;
        logger.info("ResourceInfoService initialization done!");
        logger.info("ResourceInfoService started!");
    }
    
    public GetResourceInfoResponse getResourceInfo(GetResourceInfo req) throws ResourceInfoNotFoundFault, InternalResourceInfoFault, AccessControlFault, InternalBaseFault {
        checkInitialization();
        logger.debug("BEGIN getResourceInfo");

        ActivityCmd command = null;

        try {
            command = new ActivityCmd(ActivityCommandName.GET_RESOURCE_INFO);

            CommandManager.getInstance().execute(command);
        } catch (Throwable t) {
            org.glite.ce.creamapi.ws.es.resourceinfo.types.InternalResourceInfoFault msg = new org.glite.ce.creamapi.ws.es.resourceinfo.types.InternalResourceInfoFault();
            msg.setDescription("internal error");
            msg.setMessage(t.getMessage());
            msg.setTimestamp(GregorianCalendar.getInstance());

            InternalResourceInfoFault fault = new InternalResourceInfoFault();
            fault.setFaultMessage(msg);

            throw fault;
        }

        ComputingService_t computingService = (ComputingService_t) command.getResult().getParameter(ActivityCommandField.COMPUTING_SERVICE.name());

        if (computingService == null) {
            String message = "ComputingService_t not available";

            org.glite.ce.creamapi.ws.es.resourceinfo.types.ResourceInfoNotFoundFault msg = new org.glite.ce.creamapi.ws.es.resourceinfo.types.ResourceInfoNotFoundFault();
            msg.setMessage(message);
            msg.setDescription(message);
            msg.setTimestamp(GregorianCalendar.getInstance());

            ResourceInfoNotFoundFault fault = new ResourceInfoNotFoundFault();
            fault.setFaultMessage(msg);

            throw fault;
        }
        
        URI uri = null;
        URI serviceID = null;
        String hostName = null;
        try {
            hostName = InetAddress.getLocalHost().getCanonicalHostName();
            uri = new URI("https://" + hostName + ":8443/ce-cream-es/services/ActivityManagementService");
            serviceID = new URI("urn:cream-es:ActivityManagementService:" + hostName);
        } catch (Throwable t) {
            initialization = INITIALIZATION_ERROR;
            logger.error("Configuration error: cannot get the host name: " + t.getMessage());
        }

//        Capability_t[] capability = new Capability_t[1];
//        capability[0] = new Capability_t();
//        capability[0].setCapability_t("security.delegation*");
//
//        DateTime_t creationTime = new DateTime_t(Calendar.getInstance());
//
//        Endpoint_t endpoint = new Endpoint_t();
//        // AccessPolicy_t[] accessPolicy = new AccessPolicy_t[1];
//        // accessPolicy[0] = new AccessPolicy_t();
//        // accessPolicy[0].setID(new URI("urn:" + hostName));
//        // endpoint.setAccessPolicy(accessPolicy);
//
//        // endpoint.setActivities(param);
//        endpoint.setCapability(capability);
//        endpoint.setCreationTime(creationTime);
//        // endpoint.setDowntimeAnnounce(param);
//        // endpoint.setDowntimeEnd(param);
//        // endpoint.setDowntimeInfo(param);
//        // endpoint.setDowntimeStart(param);
//        // endpoint.setExtensions(param);
//        endpoint.setHealthState(EndpointHealthState_t.ok);
//        endpoint.setHealthStateInfo("ok");
//        endpoint.setID(serviceID);
//        endpoint.setImplementationName("CREAM");
//        endpoint.setImplementationVersion("1.14.2");
//        endpoint.setImplementor("gLite");
//        // endpoint.setInterfaceExtension(param);
//        InterfaceName_t interfaceName = new InterfaceName_t();
//        interfaceName.setInterfaceName_t("org.ogf.glue.emies.delegation");
//        endpoint.setInterfaceName(interfaceName);
//        endpoint.setInterfaceVersion(new String[] { "1.14.5" });
//        // endpoint.setIssuerCA(param);
//        endpoint.setName("DelegationService");
//        // endpoint.setOtherInfo(param);
//        endpoint.setQualityLevel(QualityLevel_t.value1); // development
//        // endpoint.setSemantics(param);
//        endpoint.setServingState(ServingState_t.production); // to be modified
//        endpoint.setStartTime(creationTime);
//        // endpoint.setSupportedProfile(param);
//        // endpoint.setTrustedCA(param);
//        endpoint.setURL(uri);
//        // endpoint.setValidity(param);
//        try {
//            endpoint.addWSDL(new URI(uri.toString() + "/?wsdl"));
//        } catch (MalformedURIException e) {
//        }
//
//        EndpointTechnology_t technology = new EndpointTechnology_t();
//        technology.setEndpointTechnology_t("webservice");
//
//        endpoint.setTechnology(technology);
//
//        ServiceType_t type = new ServiceType_t();
//        type.setServiceType_t("org.glite.ce.CREAM");
//        
//        Service_t delegationService = new Service_t();
//        delegationService.addEndpoint(endpoint);
//        delegationService.setCapability(capability);
//        delegationService.setComplexity("endpointType=1,share=0,resource=1");
//        delegationService.setCreationTime(creationTime);
//        delegationService.setName("DelegationService");
//        delegationService.setQualityLevel(QualityLevel_t.value4); // testing
//        delegationService.setID(serviceID);
//        delegationService.setType(type);
//        // service.addOtherInfo(param);
//        // service.addStatusInfo(param);
//        // service.setExtensions(param);
//        // service.setLocation(param);
//        // service.setOtherInfo(param);
//        // service.setValidity(param);

        Services_type0 services = new Services_type0();
        services.addComputingService(computingService);
        //services.addService(delegationService);
        
        GetResourceInfoResponse response = new GetResourceInfoResponse();        
        response.setServices(services);

//        try {
//            // dump the output to console with caching
//            XMLStreamWriter writer = XMLOutputFactory.newInstance().createXMLStreamWriter(System.out);
//            response.serialize(GetResourceInfoResponse.MY_QNAME, writer);
//            writer.flush();
//            writer.close();
//
//        } catch (Throwable e) {
//            e.printStackTrace();
//        }
        logger.debug("END getResourceInfo");
        return response;
    }
    
    public QueryResourceInfoResponse queryResourceInfo(QueryResourceInfo req) throws NotValidQueryStatementFault, NotSupportedQueryDialectFault, InternalBaseFault,
            AccessControlFault {
        checkInitialization();
        logger.debug("BEGIN queryResourceInfo");

        if (req == null || req.getQueryExpression() == null) {
            throw new NotValidQueryStatementFault("query not defined!");
        }
        
        if (!req.getQueryDialect().toString().equals(DefaultQueryDialectsEnumType.value1.toString())) {
            throw new NotSupportedQueryDialectFault("dialect not supported; please define " + DefaultQueryDialectsEnumType.value1);
        }

        ActivityCmd command = null;
        try {
            command = new ActivityCmd(ActivityCommandName.QUERY_RESOURCE_INFO);
            command.addParameter(ActivityCommandField.XPATH_QUERY, req.getQueryExpression().getExtraElement().getText());

            CommandManager.getInstance().execute(command);
        } catch (Throwable t) {
            org.glite.ce.creamapi.ws.es.resourceinfo.types.InternalResourceInfoFault msg = new org.glite.ce.creamapi.ws.es.resourceinfo.types.InternalResourceInfoFault();
            msg.setDescription("internal error");
            msg.setMessage(t.getMessage());
            msg.setTimestamp(GregorianCalendar.getInstance());

            InternalResourceInfoFault fault = new InternalResourceInfoFault();
            fault.setFaultMessage(msg);
        }

        List queryResult = (List) command.getResult().getParameter(ActivityCommandField.ACTIVITY_GLUE2_ATTRIBUTE_LIST.name());

        QueryResourceInfoResponse response = new QueryResourceInfoResponse();
        Object omObj = null;

        if (queryResult != null) {
            OMNamespace ns = new OMNamespaceImpl("http://schemas.ogf.org/glue/2009/03/spec_2.0_r1", "xmlns");
            OMAttribute attribute = null;
            
            for (int i = 0; i < queryResult.size(); i++) {
                omObj = queryResult.get(i);
                
                if (omObj instanceof OMAttribute) {                                    
                    attribute = new OMAttributeImpl(((OMAttribute) omObj).getLocalName(), ns, ((OMAttribute) omObj).getAttributeValue(), ((OMAttribute) omObj).getOMFactory());
                    
                    QueryResourceInfoItem_type0 item = new QueryResourceInfoItem_type0();
                    item.addExtraAttributes(attribute);
                    
                    response.addQueryResourceInfoItem(item);     
                } else if (omObj instanceof OMElement) {      
                //    ((OMElement) omObj).declareDefaultNamespace("http://schemas.ogf.org/glue/2009/03/spec_2.0_r1");
              
                    QueryResourceInfoItem_type0 item = new QueryResourceInfoItem_type0();
                    item.setExtraElement((OMElement) omObj);
                    
                    response.addQueryResourceInfoItem(item); 
                }
            }
        }
//        try {
//            // dump the output to console with caching
//            XMLStreamWriter writer = XMLOutputFactory.newInstance().createXMLStreamWriter(System.out);
//            response.serialize(QueryResourceInfoResponse.MY_QNAME, writer);
//            writer.flush();
//            writer.close();
//
//        } catch (Throwable e) {
//            e.printStackTrace();
//        }
        logger.debug("END queryResourceInfo");
        return response;
    }
}
