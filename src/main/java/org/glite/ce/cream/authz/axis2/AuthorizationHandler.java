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

package org.glite.ce.cream.authz.axis2;

import java.security.cert.X509Certificate;
import java.util.Calendar;

import javax.xml.namespace.QName;

import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.soap.SOAPBody;
import org.apache.axiom.soap.SOAPEnvelope;
import org.apache.axiom.soap.SOAPFactory;
import org.apache.axis2.AxisFault;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.context.OperationContext;
import org.apache.axis2.description.AxisOperation;
import org.apache.log4j.Logger;
import org.glite.ce.commonj.authz.AuthZConstants;
import org.glite.ce.commonj.configuration.CommonServiceConfig;
import org.glite.ce.cream.configuration.ServiceConfig;
import org.glite.ce.creamapi.ws.cream2.types.AuthorizationFault;
import org.glite.ce.security.delegation.DelegationException;

import eu.emi.security.authn.x509.proxy.ProxyChainInfo;

public class AuthorizationHandler
    extends org.glite.ce.commonj.authz.axis2.AuthorizationHandler {

    private static final Logger logger = Logger.getLogger(AuthorizationHandler.class.getName());

    private static final String DELEGATION_NS = "http://www.gridsite.org/namespaces/delegation-2";

    private static final String CREAM_NS = "http://glite.org/2007/11/ce/cream";

    static final long serialVersionUID = 1273759224;

    public AuthorizationHandler() {
        super();
    }

    protected AxisFault getAuthorizationFault(String message, MessageContext msgContext) {

        SOAPFactory soapFactory;
        if (msgContext.isSOAP11()) {
            soapFactory = OMAbstractFactory.getSOAP11Factory();
        } else {
            soapFactory = OMAbstractFactory.getSOAP12Factory();
        }

        QName faultCode = new QName("http://www.w3.org/2003/05/soap-envelope", "Sender", "env");
        String faultReason = "Authorization error";

        try {

            OMElement faultDetail = null;
            QName operation = this.getOperation(msgContext);

            if (operation == null) {

                logger.error("Cannot find operation");

            } else if (operation.getNamespaceURI().equals(DELEGATION_NS)) {

                DelegationException delegFault = new DelegationException();
                delegFault.setMsg(message);

                faultDetail = delegFault.getOMElement(null, soapFactory);

            } else if (operation.getNamespaceURI().startsWith(CREAM_NS)) {

                logger.debug("Building a cream legacy fault with message: " + message);
                AuthorizationFault authzFault = new AuthorizationFault();
                authzFault.setDescription(message);
                authzFault.setErrorCode("0");
                authzFault.setFaultCause(faultReason);
                authzFault.setMethodName(operation.getLocalPart());
                authzFault.setTimestamp(Calendar.getInstance());

                faultDetail = authzFault.getOMElement(null, soapFactory);

            } else {
                logger.error("Unreachable condition for " + operation.toString() + " operation.getNamespaceURI() = "
                        + operation.getNamespaceURI());
            }

            return new AxisFault(faultCode, faultReason, null, null, faultDetail);

        } catch (Exception ex) {
            if (logger.isDebugEnabled()) {
                logger.error(ex.getMessage(), ex);
            } else {
                logger.error(ex.getMessage());
            }
        }

        return new AxisFault(faultReason);
    }

    protected CommonServiceConfig getCommonConfiguration() {

        return ServiceConfig.getConfiguration();

    }

    protected QName getOperation(MessageContext context) {
        OperationContext opCtx = context.getOperationContext();
        AxisOperation operation = opCtx.getAxisOperation();

        QName result = operation.getName();

        /*
         * workaround for axis2
         */
        if (result.getNamespaceURI().isEmpty()) {
            SOAPEnvelope envelope = context.getEnvelope();
            SOAPBody body = envelope.getBody();
            OMElement elem = body.getFirstElement();
            if (elem != null) {
                String opNamespace = elem.getNamespace().getNamespaceURI();
                logger.debug("Workaround detected: " + opNamespace);
                result = new QName(opNamespace, result.getLocalPart());
            } else {
                /*
                 * workaround for joblist
                 */
                result = new QName(CREAM_NS, "JobList");
            }
        }

        return result;
    }

    protected void checkOperation(QName operation, MessageContext context)
        throws AxisFault {

        logger.debug("Checking operation: " + operation.getLocalPart());
        if (operation.getLocalPart().equals("JobRegister")) {
            try {
                X509Certificate[] certChain = (X509Certificate[]) context
                        .getProperty(AuthZConstants.USER_CERTCHAIN_LABEL);
                ProxyChainInfo pChainInfo = new ProxyChainInfo(certChain);
                if (pChainInfo.isLimited()) {
                    throw getAuthorizationFault("Cannot submit job: proxy is limited", context);
                }
            } catch (AxisFault fault) {
                throw fault;
            } catch (Exception ex) {
                throw getAuthorizationFault("Cannot check operation: " + ex.getMessage(), context);
            }
        }

    }

}
