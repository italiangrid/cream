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

import java.io.ByteArrayInputStream;
import java.security.cert.X509Certificate;
import java.util.List;

import javax.security.auth.x500.X500Principal;
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
import org.apache.axis2.engine.Handler;
import org.apache.axis2.handlers.AbstractHandler;
import org.apache.log4j.Logger;
import org.glite.ce.commonj.authz.AuthZConstants;
import org.glite.ce.commonj.authz.VOMSResultCollector;
import org.glite.ce.commonj.authz.axis2.AuthorizationModule;
import org.glite.ce.security.delegation.DelegationException;
import org.italiangrid.voms.VOMSAttribute;
import org.italiangrid.voms.VOMSValidators;
import org.italiangrid.voms.ac.VOMSACValidator;

import eu.emi.security.authn.x509.ValidationError;
import eu.emi.security.authn.x509.ValidationResult;
import eu.emi.security.authn.x509.impl.CertificateUtils;
import eu.emi.security.authn.x509.proxy.ProxyUtils;

public class DlgProxyChecker
    extends AbstractHandler {

    private static final Logger logger = Logger.getLogger(DlgProxyChecker.class.getName());

    private static final String DELEG_NS = "http://www.gridsite.org/namespaces/delegation-2";

    public Handler.InvocationResponse invoke(MessageContext msgContext)
        throws AxisFault {

        String clientDN = (String) msgContext.getProperty(AuthZConstants.USERDN_RFC2253_LABEL);

        OperationContext opCtx = msgContext.getOperationContext();
        AxisOperation axisOp = opCtx.getAxisOperation();
        QName operation = axisOp.getName();

        if (operation.getLocalPart().equalsIgnoreCase("putProxy")) {
            SOAPEnvelope envelop = msgContext.getEnvelope();
            SOAPBody body = envelop.getBody();
            OMElement putProxyMsg = body.getFirstChildWithName(new QName(DELEG_NS, "putProxy"));
            if (putProxyMsg == null) {
                logger.error("Malformed request for user " + clientDN);
                throw this.getDelegationFault("Malformed request", msgContext);
            }

            OMElement proxyElem = putProxyMsg.getFirstChildWithName(new QName(DELEG_NS, "proxy"));
            if (proxyElem == null) {
                logger.error("Malformed request for user " + clientDN);
                throw this.getDelegationFault("Malformed request", msgContext);
            }

            String proxyBase64 = proxyElem.getText();
            X509Certificate[] delegProxyChain = null;

            try {

                ByteArrayInputStream strStream = new ByteArrayInputStream(proxyBase64.getBytes());
                delegProxyChain = CertificateUtils.loadCertificateChain(strStream, CertificateUtils.Encoding.PEM);

                ValidationResult vRes = AuthorizationModule.validator.validate(delegProxyChain);
                if (!vRes.isValid()) {

                    StringBuffer buff = new StringBuffer("Proxy certificate is not valid\n");
                    for (ValidationError vErr : vRes.getErrors()) {
                        buff.append(vErr.getMessage()).append("\n");
                    }

                    String tmps = buff.toString();
                    logger.warn(tmps);
                    throw getDelegationFault(tmps, msgContext);
                }

                X500Principal userPrincipal = ProxyUtils.getOriginalUserDN(delegProxyChain);
                if (userPrincipal == null || !clientDN.equals(userPrincipal.getName())) {
                    throw getDelegationFault("Proxy DN doesn't match user DN", msgContext);
                }

                VOMSResultCollector collector = new VOMSResultCollector();
                VOMSACValidator validator = VOMSValidators.newValidator(AuthorizationModule.vomsStore,
                        AuthorizationModule.validator, collector);
                List<VOMSAttribute> vomsList = validator.validate(delegProxyChain);

                if (collector.size() > 0) {
                    String tmps = "Cannot validate VOMS attributes in proxy\n" + collector.toString();
                    throw getDelegationFault(tmps, msgContext);
                }

                logger.info("Verified delegated proxy for " + userPrincipal.getName());

                msgContext.setProperty(AuthZConstants.DLG_PROXY_ATTRIBUTES, vomsList);
                msgContext.setProperty(AuthZConstants.DLG_PROXY_CERT_LIST, delegProxyChain);

            } catch (Throwable th) {
                if (logger.isDebugEnabled()) {
                    logger.error("Error checking delegated proxy", th);
                } else {
                    logger.error("Error checking delegated proxy: " + th.getMessage());
                }
                throw getDelegationFault("Error checking delegated proxy: " + th.getMessage(), msgContext);
            }

        }

        return Handler.InvocationResponse.CONTINUE;
    }

    protected AxisFault getDelegationFault(String message, MessageContext msgContext) {

        SOAPFactory soapFactory;
        if (msgContext.isSOAP11()) {
            soapFactory = OMAbstractFactory.getSOAP11Factory();
        } else {
            soapFactory = OMAbstractFactory.getSOAP12Factory();
        }

        QName faultCode = new QName("http://www.w3.org/2003/05/soap-envelope", "Sender", "env");
        String faultReason = "Delegation error";
        DelegationException delegFault = new DelegationException();
        delegFault.setMsg(message);

        try {

            OMElement faultDetail = delegFault.getOMElement(null, soapFactory);
            return new AxisFault(faultCode, faultReason, null, null, faultDetail);

        } catch (Throwable th) {
            if (logger.isDebugEnabled()) {
                logger.error(th.getMessage(), th);
            } else {
                logger.error(th.getMessage());
            }
        }

        return new AxisFault(faultReason);
    }

}
