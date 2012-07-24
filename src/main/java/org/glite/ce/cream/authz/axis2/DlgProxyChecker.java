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
import org.glite.ce.commonj.authz.axis2.AuthorizationModule;
import org.glite.ce.security.delegation.DelegationException;
import org.glite.voms.VOMSAttribute;
import org.glite.voms.VOMSValidator;
import org.glite.voms.ac.ACValidator;

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

                /*
                 * TODO proxy chain verification
                 */
                ACValidator acValidator = AuthorizationModule.getACValidator();
                VOMSValidator mainValidator = new VOMSValidator(delegProxyChain, acValidator);
                mainValidator.validate();

                X509Certificate userCert = ProxyUtils.getEndUserCertificate(delegProxyChain);
                String tmpDN = userCert.getSubjectDN().getName();

                if (clientDN.equals(tmpDN)) {
                    logger.info("Delegated proxy verified: " + tmpDN);
                } else {
                    throw new RuntimeException("Delegated proxy not verified: " + tmpDN);
                }

                List<VOMSAttribute> vomsList = (List<VOMSAttribute>) mainValidator.getVOMSAttributes();
                msgContext.setProperty(AuthZConstants.DLG_PROXY_ATTRIBUTES, vomsList);
                msgContext.setProperty(AuthZConstants.DLG_PROXY_CERT_LIST, delegProxyChain);

            } catch (Throwable th) {
                logger.error(th.getMessage(), th);
                throw this.getDelegationFault("Error checking delegated proxy: " + th.getMessage(), msgContext);
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
            logger.error(th.getMessage(), th);
        }

        return new AxisFault(faultReason);
    }

}
