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

package org.glite.ce.cream.client.es;
import javax.xml.namespace.QName;
import javax.xml.parsers.FactoryConfigurationError;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

import org.apache.axis2.AxisFault;
import org.glite.ce.creamapi.ws.es.delegation.DelegationExceptionException;
import org.glite.ce.creamapi.ws.es.delegation.Destroy;
import org.glite.ce.creamapi.ws.es.delegation.GetInterfaceVersion;
import org.glite.ce.creamapi.ws.es.delegation.GetInterfaceVersionResponse;
import org.glite.ce.creamapi.ws.es.delegation.GetNewProxyReq;
import org.glite.ce.creamapi.ws.es.delegation.GetNewProxyReqResponse;
import org.glite.ce.creamapi.ws.es.delegation.GetProxyReq;
import org.glite.ce.creamapi.ws.es.delegation.GetProxyReqResponse;
import org.glite.ce.creamapi.ws.es.delegation.GetTerminationTime;
import org.glite.ce.creamapi.ws.es.delegation.GetTerminationTimeResponse;
import org.glite.ce.creamapi.ws.es.delegation.GetVersion;
import org.glite.ce.creamapi.ws.es.delegation.GetVersionResponse;
import org.glite.ce.creamapi.ws.es.delegation.PutProxy;
import org.glite.ce.creamapi.ws.es.delegation.RenewProxyReq;
import org.glite.ce.creamapi.ws.es.delegation.RenewProxyReqResponse;

public class DelegationClient extends ActivityCommand {

    public DelegationClient(String[] args, List<String> options) throws RuntimeException {
        super(args, options);
    }

    public void execute() {
        if (isGetVersion()) {
            try {
                GetVersionResponse response = getDelegationServiceStub().getVersion(new GetVersion());
                System.out.println("version: " + response.getGetVersionReturn());
            } catch (AxisFault e) {
                System.out.println("AxisFault: " + e.getMessage());
            } catch (RemoteException e) {
                System.out.println("RemoteException: " + e.getMessage());
            } catch (DelegationExceptionException e) {
                System.out.println("(DelegationException: " + e.getMessage());
            } 
        } else if (isGetInterfaceVersion()) {
            try {
                GetInterfaceVersionResponse response = getDelegationServiceStub().getInterfaceVersion(new GetInterfaceVersion());
                System.out.println("version: " + response.getGetInterfaceVersionReturn());
            } catch (AxisFault e) {
                System.out.println("AxisFault: " + e.getMessage());
            } catch (RemoteException e) {
                System.out.println("RemoteException: " + e.getMessage());
            } catch (DelegationExceptionException e) {
                System.out.println("(DelegationException: " + e.getMessage());
            }
        } else if (isGetTerminationTime()) {
            try {
                GetTerminationTimeResponse response = getDelegationServiceStub().getTerminationTime(new GetTerminationTime());
                System.out.println("time: " + response.getGetTerminationTimeReturn());
            } catch (AxisFault e) {
                System.out.println("AxisFault: " + e.getMessage());
            } catch (RemoteException e) {
                System.out.println("RemoteException: " + e.getMessage());
            } catch (DelegationExceptionException e) {
                System.out.println("(DelegationException: " + e.getMessage());
            }           
        } else if (isGetProxyRequest()) {
            List<String> idList = getIdList();
            if (idList.size() == 0) {
                printUsage();
                return;
            }
            
            try {
                GetProxyReq req = new GetProxyReq();
                req.setDelegationID(idList.get(0));

                GetProxyReqResponse response = getDelegationServiceStub().getProxyReq(req);
                System.out.println("request: " + response.getGetProxyReqReturn());

                PutProxy putProxyReq = new PutProxy();
                putProxyReq.setDelegationID(idList.get(0));
                putProxyReq.setProxy(signRequest(response.getGetProxyReqReturn()));

                getDelegationServiceStub().putProxy(putProxyReq);
            } catch (AxisFault e) {
                System.out.println("AxisFault: " + e.getMessage());
            } catch (RemoteException e) {
                System.out.println("RemoteException: " + e.getMessage());
            } catch (DelegationExceptionException e) {
                System.out.println("(DelegationException: " + e.getMessage());
            } catch (IOException e) {
                System.out.println("(IOException: " + e.getMessage());
            }            
        } else if (isGetNewProxyRequest()) {
            try {
                GetNewProxyReqResponse response = getDelegationServiceStub().getNewProxyReq(new GetNewProxyReq());
                System.out.println("id: " + response.getDelegationID());
                //System.out.println("request: " + response.getProxyRequest());
                
                PutProxy putProxyReq = new PutProxy();
                putProxyReq.setDelegationID(response.getDelegationID());
                putProxyReq.setProxy(signRequest(response.getProxyRequest()));

                getDelegationServiceStub().putProxy(putProxyReq);
            } catch (AxisFault e) {
                System.out.println("AxisFault: " + e.getMessage());
            } catch (RemoteException e) {
                System.out.println("RemoteException: " + e.getMessage());
            } catch (DelegationExceptionException e) {
                System.out.println("(DelegationException: " + e.getMessage());
            } catch (IOException e) {
                System.out.println("(IOException: " + e.getMessage());
            } 
        } else if (isRenewProxyRequest()) {
            List<String> idList = getIdList();
            if (idList.size() == 0) {
                printUsage();
                return;
            }
            
            try {                
                RenewProxyReq req = new RenewProxyReq();
                req.setDelegationID(idList.get(0));
                
                RenewProxyReqResponse response = getDelegationServiceStub().renewProxyReq(req);
                
                PutProxy putProxyReq = new PutProxy();
                putProxyReq.setDelegationID(idList.get(0));
                putProxyReq.setProxy(signRequest(response.getRenewProxyReqReturn()));
                getDelegationServiceStub().putProxy(putProxyReq);
            } catch (AxisFault e) {
                System.out.println("AxisFault: " + e.getMessage());
            } catch (RemoteException e) {
                System.out.println("RemoteException: " + e.getMessage());
            } catch (DelegationExceptionException e) {
                System.out.println("(DelegationException: " + e.getMessage());
            }  catch (IOException e) {
                System.out.println("(IOException: " + e.getMessage());
            }      
        } else if (isDestroy()) {
            List<String> idList = getIdList();
            if (idList.size() == 0) {
                printUsage();
                return;
            }
            
            try {
                Destroy req = new Destroy();
                req.setDelegationID(idList.get(0));

                getDelegationServiceStub().destroy(req);
            } catch (AxisFault e) {
                System.out.println("AxisFault: " + e.getMessage());
            } catch (RemoteException e) {
                System.out.println("RemoteException: " + e.getMessage());
            } catch (DelegationExceptionException e) {
                System.out.println("(DelegationException: " + e.getMessage());
            }  
        } else {
            printUsage();
        }
    }

    public static void main(String[] args) {
        List<String> options = new ArrayList<String>(9);
        options.add(EPR);
        options.add(PROXY);
        options.add(GET_INTERFACE_VERSION);
        options.add(GET_NEW_PROXY_REQUEST);
        options.add(GET_PROXY_REQUEST);
        options.add(GET_TERMINATION_TIME);
        options.add(GET_VERSION);
        options.add(RENEW_PROXY_REQUEST);
        options.add(DESTROY);

        new DelegationClient(args, options);
    }
}
