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
import java.rmi.RemoteException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.axis2.AxisFault;
import org.glite.ce.creamapi.ws.es.delegation.DelegationExceptionException;
import org.glite.ce.creamapi.ws.es.delegation.GetNewProxyReqResponse;
import org.glite.ce.creamapi.ws.es.delegation.RenewProxyReq;

public class DelegationClient extends ActivityCommand {

    public DelegationClient(String[] args, List<String> options) throws RuntimeException {
        super(args, options);
    }

    public void execute() {
        if (isGetVersion()) {
            try {
                String version = getDelegationServiceStub().getVersion();
                System.out.println("version: " + version);
            } catch (AxisFault e) {
                System.out.println("AxisFault: " + e.getMessage());
            } catch (RemoteException e) {
                System.out.println("RemoteException: " + e.getMessage());
            } catch (DelegationExceptionException e) {
                System.out.println("(DelegationException: " + e.getMessage());
            } 
        } else if (isGetInterfaceVersion()) {
            try {
                String ifaceVersion = getDelegationServiceStub().getInterfaceVersion();
                System.out.println("version: " + ifaceVersion);
            } catch (AxisFault e) {
                System.out.println("AxisFault: " + e.getMessage());
            } catch (RemoteException e) {
                System.out.println("RemoteException: " + e.getMessage());
            } catch (DelegationExceptionException e) {
                System.out.println("(DelegationException: " + e.getMessage());
            }
        } else if (isGetTerminationTime()) {
            List<String> idList = getIdList();
            if (idList.size() == 0) {
                printUsage();
                return;
            }

            try {
                DateFormat dFormat = DateFormat.getInstance();
                Date tmpDate = getDelegationServiceStub().getTerminationTime(idList.get(0)).getTime();
                System.out.println("time: " + dFormat.format(tmpDate));
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

                String certReq = getDelegationServiceStub().getProxyReq(idList.get(0));
                System.out.println("request: " + certReq);

                getDelegationServiceStub().putProxy(idList.get(0), signRequest(certReq, idList.get(0)));
                
            } catch (AxisFault e) {
                System.out.println("AxisFault: " + e.getMessage());
            } catch (RemoteException e) {
                System.out.println("RemoteException: " + e.getMessage());
            } catch (DelegationExceptionException e) {
                System.out.println("(DelegationException: " + e.getMessage());
            } catch (Exception e) {
                System.out.println("(IOException: " + e.getMessage());
            }            
        } else if (isGetNewProxyRequest()) {
            try {
                GetNewProxyReqResponse response = getDelegationServiceStub().getNewProxyReq();
                System.out.println("id: " + response.getDelegationID());
                
                String signedReq = signRequest(response.getDelegationID(), response.getProxyRequest());

                getDelegationServiceStub().putProxy(response.getDelegationID(), signedReq);
                
            } catch (AxisFault e) {
                System.out.println("AxisFault: " + e.getMessage());
            } catch (RemoteException e) {
                System.out.println("RemoteException: " + e.getMessage());
            } catch (DelegationExceptionException e) {
                System.out.println("(DelegationException: " + e.getMessage());
            } catch (Exception e) {
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
                
                String certReq = getDelegationServiceStub().renewProxyReq(idList.get(0));
                
                String signedReq = signRequest(certReq, idList.get(0));
                getDelegationServiceStub().putProxy(idList.get(0), signedReq);
                
            } catch (AxisFault e) {
                System.out.println("AxisFault: " + e.getMessage());
            } catch (RemoteException e) {
                System.out.println("RemoteException: " + e.getMessage());
            } catch (DelegationExceptionException e) {
                System.out.println("(DelegationException: " + e.getMessage());
            }  catch (Exception e) {
                System.out.println("(IOException: " + e.getMessage());
            }      
        } else if (isDestroy()) {
            List<String> idList = getIdList();
            if (idList.size() == 0) {
                printUsage();
                return;
            }
            
            try {
                getDelegationServiceStub().destroy(idList.get(0));
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
