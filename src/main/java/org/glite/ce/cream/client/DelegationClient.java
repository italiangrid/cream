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

package org.glite.ce.cream.client;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

import org.apache.axis2.AxisFault;
import org.glite.ce.security.delegation.DelegationException_Fault;
import org.glite.ce.security.delegation.DelegationServiceStub;

public class DelegationClient extends JobCommand {

    public DelegationClient(String[] args, List<String> options) throws RuntimeException {
        super(args, options);
    }

    public void execute() {
        if (getDelegationId() == null) {
            printUsage();
        }

        try {
            String pkcs10 = null;

            DelegationServiceStub delegationServiceStub = getDelegationServiceStub();
            if (isRenew()) {
                pkcs10 = delegationServiceStub.renewProxyReq(getDelegationId());
            } else {
                pkcs10 = delegationServiceStub.getProxyReq(getDelegationId());
            }

            if (pkcs10 == null) {
                System.out.println("cannot get the certificate request!");
                System.exit(0);
            }

            String request = signRequest(pkcs10, getDelegationId());
            delegationServiceStub.putProxy(getDelegationId(), request);
        } catch (DelegationException_Fault e) {
            System.out.println(e.getFaultMessage());
        } catch (AxisFault e) {
            printFault(e);
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (Exception e) {
            System.out.println("error: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        List<String> options = new ArrayList<String>(4);
        options.add(EPR);
        options.add(PROXY);
        options.add(DELEGATION_ID);
        options.add(RENEW);

        new DelegationClient(args, options);
    }
}
