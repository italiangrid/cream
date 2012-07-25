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

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

import org.apache.axis2.AxisFault;
import org.glite.ce.creamapi.ws.es.delegation.AccessControlFault;
import org.glite.ce.creamapi.ws.es.delegation.InternalBaseFault;
import org.glite.ce.creamapi.ws.es.delegation.InternalServiceDelegationFault;
import org.glite.ce.creamapi.ws.es.delegation.UnknownDelegationIDFault;
import org.glite.ce.creamapi.ws.es.delegation.types.GetDelegationInfo;
import org.glite.ce.creamapi.ws.es.delegation.types.GetDelegationInfoResponse;
import org.glite.ce.creamapi.ws.es.delegation.types.InitDelegation;
import org.glite.ce.creamapi.ws.es.delegation.types.InitDelegationResponse;
import org.glite.ce.creamapi.ws.es.delegation.types.PutDelegation;

public class DelegationClient extends ActivityCommand {

    public DelegationClient(String[] args, List<String> options) throws RuntimeException {
        super(args, options);
    }

    public void execute() {
        if (isGetDelegationInfo()) {
            if (getIdList().size() == 0) {
                printUsage();
                return;
            }

            for (String delegationId : getIdList()) {
                System.out.println("getting info about the delegation " + delegationId);

                try {
                    GetDelegationInfo getDelegationInfo = new GetDelegationInfo();
                    getDelegationInfo.setDelegationID(delegationId);

                    GetDelegationInfoResponse response = getDelegationServiceStub().getDelegationInfo(getDelegationInfo);

                    System.out.println("issuer = " + response.getIssuer());
                    System.out.println("subject = " + response.getSubject());
                    System.out.println("lifetime = " + response.getLifetime().getTime());
                } catch (AxisFault e) {
                    System.out.println(e.getMessage());
                } catch (RemoteException e) {
                    System.out.println(e.getMessage());
                } catch (InternalBaseFault e) {
                    System.out.println(e.getFaultMessage().getInternalBaseFault().getMessage());
                } catch (AccessControlFault e) {
                    System.out.println(e.getFaultMessage().getMessage());
                } catch (UnknownDelegationIDFault e) {
                    System.out.println(e.getFaultMessage().getMessage());
                }
            }
        } else {
            InitDelegation initDelegation = new InitDelegation();
            initDelegation.setCredentialType("RFC3820");

            if (isRenew()) {
                if (getIdList().size() == 0) {
                    printUsage();
                    return;
                }

                for (String delegationId : getIdList()) {
                    System.out.println("renewing the delegation " + delegationId);

                    initDelegation.setRenewalID(delegationId);

                    try {
                        InitDelegationResponse response = getDelegationServiceStub().initDelegation(initDelegation);

                        String csr = response.getCSR();
                        String delegId = response.getDelegationID();

                        if (delegId == null) {
                            System.out.println("cannot get the delegationId!");
                            System.exit(0);
                        }

                        if (csr == null) {
                            System.out.println("cannot get the certificate request!");
                            continue;
                        }

                        String credential = signRequest(csr, delegationId);

                        PutDelegation putDelegation = new PutDelegation();
                        putDelegation.setDelegationID(delegationId);
                        putDelegation.setCredential(credential);

                        System.out.println(getDelegationServiceStub().putDelegation(putDelegation).getPutDelegationResponse());
                    } catch (AxisFault e) {
                        System.out.println(e.getMessage());
                    } catch (RemoteException e) {
                        System.out.println(e.getMessage());
                    } catch (InternalBaseFault e) {
                        System.out.println(e.getFaultMessage().getInternalBaseFault().getMessage());
                    } catch (AccessControlFault e) {
                        System.out.println(e.getFaultMessage().getMessage());
                    } catch (UnknownDelegationIDFault e) {
                        System.out.println(e.getFaultMessage().getMessage());
                    } catch (InternalServiceDelegationFault e) {
                        System.out.println(e.getFaultMessage().getMessage());
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                    }
                }
            } else {
                if (getIdList().size() > 0) {
                    printUsage();
                    return;
                }
                
                System.out.println("creating a new delegation...");

                try {
                    InitDelegationResponse response = getDelegationServiceStub().initDelegation(initDelegation);
                    String csr = response.getCSR();
                    String delegationId = response.getDelegationID();

                    if (delegationId == null) {
                        System.out.println("cannot get the delegationId!");
                        System.exit(0);
                    }

                    if (csr == null) {
                        System.out.println("cannot get the certificate request!");
                        System.exit(0);
                    }

                    System.out.println("delegationId = " + response.getDelegationID());
                    String credential = signRequest(csr, delegationId);

                    PutDelegation putDelegation = new PutDelegation();
                    putDelegation.setDelegationID(delegationId);
                    putDelegation.setCredential(credential);

                    System.out.println(getDelegationServiceStub().putDelegation(putDelegation).getPutDelegationResponse());
                } catch (AxisFault e) {
                    System.out.println(e.getMessage());
                } catch (RemoteException e) {
                    System.out.println(e.getMessage());
                } catch (InternalBaseFault e) {
                    System.out.println(e.getFaultMessage().getInternalBaseFault().getMessage());
                } catch (AccessControlFault e) {
                    System.out.println(e.getFaultMessage().getMessage());
                } catch (UnknownDelegationIDFault e) {
                    System.out.println(e.getFaultMessage().getMessage());
                } catch (InternalServiceDelegationFault e) {
                    System.out.println(e.getFaultMessage().getMessage());
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            }
        }
    }

    public static void main(String[] args) {
        List<String> options = new ArrayList<String>(8);
        options.add(EPR);
        options.add(PROXY);
        options.add(RENEW_DELEGATION);
        options.add(GET_DELEGATION_INFO);

        new DelegationClient(args, options);
    }
}