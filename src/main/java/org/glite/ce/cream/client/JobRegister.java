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

import java.io.FileReader;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

import org.apache.axis2.AxisFault;
import org.glite.ce.creamapi.ws.cream2.Authorization_Fault;
import org.glite.ce.creamapi.ws.cream2.Generic_Fault;
import org.glite.ce.creamapi.ws.cream2.InvalidArgument_Fault;
import org.glite.ce.creamapi.ws.cream2.JobSubmissionDisabled_Fault;
import org.glite.ce.creamapi.ws.cream2.types.JobDescription;
import org.glite.ce.creamapi.ws.cream2.types.JobId;
import org.glite.ce.creamapi.ws.cream2.types.JobRegisterResult;
import org.glite.ce.creamapi.ws.cream2.types.Property;
import org.glite.ce.security.delegation.DelegationException_Fault;
import org.glite.ce.security.delegation.DelegationServiceStub;

public class JobRegister extends JobCommand {

    public JobRegister(String[] args, List<String> options) throws RuntimeException {
        super(args, options);
    }

    protected void execute() {
        String delegProxy = null;

        if (isDelegate()) {
            try {
                DelegationServiceStub delegationServiceStub = getDelegationServiceStub();
                String pkcs10 = delegationServiceStub.getProxyReq(getDelegationId());
                delegProxy = signRequest(pkcs10, getDelegationId());
                // delegation.putProxy(getDelegationId(), request);
            } catch (DelegationException_Fault e) {
                System.out.println(e.getFaultMessage());
            } catch (AxisFault e) {
                printFault(e);
            } catch (RemoteException e) {
                e.printStackTrace();
            } catch (IOException e) {
                System.out.println("error: " + e.getMessage());
            }
        }

        try {
            String[] jdlFileArray = getJdlFileArray();
            if (jdlFileArray == null) {
                System.out.println("no jdl specified!");
                return;
            }

            JobDescription[] jobDescr = new JobDescription[jdlFileArray.length];
            for (int i = 0; i < jdlFileArray.length; i++) {
                String jdl = readFile(jdlFileArray[i]);

                jobDescr[i] = new JobDescription();
                jobDescr[i].setJobDescriptionId("" + i);
                jobDescr[i].setJDL(jdl);
                jobDescr[i].setAutoStart(isAutostart());
                jobDescr[i].setDelegationId(getDelegationId());
                jobDescr[i].setDelegationProxy(delegProxy);
                jobDescr[i].setLeaseId(getLeaseId());
            }

            JobRegisterResult[] result = getCREAMStub().jobRegister(jobDescr, null);

            for (int i = 0; i < result.length; i++) {
                System.out.println("job descrption id: " + result[i].getJobDescriptionId());

                if (result[i].getJobId() != null) {
                    JobId jobId = result[i].getJobId();
                    System.out.println("jobId: " + jobId.getId());
                    System.out.println("cream url: " + jobId.getCreamURL());

                    Property[] property = jobId.getProperty();

                    if (property != null) {
                        for (int x = 0; x < property.length; x++) {
                            System.out.println("property: name=" + property[x].getName() + " value=\"" + property[x].getValue());
                        }
                    }
                } else {
                    System.out.println("fault returned: ");

                    if (result[i].getDelegationProxyFault() != null) {
                        printFault(result[i].getDelegationProxyFault());
                    } else if (result[i].getDelegationIdMismatchFault() != null) {
                        printFault(result[i].getDelegationIdMismatchFault());
                    } else if (result[i].getGenericFault() != null) {
                        printFault(result[i].getGenericFault());
                    } else if (result[i].getLeaseIdMismatchFault() != null) {
                        printFault(result[i].getLeaseIdMismatchFault());
                    }
                }
            }
        } catch (Authorization_Fault e) {
            printFault(e.getFaultMessage());
        } catch (JobSubmissionDisabled_Fault e) {
            printFault(e.getFaultMessage());
        } catch (Generic_Fault gf) {
            if (gf.getFaultMessage() != null) {
                printFault(gf.getFaultMessage().getGenericFault());
            }
        } catch (InvalidArgument_Fault e) {
            printFault(e.getFaultMessage());
        } catch (AxisFault e) {
            printFault(e);
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("error: " + e.getMessage());
        }
    }

    private String readFile(String filename) throws IOException {
        String res = "";

        FileReader in = new FileReader(filename);

        char[] buffer = new char[1024];
        int n = 1;

        while (n > 0) {
            n = in.read(buffer, 0, buffer.length);

            if (n > 0) {
                res += new String(buffer, 0, n);
            }
        }

        in.close();
        return res;
    }

    public static void main(String[] args) {
        List<String> options = new ArrayList<String>(7);
        options.add(EPR);
        options.add(PROXY);
        options.add(DELEGATION_ID);
        options.add(LEASE_ID);
        options.add(AUTOSTART);
        options.add(DELEGATE);
        options.add(JDL_FILE);

        new JobRegister(args, options);
    }
}
