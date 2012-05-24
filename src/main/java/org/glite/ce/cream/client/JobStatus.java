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

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

import org.apache.axis2.AxisFault;
import org.glite.ce.creamapi.ws.cream2.Authorization_Fault;
import org.glite.ce.creamapi.ws.cream2.Generic_Fault;
import org.glite.ce.creamapi.ws.cream2.InvalidArgument_Fault;
import org.glite.ce.creamapi.ws.cream2.types.JobStatusResult;
import org.glite.ce.creamapi.ws.cream2.types.Status;

public class JobStatus extends JobCommand {

    public JobStatus(String[] args, List<String> options) throws RuntimeException {
        super(args, options);
    }

    public void execute() {
        JobStatusResult[] result = null;
        try {
            result = getCREAMStub().jobStatus(getJobIdList(), getStatus(), getFromDate(), getToDate(), getDelegationId(), getLeaseId());
        } catch (Authorization_Fault e) {
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
        }

        if (result == null) {
            return;
        }

        for (int i = 0; i < result.length; i++) {
            System.out.println("-----------------------------------------------------------------------------");
            System.out.println("" + i + ") " + result[i].getJobDescriptionId() + "\n");

            if (result[i].getJobStatus() != null) {
                Status status = result[i].getJobStatus();
                System.out.println("jobId = " + status.getJobId().getId());
                System.out.println("name = " + status.getName());
                System.out.println("timestamp = " + status.getTimestamp().getTime());
                if (status.getDescription() != null) {
                    System.out.println("description = " + status.getDescription());
                }
                if (status.getFailureReason() != null) {
                    System.out.println("failure reason = " + status.getFailureReason());
                }
                if (status.getExitCode() != null) {
                    System.out.println("exit code = " + status.getExitCode());
                }
            } else {
                if (result[i].getDateMismatchFault() != null) {
                    printFault(result[i].getDateMismatchFault());
                } else if (result[i].getDelegationIdMismatchFault() != null) {
                    printFault(result[i].getDelegationIdMismatchFault());
                } else if (result[i].getGenericFault() != null) {
                    printFault(result[i].getGenericFault());
                } else if (result[i].getJobStatusInvalidFault() != null) {
                    printFault(result[i].getJobStatusInvalidFault());
                } else if (result[i].getJobUnknownFault() != null) {
                    printFault(result[i].getJobUnknownFault());
                } else if (result[i].getLeaseIdMismatchFault() != null) {
                    printFault(result[i].getLeaseIdMismatchFault());
                }
            }
        }
    }

    public static void main(String[] args) {
        List<String> options = new ArrayList<String>(8);
        options.add(EPR);
        options.add(PROXY);
        options.add(STATUS);
        options.add(ALL_JOBS);
        options.add(DELEGATION_ID);
        options.add(LEASE_ID);
        options.add(FROM_DATE);
        options.add(TO_DATE);

        new JobStatus(args, options);
    }
}
