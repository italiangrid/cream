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

public class JobCancel extends JobCommand {

    public JobCancel(String[] args, List<String> options) throws RuntimeException {
        super(args, options);
    }

    public void execute() {
        try {
            printResult(getCREAMStub().jobCancel(getJobIdList(), getStatus(), getFromDate(), getToDate(), getDelegationId(), getLeaseId()), "job being cancelled");
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
        
        new JobCancel(args, options);
    }
}
