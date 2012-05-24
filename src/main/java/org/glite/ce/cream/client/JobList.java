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
import org.glite.ce.creamapi.ws.cream2.types.JobId;

public class JobList extends JobCommand {

    public JobList(String[] args, List<String> options) throws RuntimeException {
        super(args, options);
    }

    protected void execute() {
        JobId[] result = null;

        try {
            result = getCREAMStub().jobList();
        } catch (Authorization_Fault e) {
            printFault(e.getFaultMessage());
        } catch (Generic_Fault gf) {
            if (gf.getFaultMessage() != null) {
                printFault(gf.getFaultMessage().getGenericFault());
            }
        } catch (AxisFault e) {
            printFault(e);
        } catch (RemoteException e) {
            e.printStackTrace();
        }

        if (result != null) {
            for (int i = 0; i < result.length; i++) {
                System.out.println("" + i + ") " + result[i].getId());
            }
        }
    }

    public static void main(String[] args) {
        List<String> options = new ArrayList<String>(2);
        options.add(EPR);
        options.add(PROXY);

        new JobList(args, options);
    }
}
