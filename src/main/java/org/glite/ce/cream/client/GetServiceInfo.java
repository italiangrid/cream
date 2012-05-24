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
import org.glite.ce.creamapi.ws.cream2.types.Property;
import org.glite.ce.creamapi.ws.cream2.types.ServiceInfo;
import org.glite.ce.creamapi.ws.cream2.types.ServiceMessage;

public class GetServiceInfo extends JobCommand {

    public GetServiceInfo(String[] args, List<String> options) throws RuntimeException {
        super(args, options);
    }

    public void execute() {
        ServiceInfo info = null;
        try {
            info = getCREAMStub().getServiceInfo(2);
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

        System.out.println("service descrition: " + (info.getDescription() != null? info.getDescription(): "N/A"));
        System.out.println("interface version: " + info.getInterfaceVersion());
        System.out.println("service version: " + info.getServiceVersion());
        System.out.println("service startup time: " + info.getStartupTime().getTime());
        System.out.println("service status: " + info.getStatus());
        System.out.println("service does accept new jobs: " + info.getDoesAcceptNewJobSubmissions());

        ServiceMessage[] message = info.getMessage();
        System.out.print("messages: ");
        if (message == null) {
            System.out.println("0");
        } else {
            System.out.println(message.length);
            for (int i = 0; i < message.length; i++) {
                System.out.println("" + i + ") " + message[i].getMessage() + " type: " + message[i].getType() + " timestamp: " + message[i].getTimastamp().getTime());
            }
        }

        Property[] property = info.getProperty();
        System.out.print("properties: ");
        if (property == null) {
            System.out.println("0");
        } else {
            System.out.println(property.length);
            for (int i = 0; i < property.length; i++) {
                System.out.println("" + i + ") " + property[i].getName() + " = " + property[i].getValue());
            }
        }
    }

    public static void main(String[] args) {
        List<String> options = new ArrayList<String>(2);
        options.add(EPR);
        options.add(PROXY);

        new GetServiceInfo(args, options);
    }
}
