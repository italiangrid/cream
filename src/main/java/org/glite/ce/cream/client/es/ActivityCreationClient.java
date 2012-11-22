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

import java.io.FileReader;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;

import org.apache.axis2.AxisFault;
import org.apache.axis2.databinding.types.URI;
//import org.glite.ce.creamapi.ws.es.creation.AccessControlFault;
import org.glite.ce.creamapi.ws.es.creation.AccessControlFault;
import org.glite.ce.creamapi.ws.es.creation.InternalBaseFault;
import org.glite.ce.creamapi.ws.es.creation.VectorLimitExceededFault;
import org.glite.ce.creamapi.ws.es.creation.types.ActivityCreationResponseSequence_type1;
import org.glite.ce.creamapi.ws.es.creation.types.ActivityCreationResponse_type0;
import org.glite.ce.creamapi.ws.es.creation.types.ActivityStatusAttribute;
import org.glite.ce.creamapi.ws.es.creation.types.ActivityStatus_type0;
import org.glite.ce.creamapi.ws.es.creation.types.CreateActivity;
import org.glite.ce.creamapi.ws.es.creation.types.CreateActivityResponse;
import org.glite.ce.creamapi.ws.es.creation.types.DirectoryReference;

public class ActivityCreationClient extends ActivityCommand {

    public static void main(String[] args) {
        List<String> options = new ArrayList<String>(8);
        options.add(EPR);
        options.add(PROXY);
        options.add(ADL);

        new ActivityCreationClient(args, options);
    }

    public ActivityCreationClient(String[] args, List<String> options) throws RuntimeException {
        super(args, options);
    }

    private String checkValue(ActivityStatus_type0 activityStatus) {
        if (activityStatus == null || activityStatus.getStatus() == null) {
            return "N/A";
        }

        return activityStatus.getStatus().getValue();
    }

    private String checkValue(ActivityStatusAttribute[] attribute) {
        StringBuffer buffer = new StringBuffer("[ ");

        if (attribute == null) {
            buffer.append("]");
        } else {
            for (int i = 0; i < attribute.length; i++) {
                buffer.append(attribute[i].getValue()).append(", ");
            }

            buffer.replace(buffer.length() - 2, buffer.length(), " ]");
        }
        return buffer.toString();
    }

    private String checkValue(DirectoryReference dir) {
        StringBuffer buffer = new StringBuffer("[ ");

        if (dir == null || dir.getURL() == null) {
            buffer.append("]");
        } else {
            URI[] urlArray = dir.getURL();
            for (int i = 0; i < urlArray.length; i++) {
                buffer.append(urlArray[i].toString()).append(", ");
            }

            buffer.replace(buffer.length() - 2, buffer.length(), " ]");
        }

        return buffer.toString();
    }

    private String checkValue(String value) {
        if (value == null) {
            value = "N/A";
        }

        return value;
    }

    private String checkValue(URI uri) {
        if (uri == null) {
            return "N/A";
        }

        return uri.toString();
    }

    public void execute() {
        if (getIdList().size() == 0) {
            printUsage();
            return;
        }

        try {
            XMLInputFactory xmlif = XMLInputFactory.newInstance();
            CreateActivity createActivity = CreateActivity.Factory.parse(xmlif.createXMLStreamReader(new FileReader(getIdArray()[0])));

            ActivityCreationResponseSequence_type1 sequence = null;
            CreateActivityResponse response = getCreationServiceStub().createActivity(createActivity);
            ActivityCreationResponse_type0[] activityResponse = response.getActivityCreationResponse();

            for (int i = 0; i < activityResponse.length; i++) {
                if (activityResponse[i].isActivityCreationResponseSequence_type1Specified()) {
                    sequence = activityResponse[i].getActivityCreationResponseSequence_type1();
                    System.out.println("activityID = " + sequence.getActivityID());
                    System.out.println("activityStatus = " + checkValue(sequence.getActivityStatus()));
                    System.out.println("activityStatusAttributes = " + checkValue(sequence.getActivityStatus().getAttribute()));
                    System.out.println("activityStatusDescription = " + checkValue(sequence.getActivityStatus().getDescription()));
                    System.out.println("activityStageInDirectory = " + checkValue(sequence.getStageInDirectory()));
                    System.out.println("activityStageOutDirectory = " + checkValue(sequence.getStageOutDirectory()));
                    System.out.println("activityMgmtEndpointURL = " + checkValue(sequence.getActivityMgmtEndpointURL()));
                    System.out.println("resourceInfoEndpointURL = " + checkValue(sequence.getResourceInfoEndpointURL()));
                } else if (activityResponse[i].isAccessControlFaultSpecified()) {
                    System.out.println("AccessControlFault = " + activityResponse[i].getAccessControlFault().getMessage());
                } else if (activityResponse[i].isInternalBaseFaultSpecified()) {
                    System.out.println("InternalBaseFault = " + activityResponse[i].getInternalBaseFault().getMessage());
                } else if (activityResponse[i].isInvalidActivityDescriptionFaultSpecified()) {
                    System.out.println("InvalidActivityDescriptionFault = " + activityResponse[i].getInvalidActivityDescriptionFault().getMessage());
                } else if (activityResponse[i].isInvalidActivityDescriptionSemanticFaultSpecified()) {
                    System.out.println("InvalidActivityDescriptionSemanticFault = " + activityResponse[i].getInvalidActivityDescriptionSemanticFault().getMessage());
                } else if (activityResponse[i].isUnsupportedCapabilityFaultSpecified()) {
                    System.out.println("UnsupportedCapabilityFault = " + activityResponse[i].getUnsupportedCapabilityFault().getMessage());
                }
            }
        } catch (AxisFault e) {
            System.out.println(e.getMessage());
        } catch (RemoteException e) {
            System.out.println(e.getMessage());
        } catch (VectorLimitExceededFault e) {
            System.out.println(e.getFaultMessage().getMessage());
        } catch (InternalBaseFault e) {
            System.out.println(e.getFaultMessage().getInternalBaseFault().getMessage());
        } catch (AccessControlFault e) {
            System.out.println(e.getFaultMessage().getMessage());
        } catch (XMLStreamException e) {
            System.out.println("adl error: " + e.getMessage());
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
