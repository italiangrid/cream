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

import java.math.BigInteger;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.axis2.AxisFault;
import org.apache.axis2.databinding.types.NonNegativeInteger;
import org.apache.axis2.databinding.types.URI;
import org.apache.axis2.databinding.types.UnsignedLong;
import org.glite.ce.creamapi.ws.es.activityinfo.AccessControlFault;
import org.glite.ce.creamapi.ws.es.activityinfo.InternalBaseFault;
import org.glite.ce.creamapi.ws.es.activityinfo.InvalidParameterFault;
import org.glite.ce.creamapi.ws.es.activityinfo.UnknownAttributeFault;
import org.glite.ce.creamapi.ws.es.activityinfo.VectorLimitExceededFault;
import org.glite.ce.creamapi.ws.es.activityinfo.types.ActivityInfoDocument_t;
import org.glite.ce.creamapi.ws.es.activityinfo.types.ActivityInfoItemChoice_type1;
import org.glite.ce.creamapi.ws.es.activityinfo.types.ActivityInfoItem_type0;
import org.glite.ce.creamapi.ws.es.activityinfo.types.ActivityStatusAttribute;
import org.glite.ce.creamapi.ws.es.activityinfo.types.ActivityStatusItemChoice_type1;
import org.glite.ce.creamapi.ws.es.activityinfo.types.ActivityStatusItem_type0;
import org.glite.ce.creamapi.ws.es.activityinfo.types.ActivityStatusState;
import org.glite.ce.creamapi.ws.es.activityinfo.types.ActivityStatus_type0;
import org.glite.ce.creamapi.ws.es.activityinfo.types.ActivityStatus_type1;
import org.glite.ce.creamapi.ws.es.activityinfo.types.ComputingActivityHistory_type0;
import org.glite.ce.creamapi.ws.es.activityinfo.types.GetActivityInfo;
import org.glite.ce.creamapi.ws.es.activityinfo.types.GetActivityInfoResponse;
import org.glite.ce.creamapi.ws.es.activityinfo.types.GetActivityStatus;
import org.glite.ce.creamapi.ws.es.activityinfo.types.GetActivityStatusResponse;
import org.glite.ce.creamapi.ws.es.activityinfo.types.InternalBaseFault_Type;
import org.glite.ce.creamapi.ws.es.activityinfo.types.ListActivities;
import org.glite.ce.creamapi.ws.es.activityinfo.types.ListActivitiesResponse;
import org.glite.ce.creamapi.ws.es.activityinfo.types.Operation_type0;
import org.glite.ce.creamapi.ws.es.glue.ComputingActivityState_t;
import org.glite.ce.creamapi.ws.es.glue.ComputingActivityType_t;
import org.glite.ce.creamapi.ws.es.glue.JobDescription_t;

public class ActivityInfoClient extends ActivityCommand {

    public static void main(String[] args) {
        List<String> options = new ArrayList<String>(8);
        options.add(EPR);
        options.add(PROXY);
        options.add(GET_ACTIVITY_INFO);
        options.add(GET_ACTIVITY_STATUS);
        options.add(LIST_ACTIVITIES);

        new ActivityInfoClient(args, options);
    }

    public ActivityInfoClient(String[] args, List<String> options) throws RuntimeException {
        super(args, options);
    }


    private String checkAttribute(String name, BigInteger value) {
        if (value == null) {
            return "";
        }

        return "\n" + name + " = " + value.toString();
    }

    private String checkAttribute(String name, ComputingActivityState_t[] state) {
        if (state == null) {
            return "";
        }

        StringBuffer buffer = new StringBuffer("[ ");

        for (int i = 0; i < state.length; i++) {
            buffer.append(state[i].getComputingActivityState_t()).append(", ");
        }

        buffer.replace(buffer.length() - 2, buffer.length(), " ]");

        return "\n" + name + " = " + buffer.toString();
    }

    private String checkAttribute(String name, ComputingActivityType_t type) {
        if (type == null) {
            return "";
        }

        return "\n" + name + " = " + type.getValue();
    }

    private String checkAttribute(String name, Integer value) {
        if (value == null) {
            return "";
        }

        return "\n" + name + " = " + value.toString();
    }

    private String checkAttribute(String name, JobDescription_t jobDescription) {
        if (jobDescription == null) {
            return "";
        }

        return "\n" + name + " = " + jobDescription.getJobDescription_t();
    }

    private String checkAttribute(String name, List<String> value) {
        if (value == null) {
            return "";
        }

        return "\n" + name + " = " + value.toString();
    }

    private String checkAttribute(String name, String value) {
        if (value == null) {
            return "";
        }

        return "\n" + name + " = " + value;
    }

    private String checkAttribute(String name, String[] values) {
        if (values == null) {
            return "";
        }

        StringBuffer buffer = new StringBuffer("[ ");

        for (int i = 0; i < values.length; i++) {
            buffer.append(values[i]).append(", ");
        }

        buffer.replace(buffer.length() - 2, buffer.length(), " ]");

        return "\n" + name + " = " + buffer.toString();
    }

    private String checkAttribute(String name, URI[] values) {
        if (values == null) {
            return "";
        }

        StringBuffer buffer = new StringBuffer("[ ");

        for (int i = 0; i < values.length; i++) {
            buffer.append(values[i]).append(", ");
        }

        buffer.replace(buffer.length() - 2, buffer.length(), " ]");

        return "\n" + name + " = " + buffer.toString();
    }

    private String checkAttribute(String name, UnsignedLong value) {
        if (value == null) {
            return "";
        }

        return "\n" + name + " = " + value.toString();
    }

    private String checkAttribute(String name, URI uri) {
        if (uri == null) {
            return "";
        }

        return "\n" + name + " = " + uri.toString();
    }

    private String checkAttribute(String name, Calendar time) {
        if (time == null) {
            return "";
        }

        return "\n" + name + " = " + time.getTime();
    }

    public void execute() {
        if (isListActivities()) {
            ListActivities req = new ListActivities();
            req.setFromDate(getFromDate());
            req.setToDate(getToDate());

            if (getLimit() > 0) {
                req.setLimit(new NonNegativeInteger(""+getLimit()));
            }
            
            String status = null;
            String attribute = null;
            StringTokenizer st = null;
            ListActivitiesResponse response = null;
            
            try {
                ActivityStatus_type1 activityStatus = null;
                List<String> idList = getIdList();
                
                for (int i=0; i < idList.size(); i++) {                    
                    st = new StringTokenizer(idList.get(i), ":");
                    status = st.nextToken();
                    attribute = null;
                    
                    activityStatus = new ActivityStatus_type1();
                    activityStatus.setStatus(ActivityStatusState.Factory.fromValue(status));
                    
                    while (st.hasMoreTokens()) {
                        attribute = st.nextToken();                        
                        activityStatus.addAttribute(ActivityStatusAttribute.Factory.fromValue(attribute));
                    }

                    req.addActivityStatus(activityStatus);
                }
                
                response = getActivityInfoServiceStub().listActivities(req);
            } catch (AxisFault e) {
                System.out.println(e.getMessage());
                return;
            } catch (RemoteException e) {
                System.out.println("RemoteException: " + e.getMessage());
                return;
            } catch (AccessControlFault e) {
                System.out.println("AccessControlFault: " + e.getFaultMessage().getMessage());
                return;
            } catch (InternalBaseFault e) {
                System.out.println("InternalBaseFault:" + e.getFaultMessage().getInternalBaseFault().getMessage());
                return;
            } catch (InvalidParameterFault e) {
                System.out.println("InvalidParameterFault:" + e.getFaultMessage().getMessage());
                return;
            } catch (IllegalArgumentException e) {
                if (attribute == null) {
                    System.out.println("IllegalArgumentException: wrong status name found (" + status + "); please use: " + 
                        ActivityStatusState._value1 + ", " + ActivityStatusState._value2 + ", " + ActivityStatusState._value3 + ", " +
                        ActivityStatusState._value4 + ", " + ActivityStatusState._value5 + ", " + ActivityStatusState._value6 + ", " +
                        ActivityStatusState._value7 + ", " + ActivityStatusState._value8);
                } else {
                    System.out.println("IllegalArgumentException: wrong status attribute name found (" + attribute + "); please use: " + 
                            ActivityStatusAttribute._value1 + ", " + ActivityStatusAttribute._value2 + ", " + ActivityStatusAttribute._value3 + ", " +
                            ActivityStatusAttribute._value4 + ", " + ActivityStatusAttribute._value5 + ", " + ActivityStatusAttribute._value6 + ", " +
                            ActivityStatusAttribute._value7 + ", " + ActivityStatusAttribute._value8 + ", " + ActivityStatusAttribute._value9 + ", " +
                            ActivityStatusAttribute._value10 + ", " + ActivityStatusAttribute._value11 + ", " + ActivityStatusAttribute._value12 + ", " +
                            ActivityStatusAttribute._value13 + ", " + ActivityStatusAttribute._value14 + ", " + ActivityStatusAttribute._value15 + ", " +
                            ActivityStatusAttribute._value16 + ", " + ActivityStatusAttribute._value17 + ", " + ActivityStatusAttribute._value18 + ", " +
                            ActivityStatusAttribute._value19);                    
                }
                return;
            }
            
            if (response == null) {
                System.out.println("none response received!");
                return;
            }
            
            if (response.isActivityIDSpecified()) {
                String[] responseItems = response.getActivityID();

                for (int i = 0; i < responseItems.length; i++) {
                    System.out.println(i+1 + ") activityId = " + responseItems[i]);
                }

                System.out.println("\nthe result was " + (response.getTruncated()? "": "not ") + "truncated");  
            } else {
                System.out.println("none activity found");
            }          
        } else if (isGetActivityStatus()) {
            if (getIdList().size() == 0) {
                printUsage();
                return;
            }

            GetActivityStatus req = new GetActivityStatus();
            req.setActivityID(getIdArray());

            GetActivityStatusResponse response = null;
            try {
                response = getActivityInfoServiceStub().getActivityStatus(req);
            } catch (AxisFault e) {
                System.out.println(e.getMessage());
                return;
            } catch (RemoteException e) {
                System.out.println(e.getMessage());
                return;
            } catch (VectorLimitExceededFault e) {
                System.out.println(e.getFaultMessage().getMessage());
                return;
            } catch (InternalBaseFault e) {
                System.out.println(e.getFaultMessage().getInternalBaseFault().getMessage());
                return;
            } catch (AccessControlFault e) {
                System.out.println(e.getFaultMessage().getMessage());
                return;
            }

            if (response == null) {
                System.out.println("none response received!");
                return;
            }

            ActivityStatus_type0 status = null;
            ActivityStatusItemChoice_type1 choice = null;
            ActivityStatusAttribute[] statusAttributes = null;
            ActivityStatusItem_type0[] activityStatusItems = response.getActivityStatusItem();

            StringBuffer buffer = new StringBuffer();
            
            for (int i = 0; i < activityStatusItems.length; i++) {
                buffer.append("\n").append(i).append(") activityId = ");
                buffer.append(activityStatusItems[i].getActivityID());
                choice = activityStatusItems[i].getActivityStatusItemChoice_type1();

                if (choice.isActivityStatusSpecified()) {
                    status = choice.getActivityStatus();

                    buffer.append("\nstatus = ").append(status.getStatus().getValue());

                    if (status.isAttributeSpecified()) {
                        buffer.append("\nattributes = { ");

                        statusAttributes = status.getAttribute();

                        for (int j = 0; j < statusAttributes.length; j++) {
                            buffer.append(statusAttributes[j].getValue()).append(", ");
                        }

                        buffer.replace(buffer.length() - 2, buffer.length() - 1, " }");
                    }

                    if (status.getDescription() != null) {
                        buffer.append("\ndescription = \"" + status.getDescription()).append("\"");
                    }

                    buffer.append("\ntimestamp = " + status.getTimestamp().getTime());
                } else if (choice.isAccessControlFaultSpecified()) {
                    buffer.append("\nfault = AccessControlFault");
                    buffer.append(printFault(choice.getAccessControlFault()));
                } else if (choice.isActivityNotFoundFaultSpecified()) {
                    buffer.append("\nfault = ActivityNotFoundFault");
                    buffer.append(printFault(choice.getActivityNotFoundFault()));
                } else if (choice.isInternalBaseFaultSpecified()) {
                    buffer.append("\nfault = InternalBaseFault");
                    buffer.append(printFault(choice.getInternalBaseFault()));
                } else if (choice.isOperationNotAllowedFaultSpecified()) {
                    buffer.append("\nfault = OperationNotAllowedFault");
                    buffer.append(printFault(choice.getOperationNotAllowedFault()));
                } else if (choice.isOperationNotPossibleFaultSpecified()) {
                    buffer.append("\nfault = OperationNotPossibleFault");
                    buffer.append(printFault(choice.getOperationNotPossibleFault()));
                } else if (choice.isUnableToRetrieveStatusFaultSpecified()) {
                    buffer.append("\nfault = UnableToRetrieveStatusFault");
                    buffer.append(printFault(choice.getUnableToRetrieveStatusFault()));
                }
                
                buffer.append("\n");
            }

            System.out.println(buffer.toString()); 
        } else if (isGetActivityInfo()) {
            if (getIdList().size() == 0) {
                printUsage();
                return;
            }

            GetActivityInfo req = new GetActivityInfo();
            req.setActivityID(getIdArray());

            GetActivityInfoResponse response = null;
            try {
                response = getActivityInfoServiceStub().getActivityInfo(req);
            } catch (AxisFault e) {
                System.out.println("AxisFault: " + e.getMessage());
                return;
            } catch (RemoteException e) {
                System.out.println("RemoteException: " + e.getMessage());
                return;
            } catch (AccessControlFault e) {
                System.out.println("AccessControlFault: " + e.getFaultMessage().getMessage());
                return;
            } catch (InternalBaseFault e) {
                System.out.println("InternalBaseFault:" + e.getFaultMessage().getInternalBaseFault().getMessage());
                return;
            } catch (UnknownAttributeFault e) {
                System.out.println("UnknownAttributeFault: " + e.getFaultMessage().getUnknownAttributeFault().getMessage());
                return;
            } catch (VectorLimitExceededFault e) {
                System.out.println("VectorLimitExceededFault " + e.getFaultMessage().getMessage());
                return;
            }

            if (response == null) {
                System.out.println("none response received!");
                return;
            }

            ActivityInfoDocument_t activityInfoDocument = null;
            //Extensions_t extensions = null;
            ActivityInfoItemChoice_type1 choice = null;
            ActivityInfoItem_type0[] activityInfoItems = response.getActivityInfoItem();

            StringBuffer buffer = new StringBuffer();
            
            for (int i = 0; i < activityInfoItems.length; i++) {
                buffer.append("\n").append(i).append(") activityId = ");
                buffer.append(activityInfoItems[i].getActivityID());

                choice = activityInfoItems[i].getActivityInfoItemChoice_type1();

                if (choice.isActivityInfoDocumentSpecified()) {
                    activityInfoDocument = choice.getActivityInfoDocument();

                    buffer.append(checkAttribute("id", activityInfoDocument.getID()));
                    buffer.append(checkAttribute("idFromEndpoint", activityInfoDocument.getIDFromEndpoint()));
                    buffer.append(checkAttribute("localIDFromManager", activityInfoDocument.getLocalIDFromManager()));
                    //buffer.append(checkAttribute("baseType", activityInfoDocument.getType()));
                    if (activityInfoDocument.isTypeSpecified()) {
                        buffer.append(checkAttribute("type", activityInfoDocument.getType().toString()));
                    }
                    buffer.append(checkAttribute("name", activityInfoDocument.getName()));
                    buffer.append(checkAttribute("description", activityInfoDocument.getJobDescription()));
                    buffer.append(checkAttribute("computingManagerExitCode", activityInfoDocument.getComputingManagerExitCode()));
                    buffer.append(checkAttribute("localOwner", activityInfoDocument.getLocalOwner()));
                    buffer.append(checkAttribute("owner", activityInfoDocument.getOwner()));
                    buffer.append(checkAttribute("userDomain", activityInfoDocument.getUserDomain()));
                    buffer.append(checkAttribute("logDir", activityInfoDocument.getLogDir()));
                    buffer.append(checkAttribute("queue", activityInfoDocument.getQueue()));
                    buffer.append(checkAttribute("stdIn", activityInfoDocument.getStdIn()));
                    buffer.append(checkAttribute("stdOut", activityInfoDocument.getStdOut()));
                    buffer.append(checkAttribute("stdErr", activityInfoDocument.getStdErr()));
                    buffer.append(checkAttribute("sessionDirectory", activityInfoDocument.getSessionDirectory()));
                    buffer.append(checkAttribute("stageInDirectory", activityInfoDocument.getStageInDirectory()));
                    buffer.append(checkAttribute("stageOutDirectory", activityInfoDocument.getStageOutDirectory()));
                    buffer.append(checkAttribute("submissionClientName", activityInfoDocument.getSubmissionClientName()));
                    buffer.append(checkAttribute("submissionHost", activityInfoDocument.getSubmissionHost()));
                    if (activityInfoDocument.isExitCodeSpecified()) {
                        buffer.append(checkAttribute("exitCode", activityInfoDocument.getExitCode()));
                    }
                    buffer.append(checkAttribute("creationTime", activityInfoDocument.getCreationTime()));
                    buffer.append(checkAttribute("startTime", activityInfoDocument.getStartTime()));
                    buffer.append(checkAttribute("submissionTime ", activityInfoDocument.getSubmissionTime()));
                    buffer.append(checkAttribute("computingManagerSubmissionTime", activityInfoDocument.getComputingManagerSubmissionTime()));
                    buffer.append(checkAttribute("computingManagerEndTime", activityInfoDocument.getComputingManagerEndTime()));
                    buffer.append(checkAttribute("endTime", activityInfoDocument.getEndTime()));
                    buffer.append(checkAttribute("workingAreaEraseTime", activityInfoDocument.getWorkingAreaEraseTime()));
                    buffer.append(checkAttribute("proxyExpirationTime", activityInfoDocument.getProxyExpirationTime()));
                    buffer.append(checkAttribute("state", activityInfoDocument.getState()));
                    buffer.append(checkAttribute("restartState", activityInfoDocument.getRestartState()));
                    buffer.append(checkAttribute("error", activityInfoDocument.getError()));
                    buffer.append(checkAttribute("otherInfo", activityInfoDocument.getOtherInfo()));
                    buffer.append(checkAttribute("otherMessages", activityInfoDocument.getOtherMessages()));
                    buffer.append(checkAttribute("executionNode", activityInfoDocument.getExecutionNode()));
                    buffer.append(checkAttribute("requestedApplicationEnvironment", activityInfoDocument.getRequestedApplicationEnvironment()));
                    if (activityInfoDocument.isRequestedSlotsSpecified()) {
                        buffer.append("\nrequestedSlots = ").append(activityInfoDocument.getRequestedSlots());
                    }
                    buffer.append(checkAttribute("requestedTotalCPUTime", activityInfoDocument.getRequestedTotalCPUTime()));
                    buffer.append(checkAttribute("requestedTotalWallTime", activityInfoDocument.getRequestedTotalWallTime()));
                    buffer.append(checkAttribute("usedMainMemory", activityInfoDocument.getUsedMainMemory()));
                    buffer.append(checkAttribute("usedTotalCPUTime", activityInfoDocument.getUsedTotalCPUTime()));
                    buffer.append(checkAttribute("usedTotalWallTime", activityInfoDocument.getUsedTotalWallTime()));
                    buffer.append(checkAttribute("validity", activityInfoDocument.getValidity()));
                    if (activityInfoDocument.isWaitingPositionSpecified()) {
                        buffer.append("\nwaitingPosition = ").append(activityInfoDocument.getWaitingPosition());
                    }

                    ComputingActivityHistory_type0 history = activityInfoDocument.getComputingActivityHistory();
                    if (history != null) {
                        if (history.getActivityStatus() != null) {
                            ActivityStatus_type0[] activityStatusArray = history.getActivityStatus();
                            for (int x = 0; x < activityStatusArray.length; x++) {
                                buffer.append("\n[ status = ").append(activityStatusArray[x].getStatus().getValue());
                                buffer.append("; timestamp = ").append(activityStatusArray[x].getTimestamp().getTime());

                                if (activityStatusArray[x].isDescriptionSpecified()) {
                                    buffer.append("; description = \"").append(activityStatusArray[x].getDescription()).append("\"");
                                }

                                ActivityStatusAttribute[] attributes = activityStatusArray[x].getAttribute();
                                if (attributes != null && attributes.length > 0) {
                                    buffer.append("; attributes = { ");

                                    for (int y = 0; y < attributes.length; y++) {
                                        buffer.append(attributes[y].getValue()).append(", ");
                                    }

                                    buffer.replace(buffer.length() - 2, buffer.length(), " }");
                                }

                                buffer.append(" ]");
                            }
                        }

                        if (history.isOperationSpecified()) {
                            Operation_type0[] operationArray = history.getOperation();
                            for (int x = 0; x < operationArray.length; x++) {
                                buffer.append("\n[ operation = ").append(operationArray[x].getRequestedOperation().toString());
                                buffer.append("; timestamp = ").append(operationArray[x].getTimestamp().getTime());
                                buffer.append("; success = ").append(operationArray[x].getSuccess());
                                buffer.append(" ]");
                            }
                        }
                    }

                    /*
                     * extensions = activityInfoDocument.getExtensions();
                     * 
                     * if (extensions != null) {
                     * buffer.append("\nextensions = {");
                     * 
                     * for (Extension_t extension : extensions.getExtension()) {
                     * buffer.append("\n\t[").append(checkAttribute("localID",
                     * extension.getLocalID().getLocalID_t()));
                     * buffer.append(checkAttribute("key", extension.getKey()));
                     * buffer.append(checkAttribute("value",
                     * extension.getValue())).append("]");
                     * buffer.append("; any = "
                     * ).append(extension.getExtraElement()); }
                     * 
                     * buffer.append("\n}"); }
                     */
                } else if (choice.isAccessControlFaultSpecified()) {
                    buffer.append("\nfault = AccessControlFault");
                    buffer.append(printFault(choice.getAccessControlFault()));
                } else if (choice.isActivityNotFoundFaultSpecified()) {
                    buffer.append("\nfault = ActivityNotFoundFault");
                    buffer.append(printFault(choice.getActivityNotFoundFault()));
                } else if (choice.isInternalBaseFaultSpecified()) {
                    buffer.append("\nfault = InternalBaseFault");
                    buffer.append(printFault(choice.getInternalBaseFault()));
                } else if (choice.isOperationNotAllowedFaultSpecified()) {
                    buffer.append("\nfault = OperationNotAllowedFault");
                    buffer.append(printFault(choice.getOperationNotAllowedFault()));
                } else if (choice.isOperationNotPossibleFaultSpecified()) {
                    buffer.append("\nfault = OperationNotPossibleFault");
                    buffer.append(printFault(choice.getOperationNotPossibleFault()));
                } else if (choice.isUnableToRetrieveStatusFaultSpecified()) {
                    buffer.append("\nfault = UnableToRetrieveStatusFault");
                    buffer.append(printFault(choice.getUnableToRetrieveStatusFault()));
                } else if (choice.isUnknownAttributeFaultSpecified()) {
                    buffer.append("\nfault = UnknownAttributeFault");
                    buffer.append(printFault(choice.getUnknownAttributeFault()));
                }

                buffer.append("\n");
            }
            
            System.out.println(buffer.toString());
        } else {
            printUsage();
        }
    }

    private String printFault(InternalBaseFault_Type fault) {
        StringBuffer buffer = new StringBuffer();
        
        if (fault != null) {
            buffer.append("\nmessage = \"").append(fault.getMessage()).append("\"");
            buffer.append("\ntimestamp = " + fault.getTimestamp().getTime());

            if (fault.getDescription() != null) {
                buffer.append("\ndescription = \"" + fault.getDescription()).append("\"");
            }
        }
        
        return buffer.toString();
    }
}
