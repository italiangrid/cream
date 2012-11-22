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

import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.parsers.FactoryConfigurationError;

import org.apache.axis2.AxisFault;
import org.apache.axis2.databinding.types.URI;
import org.apache.axis2.databinding.types.UnsignedLong;
import org.glite.ce.creamapi.ws.es.activitymanagement.AccessControlFault;
import org.glite.ce.creamapi.ws.es.activitymanagement.InternalBaseFault;
import org.glite.ce.creamapi.ws.es.activitymanagement.InternalNotificationFault;
import org.glite.ce.creamapi.ws.es.activitymanagement.UnknownAttributeFault;
import org.glite.ce.creamapi.ws.es.activitymanagement.VectorLimitExceededFault;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.ActivityInfoDocument_t;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.ActivityInfoItemChoice_type1;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.ActivityInfoItem_type0;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.ActivityStatusAttribute;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.ActivityStatusItemChoice_type1;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.ActivityStatusItem_type0;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.ActivityStatus_type0;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.AttributeInfoItem_type0;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.CancelActivity;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.CancelActivityResponse;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.CancelActivityResponseItemChoice_type1;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.CancelActivityResponseItem_type0;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.ComputingActivityHistory_type0;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.GetActivityInfo;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.GetActivityInfoResponse;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.GetActivityStatus;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.GetActivityStatusResponse;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.InternalBaseFault_Type;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.NotifyMessageType;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.NotifyRequestItem_type0;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.NotifyResponseItemChoice_type0;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.NotifyResponseItem_type0;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.NotifyService;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.NotifyServiceResponse;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.Operation_type0;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.PauseActivity;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.PauseActivityResponse;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.PauseActivityResponseItemChoice_type1;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.PauseActivityResponseItem_type0;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.ResumeActivity;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.ResumeActivityResponse;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.ResumeActivityResponseItemChoice_type1;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.ResumeActivityResponseItem_type0;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.WipeActivity;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.WipeActivityResponse;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.WipeActivityResponseItemChoice_type1;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.WipeActivityResponseItem_type0;
import org.glite.ce.creamapi.ws.es.glue.ComputingActivityState_t;
import org.glite.ce.creamapi.ws.es.glue.ComputingActivityType_t;
import org.glite.ce.creamapi.ws.es.glue.JobDescription_t;

public class ActivityManagementClient extends ActivityCommand {

    public static void main(String[] args) {
        List<String> options = new ArrayList<String>(9);
        options.add(EPR);
        options.add(PROXY);
        options.add(GET_ACTIVITY_INFO);
        options.add(GET_ACTIVITY_STATUS);
        options.add(CANCEL_ACTIVITY);
        options.add(PAUSE_ACTIVITY);
        options.add(RESUME_ACTIVITY);
        options.add(WIPE_ACTIVITY);
        options.add(NOTIFY_SERVICE);

        new ActivityManagementClient(args, options);
    }

    public ActivityManagementClient(String[] args, List<String> options) throws RuntimeException {
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
        if (isNotifyService()) {
            String messageType = getNotifyMessageType();

            if (getIdList().size() == 0 || messageType == null) {
                printUsage();
                return;
            }

            NotifyMessageType notifyMessageType = null;
            if (messageType.equals("dataPullDone")) {
                notifyMessageType = NotifyMessageType.value1;
            } else if (messageType.equals("dataPushDone")) {
                notifyMessageType = NotifyMessageType.value2;
            } else {
                printUsage();
                return;
            }

            NotifyService req = new NotifyService();
            NotifyRequestItem_type0[] notifyRequestItem = new NotifyRequestItem_type0[getIdList().size()];
            int index = 0;

            for (String activityId : getIdList()) {
                System.out.println("notifying the message " + messageType + " for the activity " + activityId);

                notifyRequestItem[index] = new NotifyRequestItem_type0();
                notifyRequestItem[index].setActivityID(activityId);
                notifyRequestItem[index++].setNotifyMessage(notifyMessageType);
            }

            req.setNotifyRequestItem(notifyRequestItem);

            NotifyServiceResponse response = null;
            try {
                response = getActivityManagementServiceStub().notifyService(req);
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
            } catch (InternalNotificationFault e) {
                System.out.println("InternalNotificationFault " + e.getFaultMessage().getMessage());
                return;
            } catch (VectorLimitExceededFault e) {
                System.out.println("VectorLimitExceededFault " + e.getFaultMessage().getMessage());
                return;
            }

            if (response == null) {
                System.out.println("none response received!");
                return;
            }

            printResultItems(response.getNotifyResponseItem());
        } else if (isCancelActivity()) {
            if (getIdList().size() == 0) {
                printUsage();
                return;
            }

            CancelActivity req = new CancelActivity();
            req.setActivityID(getIdArray());

            CancelActivityResponse response = null;
            try {
                response = getActivityManagementServiceStub().cancelActivity(req);
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
            } catch (VectorLimitExceededFault e) {
                System.out.println("VectorLimitExceededFault " + e.getFaultMessage().getMessage());
                return;
            }

            printResultItems(response.getCancelActivityResponseItem());
        } else if (isPauseActivity()) {
            if (getIdList().size() == 0) {
                printUsage();
                return;
            }

            PauseActivity req = new PauseActivity();
            req.setActivityID(getIdArray());

            PauseActivityResponse response = null;
            try {
                response = getActivityManagementServiceStub().pauseActivity(req);
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
            } catch (VectorLimitExceededFault e) {
                System.out.println("VectorLimitExceededFault " + e.getFaultMessage().getMessage());
                return;
            }

            printResultItems(response.getPauseActivityResponseItem());
        } else if (isResumeActivity()) {
            if (getIdList().size() == 0) {
                printUsage();
                return;
            }

            ResumeActivity req = new ResumeActivity();
            req.setActivityID(getIdArray());

            ResumeActivityResponse response = null;
            try {
                response = getActivityManagementServiceStub().resumeActivity(req);
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
            } catch (VectorLimitExceededFault e) {
                System.out.println("VectorLimitExceededFault " + e.getFaultMessage().getMessage());
                return;
            }

            printResultItems(response.getResumeActivityResponseItem());
        } else if (isWipeActivity()) {
            if (getIdList().size() == 0) {
                printUsage();
                return;
            }

            WipeActivity req = new WipeActivity();
            req.setActivityID(getIdArray());

            WipeActivityResponse response = null;
            try {
                response = getActivityManagementServiceStub().wipeActivity(req);
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
            } catch (VectorLimitExceededFault e) {
                System.out.println("VectorLimitExceededFault " + e.getFaultMessage().getMessage());
                return;
            }

            printResultItems(response.getWipeActivityResponseItem());
        } else if (isGetActivityStatus()) {
            if (getIdList().size() == 0) {
                printUsage();
                return;
            }

            GetActivityStatus req = new GetActivityStatus();
            req.setActivityID(getIdArray());

            GetActivityStatusResponse response = null;
            try {
                response = getActivityManagementServiceStub().getActivityStatus(req);
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
            } catch (VectorLimitExceededFault e) {
                System.out.println("VectorLimitExceededFault " + e.getFaultMessage().getMessage());
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
            
            for (String attribute : getAttributeList()) {
                req.addAttributeName(new QName(attribute));
            }

            GetActivityInfoResponse response = null;
            try {
                response = getActivityManagementServiceStub().getActivityInfo(req);
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

                if (choice.isAttributeInfoItemSpecified()) {
                    AttributeInfoItem_type0[] attributeInfoItem = choice.getAttributeInfoItem();
                    
                    for (int x=0; x<attributeInfoItem.length; x++) {
                        buffer.append("\n").append(attributeInfoItem[x].getAttributeName().getLocalPart()).append(" = " ).append(attributeInfoItem[x].getAttributeValue());
                    }                    
                } else if (choice.isActivityInfoDocumentSpecified()) {
                    activityInfoDocument = choice.getActivityInfoDocument();

                    buffer.append(checkAttribute("id", activityInfoDocument.getID()));
                    buffer.append(checkAttribute("idFromEndpoint", activityInfoDocument.getIDFromEndpoint()));
                    buffer.append(checkAttribute("localIDFromManager", activityInfoDocument.getLocalIDFromManager()));
                    buffer.append(checkAttribute("baseType", activityInfoDocument.getType()));
                    buffer.append(checkAttribute("type", activityInfoDocument.getType().toString()));
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

    private void printResultItems(CancelActivityResponseItem_type0[] responseItems) {
        if (responseItems == null) {
            System.out.println("none result received");
        }
        CancelActivityResponseItemChoice_type1 choice = null;
        
        for (int i = 0; i < responseItems.length; i++) {
            System.out.println("activityId = " + responseItems[i].getActivityID());
            choice = responseItems[i].getCancelActivityResponseItemChoice_type1();

            if (choice == null) {
                System.out.println("SUCCESS");
            } else if (choice.isEstimatedTimeSpecified()) {
                Calendar esTime = Calendar.getInstance();
                esTime.setTimeInMillis(choice.getEstimatedTime().longValue());
                System.out.println("SUCCESS\nestimated time = " + esTime.getTime());
            } else if (choice.isAccessControlFaultSpecified()) {
                System.out.println("fault = AccessControlFault" + printFault(choice.getAccessControlFault()));
            } else if (choice.isActivityNotFoundFaultSpecified()) {
                System.out.println("fault = ActivityNotFoundFault" + printFault(choice.getActivityNotFoundFault()));
            } else if (choice.isInternalBaseFaultSpecified()) {
                System.out.println("fault = InternalBaseFault" + printFault(choice.getInternalBaseFault()));
            } else if (choice.isOperationNotAllowedFaultSpecified()) {
                System.out.println("fault = OperationNotAllowedFault" + printFault(choice.getOperationNotAllowedFault()));
            } else if (choice.isOperationNotPossibleFaultSpecified()) {
                System.out.println("fault = OperationNotPossibleFault" + printFault(choice.getOperationNotPossibleFault()));
            }
        }
    }

    private void printResultItems(NotifyResponseItem_type0[] responseItems) {
        if (responseItems == null) {
            System.out.println("none result received");
        }

        for (int i = 0; i < responseItems.length; i++) {
            System.out.println("activityId = " + responseItems[i].getActivityID());
            NotifyResponseItemChoice_type0 choice = responseItems[i].getNotifyResponseItemChoice_type0();

            if (choice == null) {
                System.out.println("SUCCESS");
            } else if (choice.isAccessControlFaultSpecified()) {
                System.out.println("fault = AccessControlFault" + printFault(choice.getAccessControlFault()));
            } else if (choice.isActivityNotFoundFaultSpecified()) {
                System.out.println("fault = ActivityNotFoundFault" + printFault(choice.getActivityNotFoundFault()));
            } else if (choice.isInternalBaseFaultSpecified()) {
                System.out.println("fault = InternalBaseFault" + printFault(choice.getInternalBaseFault()));
            } else if (choice.isOperationNotAllowedFaultSpecified()) {
                System.out.println("fault = OperationNotAllowedFault" + printFault(choice.getOperationNotAllowedFault()));
            } else if (choice.isOperationNotPossibleFaultSpecified()) {
                System.out.println("fault = OperationNotPossibleFault" + printFault(choice.getOperationNotPossibleFault()));
            } else if (choice.isAcknowledgementSpecified()) {
                System.out.println("SUCCESS\nack = " + choice.getAcknowledgement());
            }
        }
    }
    
    private void printResultItems(PauseActivityResponseItem_type0[] responseItems) {
        if (responseItems == null) {
            System.out.println("none result received");
        }
        
        PauseActivityResponseItemChoice_type1 choice = null;
        
        for (int i = 0; i < responseItems.length; i++) {
            System.out.println("activityId = " + responseItems[i].getActivityID());
            choice = responseItems[i].getPauseActivityResponseItemChoice_type1();

            if (choice == null) {
                System.out.println("SUCCESS");
            } else if (choice.isEstimatedTimeSpecified()) {
                Calendar esTime = Calendar.getInstance();
                esTime.setTimeInMillis(choice.getEstimatedTime().longValue());
                System.out.println("SUCCESS\nestimated time = " + esTime.getTime());
            } else if (choice.isAccessControlFaultSpecified()) {
                System.out.println("fault = AccessControlFault" + printFault(choice.getAccessControlFault()));
            } else if (choice.isActivityNotFoundFaultSpecified()) {
                System.out.println("fault = ActivityNotFoundFault" + printFault(choice.getActivityNotFoundFault()));
            } else if (choice.isInternalBaseFaultSpecified()) {
                System.out.println("fault = InternalBaseFault" + printFault(choice.getInternalBaseFault()));
            } else if (choice.isOperationNotAllowedFaultSpecified()) {
                System.out.println("fault = OperationNotAllowedFault" + printFault(choice.getOperationNotAllowedFault()));
            } else if (choice.isOperationNotPossibleFaultSpecified()) {
                System.out.println("fault = OperationNotPossibleFault" + printFault(choice.getOperationNotPossibleFault()));
            }
        }
    }

    private void printResultItems(ResumeActivityResponseItem_type0[] responseItems) {
        if (responseItems == null) {
            System.out.println("none result received");
        }

        ResumeActivityResponseItemChoice_type1 choice = null; 
        
        for (int i = 0; i < responseItems.length; i++) {
            System.out.println("activityId = " + responseItems[i].getActivityID());
            choice = responseItems[i].getResumeActivityResponseItemChoice_type1();

            if (choice == null) {
                System.out.println("SUCCESS");
            } else if (choice.isEstimatedTimeSpecified()) {
                Calendar esTime = Calendar.getInstance();
                esTime.setTimeInMillis(choice.getEstimatedTime().longValue());
                System.out.println("SUCCESS\nestimated time = " + esTime.getTime());
            } else if (choice.isAccessControlFaultSpecified()) {
                System.out.println("fault = AccessControlFault" + printFault(choice.getAccessControlFault()));
            } else if (choice.isActivityNotFoundFaultSpecified()) {
                System.out.println("fault = ActivityNotFoundFault" + printFault(choice.getActivityNotFoundFault()));
            } else if (choice.isInternalBaseFaultSpecified()) {
                System.out.println("fault = InternalBaseFault" + printFault(choice.getInternalBaseFault()));
            } else if (choice.isOperationNotAllowedFaultSpecified()) {
                System.out.println("fault = OperationNotAllowedFault" + printFault(choice.getOperationNotAllowedFault()));
            } else if (choice.isOperationNotPossibleFaultSpecified()) {
                System.out.println("fault = OperationNotPossibleFault" + printFault(choice.getOperationNotPossibleFault()));
            }
        }
    }

    private void printResultItems(WipeActivityResponseItem_type0[] responseItems) {
        if (responseItems == null) {
            System.out.println("none result received");
        }
        
        WipeActivityResponseItemChoice_type1 choice = null;
        
        for (int i = 0; i < responseItems.length; i++) {
            System.out.println("activityId = " + responseItems[i].getActivityID());
            choice = responseItems[i].getWipeActivityResponseItemChoice_type1();

            if (choice == null) {
                System.out.println("SUCCESS");
            } else if (choice.isEstimatedTimeSpecified()) {
                Calendar esTime = Calendar.getInstance();
                esTime.setTimeInMillis(choice.getEstimatedTime().longValue());
                System.out.println("SUCCESS\nestimated time = " + esTime.getTime());
            } else if (choice.isAccessControlFaultSpecified()) {
                System.out.println("fault = AccessControlFault" + printFault(choice.getAccessControlFault()));
            } else if (choice.isActivityNotFoundFaultSpecified()) {
                System.out.println("fault = ActivityNotFoundFault" + printFault(choice.getActivityNotFoundFault()));
            } else if (choice.isInternalBaseFaultSpecified()) {
                System.out.println("fault = InternalBaseFault" + printFault(choice.getInternalBaseFault()));
            } else if (choice.isOperationNotAllowedFaultSpecified()) {
                System.out.println("fault = OperationNotAllowedFault" + printFault(choice.getOperationNotAllowedFault()));
            } else if (choice.isOperationNotPossibleFaultSpecified()) {
                System.out.println("fault = OperationNotPossibleFault" + printFault(choice.getOperationNotPossibleFault()));
            }
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
