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

import org.apache.axis2.AxisFault;
import org.apache.axis2.databinding.types.URI;
import org.apache.axis2.databinding.types.UnsignedLong;
import org.glite.ce.creamapi.ws.es.activitymanagement.AccessControlFault;
import org.glite.ce.creamapi.ws.es.activitymanagement.InternalBaseFault;
import org.glite.ce.creamapi.ws.es.activitymanagement.UnknownGlue2ActivityAttributeFault;
import org.glite.ce.creamapi.ws.es.activitymanagement.VectorLimitExceededFault;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.ActivityInfoItemChoice_type1;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.ActivityInfoItem_type0;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.ActivityStatusAttribute;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.ActivityStatusItemChoice_type1;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.ActivityStatusItem_type0;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.ActivityStatus_type0;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.CancelActivity;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.CancelActivityResponse;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.GetActivityInfo;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.GetActivityInfoResponse;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.GetActivityStatus;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.GetActivityStatusResponse;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.InternalBaseFault_Type;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.NotifyMessageType;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.NotifyRequestItem_type0;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.NotifyResponseItem_type0;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.NotifyService;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.NotifyServiceResponse;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.PauseActivity;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.PauseActivityResponse;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.ResponseItemChoice_type0;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.ResponseItem_type0;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.ResumeActivity;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.ResumeActivityResponse;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.WipeActivity;
import org.glite.ce.creamapi.ws.es.activitymanagement.types.WipeActivityResponse;
import org.glite.ce.creamapi.ws.es.glue.ComputingActivityState_t;
import org.glite.ce.creamapi.ws.es.glue.ComputingActivityType_t;
import org.glite.ce.creamapi.ws.es.glue.ComputingActivity_t;
import org.glite.ce.creamapi.ws.es.glue.DateTime_t;
import org.glite.ce.creamapi.ws.es.glue.Extension_t;
import org.glite.ce.creamapi.ws.es.glue.Extensions_t;
import org.glite.ce.creamapi.ws.es.glue.JobDescription_t;

public class ActivityManagementClient extends ActivityCommand {

    public static void main(String[] args) {
        List<String> options = new ArrayList<String>(8);
        options.add(EPR);
        options.add(PROXY);
        options.add(GET_ACTIVITY_INFO);
        options.add(GET_ACTIVITY_STATUS);
        options.add(CANCEL_ACTIVITY);
        options.add(PAUSE_ACTIVITY);
        options.add(RESUME_ACTIVITY);
        options.add(WIPE_ACTIVITY);
        options.add(NOTIFY_SERVICE);
        options.add(NOTIFY_MESSAGE_TYPE);

        new ActivityManagementClient(args, options);
    }

    public ActivityManagementClient(String[] args, List<String> options) throws RuntimeException {
        super(args, options);
    }

    private String checkValue(BigInteger value) {
        if (value == null) {
            return "N/A";
        }

        return value.toString();
    }

    private String checkValue(ComputingActivityState_t[] state) {
        StringBuffer buffer = new StringBuffer("[ ");

        if (state == null) {
            buffer.append("]");
        } else {
            for (int i = 0; i < state.length; i++) {
                buffer.append(state[i].getComputingActivityState_t()).append(", ");
            }

            buffer.replace(buffer.length() - 2, buffer.length(), " ]");
        }
        return buffer.toString();
    }

    private String checkValue(ComputingActivityType_t type) {
        if (type == null) {
            return "N/A";
        }

        return type.getValue();
    }

    private String checkValue(DateTime_t time) {
        if (time == null) {
            return "N/A";
        }

        return time.getDateTime_t().getTime().toString();
    }

    private String checkValue(Integer value) {
        if (value == null) {
            return "N/A";
        }

        return value.toString();
    }

    private String checkValue(JobDescription_t jobDescription) {
        if (jobDescription == null) {
            return "N/A";
        }

        return jobDescription.getJobDescription_t();
    }

    private String checkValue(List<String> value) {
        if (value == null) {
            return "N/A";
        }

        return value.toString();
    }

    private String checkValue(String value) {
        if (value == null) {
            value = "N/A";
        }

        return value;
    }

    private String checkValue(String[] values) {
        StringBuffer buffer = new StringBuffer("[ ");

        if (values == null) {
            buffer.append("]");
        } else {
            for (int i = 0; i < values.length; i++) {
                buffer.append(values[i]).append(", ");
            }

            buffer.replace(buffer.length() - 2, buffer.length(), " ]");
        }
        return buffer.toString();
    }

    private String checkValue(UnsignedLong value) {
        if (value == null) {
            return "N/A";
        }

        return value.toString();
    }

    private String checkValue(URI uri) {
        if (uri == null) {
            return "N/A";
        }

        return uri.toString();
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
            
            NotifyResponseItem_type0[] responseItems = response.getNotifyResponseItem();

            for (int i = 0; i < responseItems.length; i++) {
                System.out.println("activityId = " + responseItems[i].getActivityID());

                if (responseItems[i].getInternalBaseFault() == null) {
                    System.out.println("SUCCESS");
                } else {
                    System.out.println("ERROR: " + responseItems[i].getInternalBaseFault().getMessage());
                }
            }
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

            printResultItems(response.getResponseItem());
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

            printResultItems(response.getResponseItem());
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

            printResultItems(response.getResponseItem());
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

            printResultItems(response.getResponseItem());
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

            ActivityStatus_type0 status = null;
            ActivityStatusItemChoice_type1 choice = null;
            ActivityStatusAttribute[] statusAttributes = null;
            ActivityStatusItem_type0[] activityStatusItems = response.getActivityStatusItem();

            for (int i = 0; i < activityStatusItems.length; i++) {
                StringBuffer buffer = new StringBuffer("activityId = ");
                buffer.append(activityStatusItems[i].getActivityID());
                choice = activityStatusItems[i].getActivityStatusItemChoice_type1();

                if (choice.isActivityStatusSpecified()) {
                    status = choice.getActivityStatus();

                    buffer.append("\nstatus = ").append(status.getStatus().getValue());

                    if (status.isAttributeSpecified()) {
                        buffer.append("\nattributes = {");

                        statusAttributes = status.getAttribute();

                        for (int j = 0; j < statusAttributes.length; j++) {
                            buffer.append(statusAttributes[j].getValue()).append(", ");
                        }

                        buffer.replace(buffer.length() - 2, buffer.length() - 1, "}");
                    }

                    if (status.getDescription() != null) {
                        buffer.append("\ndescription = " + status.getDescription());
                    }

                    buffer.append("\ntimestamp = " + status.getTimestamp().getTime());
                } else {
                    InternalBaseFault_Type fault = choice.getInternalBaseFault();

                    if (fault != null) {
                        buffer.append("\nfault = ").append(fault.getClass().getName());
                        buffer.append("\nmessage = ").append(fault.getMessage());
                        buffer.append("\ntimestamp = " + fault.getTimestamp().getTime());

                        if (fault.getDescription() != null) {
                            buffer.append("\ndescription = " + fault.getDescription());
                        }
                    }
                }

                System.out.println(buffer.toString());
            }
        } else if (isGetActivityInfo()) {
            if (getIdList().size() == 0) {
                printUsage();
                return;
            }

            GetActivityInfo req = new GetActivityInfo();
            req.setActivityID(getIdArray());

            GetActivityInfoResponse response = null;
            try {
                response = getActivityManagementServiceStub().getActivityInfo(req);
            } catch (AxisFault e) {
                System.out.println("AxisFault: " + e.getMessage());
                return;
            } catch (RemoteException e) {
                System.out.println("RemoteException: " + e.getMessage());
                return;
            } catch (VectorLimitExceededFault e) {
                System.out.println("VectorLimitExceededFault " + e.getFaultMessage().getMessage());
                return;
            } catch (InternalBaseFault e) {
                System.out.println("InternalBaseFault:" + e.getFaultMessage().getInternalBaseFault().getMessage());
                return;
            } catch (AccessControlFault e) {
                System.out.println("AccessControlFault: " + e.getFaultMessage().getMessage());
                return;
            } catch (UnknownGlue2ActivityAttributeFault e) {
                System.out.println("UnknownGlue2ActivityAttributeFault: " + e.getFaultMessage().getMessage());
                return;
            }

            if (response == null) {
            	System.out.println("none response received!");
            	return;
            }

            ComputingActivity_t computingActivity = null;
            Extensions_t extensions = null;
            ActivityInfoItemChoice_type1 choice = null;
            ActivityInfoItem_type0[] activityInfoItems = response.getActivityInfoItem();

            for (int i = 0; i < activityInfoItems.length; i++) {
                StringBuffer buffer = new StringBuffer("activityId = ");
                buffer.append(activityInfoItems[i].getActivityID());

                choice = activityInfoItems[i].getActivityInfoItemChoice_type1();

                if (choice.isActivityInfoSpecified()) {
                    computingActivity = choice.getActivityInfo();

                    buffer.append("\nid = ").append(checkValue(computingActivity.getID()));
                    buffer.append("\nidFromEndpoint = ").append(checkValue(computingActivity.getIDFromEndpoint()));
                    buffer.append("\nlocalIDFromManager = ").append(checkValue(computingActivity.getLocalIDFromManager()));
                    buffer.append("\nbaseType = ").append(checkValue(computingActivity.getType()));
                    buffer.append("\ntype = ").append(checkValue(computingActivity.getType().toString()));
                    buffer.append("\nname = ").append(checkValue(computingActivity.getName()));
                    buffer.append("\ndescription = ").append(checkValue(computingActivity.getJobDescription()));
                    buffer.append("\ncomputingManagerExitCode = ").append(checkValue(computingActivity.getComputingManagerExitCode()));
                    buffer.append("\nlocalOwner = ").append(checkValue(computingActivity.getLocalOwner()));
                    buffer.append("\nowner = ").append(checkValue(computingActivity.getOwner()));
                    buffer.append("\nuserDomain = ").append(checkValue(computingActivity.getUserDomain()));
                    buffer.append("\nlogDir = ").append(checkValue(computingActivity.getLogDir()));
                    buffer.append("\nqueue = ").append(checkValue(computingActivity.getQueue()));
                    buffer.append("\nstdIn = ").append(checkValue(computingActivity.getStdIn()));
                    buffer.append("\nstdOut = ").append(checkValue(computingActivity.getStdOut()));
                    buffer.append("\nstdErr = ").append(checkValue(computingActivity.getStdErr()));
                    buffer.append("\nsubmissionClientName = ").append(checkValue(computingActivity.getSubmissionClientName()));
                    buffer.append("\nsubmissionHost = ").append(checkValue(computingActivity.getSubmissionHost()));
                    buffer.append("\nexitCode = ").append(checkValue(computingActivity.getExitCode()));
                    buffer.append("\ncreationTime = ").append(checkValue(computingActivity.getCreationTime()));
                    buffer.append("\nstartTime = ").append(checkValue(computingActivity.getStartTime()));
                    buffer.append("\nsubmissionTime = ").append(checkValue(computingActivity.getSubmissionTime()));
                    buffer.append("\ncomputingManagerSubmissionTime = ").append(checkValue(computingActivity.getComputingManagerSubmissionTime()));
                    buffer.append("\ncomputingManagerEndTime = ").append(checkValue(computingActivity.getComputingManagerEndTime()));
                    buffer.append("\nendTime = ").append(checkValue(computingActivity.getEndTime()));
                    buffer.append("\nworkingAreaEraseTime = ").append(checkValue(computingActivity.getWorkingAreaEraseTime()));
                    buffer.append("\nproxyExpirationTime = ").append(checkValue(computingActivity.getProxyExpirationTime()));
                    buffer.append("\nstate = ").append(checkValue(computingActivity.getState()));
                    buffer.append("\nrestartState = ").append(checkValue(computingActivity.getRestartState()));
                    buffer.append("\nerror = ").append(checkValue(computingActivity.getError()));
                    buffer.append("\notherInfo = ").append(checkValue(computingActivity.getOtherInfo()));
                    buffer.append("\notherMessages = ").append(checkValue(computingActivity.getOtherMessages()));
                    buffer.append("\nexecutionNode = ").append(checkValue(computingActivity.getExecutionNode()));
                    buffer.append("\nrequestedApplicationEnvironment = ").append(checkValue(computingActivity.getRequestedApplicationEnvironment()));
                    buffer.append("\nrequestedSlots = ").append(computingActivity.getRequestedSlots());
                    buffer.append("\nrequestedTotalCPUTime = ").append(checkValue(computingActivity.getRequestedTotalCPUTime()));
                    buffer.append("\nrequestedTotalWallTime = ").append(checkValue(computingActivity.getRequestedTotalWallTime()));
                    buffer.append("\nusedMainMemory = ").append(checkValue(computingActivity.getUsedMainMemory()));
                    buffer.append("\nusedTotalCPUTime = ").append(checkValue(computingActivity.getUsedTotalCPUTime()));
                    buffer.append("\nusedTotalWallTime = ").append(checkValue(computingActivity.getUsedTotalWallTime()));
                    buffer.append("\nvalidity = ").append(checkValue(computingActivity.getValidity()));
                    buffer.append("\nwaitingPosition = ").append(computingActivity.getWaitingPosition());

                    extensions = computingActivity.getExtensions();

                    if (extensions != null) {
                        buffer.append("\nextensions = {");

                        for (Extension_t extension : extensions.getExtension()) {
                            buffer.append("\n\t[localID = ").append(checkValue(extension.getLocalID().getLocalID_t()));
                            buffer.append("; key = ").append(checkValue(extension.getKey()));
                            buffer.append("; value = ").append(checkValue(extension.getValue())).append("]");
                            buffer.append("; any = ").append(extension.getExtraElement());
                        }

                        buffer.append("\n}");
                    }
                } else {
                    InternalBaseFault_Type fault = choice.getInternalBaseFault();

                    if (fault != null) {
                        buffer.append("\nfault = ").append(fault.getClass().getName());
                        buffer.append("\nmessage = ").append(fault.getMessage());
                        buffer.append("\ntimestamp = " + fault.getTimestamp().getTime());

                        if (fault.getDescription() != null) {
                            buffer.append("\ndescription = " + fault.getDescription());
                        }
                    }
                }

                System.out.println(buffer.toString());
            }
        } else {
            printUsage();
        }
    }

    private void printResultItems(ResponseItem_type0[] responseItems) {
        if (responseItems == null) {
            System.out.println("none result received");
        }

        for (int i = 0; i < responseItems.length; i++) {
            System.out.println("activityId = " + responseItems[i].getActivityID());
            ResponseItemChoice_type0 choice = responseItems[i].getResponseItemChoice_type0();
            
            if (choice == null) {
                System.out.println("SUCCESS");
            } else if (choice.isEstimatedTimeSpecified()) {
                Calendar esTime = Calendar.getInstance();
                esTime.setTimeInMillis(choice.getEstimatedTime().longValue());
                System.out.println("SUCCESS\nestimated time = " + esTime.getTime());
            } else if (choice.isInternalBaseFaultSpecified()) {
                System.out.println("ERROR: " + choice.getInternalBaseFault().getMessage());
            }
        }
    }
}
