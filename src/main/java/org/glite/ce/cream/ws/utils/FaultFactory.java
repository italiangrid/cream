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

package org.glite.ce.cream.ws.utils;

import java.util.Calendar;

import org.glite.ce.creamapi.ws.cream2.Generic_Fault;
import org.glite.ce.creamapi.ws.cream2.InvalidArgument_Fault;
import org.glite.ce.creamapi.ws.cream2.JobSubmissionDisabled_Fault;
import org.glite.ce.creamapi.ws.cream2.types.AuthorizationFault;
import org.glite.ce.creamapi.ws.cream2.types.DateMismatchFault_type0;
import org.glite.ce.creamapi.ws.cream2.types.DelegationIdMismatchFault_type0;
import org.glite.ce.creamapi.ws.cream2.types.DelegationProxyFault_type0;
import org.glite.ce.creamapi.ws.cream2.types.GenericFault;
import org.glite.ce.creamapi.ws.cream2.types.GenericFault_type0;
import org.glite.ce.creamapi.ws.cream2.types.InvalidArgumentFault;
import org.glite.ce.creamapi.ws.cream2.types.JobStatusInvalidFault_type0;
import org.glite.ce.creamapi.ws.cream2.types.JobSubmissionDisabledFault;
import org.glite.ce.creamapi.ws.cream2.types.JobUnknownFault_type0;
import org.glite.ce.creamapi.ws.cream2.types.LeaseIdMismatchFault_type0;
import org.glite.ce.creamapi.ws.cream2.types.NoSuitableResourcesFault;
import org.glite.ce.creamapi.ws.cream2.types.OperationNotSupportedFault;

public class FaultFactory {

    public static AuthorizationFault makeAuthorizationFault(String methodName, String errorCode, String description, String faultCause) {
        AuthorizationFault fault = new AuthorizationFault();
        fault.setMethodName(methodName);
        fault.setErrorCode(errorCode);
        fault.setDescription(description);
        fault.setFaultCause(faultCause);
        fault.setTimestamp(Calendar.getInstance());

        return fault;
    }

    public static DateMismatchFault_type0 makeDateMismatchFault(String methodName, String errorCode, String description, String faultCause) {
        DateMismatchFault_type0 fault = new DateMismatchFault_type0();
        fault.setMethodName(methodName);
        fault.setErrorCode(errorCode);
        fault.setDescription(description);
        fault.setFaultCause(faultCause);
        fault.setTimestamp(Calendar.getInstance());

        return fault;
    }

    public static DelegationIdMismatchFault_type0 makeDelegationIdMismatchFault(String methodName, String errorCode, String description, String faultCause) {
        DelegationIdMismatchFault_type0 fault = new DelegationIdMismatchFault_type0();
        fault.setMethodName(methodName);
        fault.setErrorCode(errorCode);
        fault.setDescription(description);
        fault.setFaultCause(faultCause);
        fault.setTimestamp(Calendar.getInstance());

        return fault;
    }

    public static DelegationProxyFault_type0 makeDelegationProxyFault(String methodName, String errorCode, String description, String faultCause) {
        DelegationProxyFault_type0 fault = new DelegationProxyFault_type0();
        fault.setMethodName(methodName);
        fault.setErrorCode(errorCode);
        fault.setDescription(description);
        fault.setFaultCause(faultCause);
        fault.setTimestamp(Calendar.getInstance());

        return fault;
    }

    public static GenericFault_type0 makeGenericFault_type0(String methodName, String errorCode, String description, String faultCause) {
        GenericFault_type0 fault = new GenericFault_type0();
        fault.setMethodName(methodName);
        fault.setErrorCode(errorCode);
        fault.setDescription(description);
        fault.setFaultCause(faultCause);
        fault.setTimestamp(Calendar.getInstance());

        return fault;
    }

    public static Generic_Fault makeGenericFault(String methodName, String errorCode, String description, String faultCause) {
        GenericFault_type0 message = new GenericFault_type0();
        message.setMethodName(methodName);
        message.setErrorCode(errorCode);
        message.setDescription(description);
        message.setFaultCause(faultCause);
        message.setTimestamp(Calendar.getInstance());

        GenericFault faultMessage = new GenericFault();
        faultMessage.setGenericFault(message);

        Generic_Fault fault = new Generic_Fault();
        fault.setFaultMessage(faultMessage);

        return fault;
    }

    public static InvalidArgument_Fault makeInvalidArgumentFault(String methodName, String errorCode, String description, String faultCause) {
        InvalidArgumentFault message = new InvalidArgumentFault();
        message.setMethodName(methodName);
        message.setErrorCode(errorCode);
        message.setDescription(description);
        message.setFaultCause(faultCause);
        message.setTimestamp(Calendar.getInstance());

        InvalidArgument_Fault fault = new InvalidArgument_Fault();
        fault.setFaultMessage(message);

        return fault;
    }

    public static JobStatusInvalidFault_type0 makeJobStatusInvalidFault(String methodName, String errorCode, String description, String faultCause) {
        JobStatusInvalidFault_type0 fault = new JobStatusInvalidFault_type0();
        fault.setMethodName(methodName);
        fault.setErrorCode(errorCode);
        fault.setDescription(description);
        fault.setFaultCause(faultCause);
        fault.setTimestamp(Calendar.getInstance());

        return fault;
    }

    public static JobSubmissionDisabled_Fault makeJobSubmissionDisabledFault(String methodName, String errorCode, String description, String faultCause) {
        JobSubmissionDisabledFault message = new JobSubmissionDisabledFault();
        message.setMethodName(methodName);
        message.setErrorCode(errorCode);
        message.setDescription(description);
        message.setFaultCause(faultCause);
        message.setTimestamp(Calendar.getInstance());

        JobSubmissionDisabled_Fault fault = new JobSubmissionDisabled_Fault();
        fault.setFaultMessage(message);

        return fault;
    }

    public static JobUnknownFault_type0 makeJobUnknownFault(String methodName, String errorCode, String description, String faultCause) {
        JobUnknownFault_type0 fault = new JobUnknownFault_type0();
        fault.setMethodName(methodName);
        fault.setErrorCode(errorCode);
        fault.setDescription(description);
        fault.setFaultCause(faultCause);
        fault.setTimestamp(Calendar.getInstance());

        return fault;
    }

    public static LeaseIdMismatchFault_type0 makeLeaseIdMismatchFault(String methodName, String errorCode, String description, String faultCause) {
        LeaseIdMismatchFault_type0 fault = new LeaseIdMismatchFault_type0();
        fault.setMethodName(methodName);
        fault.setErrorCode(errorCode);
        fault.setDescription(description);
        fault.setFaultCause(faultCause);
        fault.setTimestamp(Calendar.getInstance());

        return fault;
    }

    public static NoSuitableResourcesFault makeNoSuitableResourcesFault(String methodName, String errorCode, String description, String faultCause) {
        NoSuitableResourcesFault fault = new NoSuitableResourcesFault();
        fault.setMethodName(methodName);
        fault.setErrorCode(errorCode);
        fault.setDescription(description);
        fault.setFaultCause(faultCause);
        fault.setTimestamp(Calendar.getInstance());

        return fault;
    }

    public static OperationNotSupportedFault makeOperationNotSupportedFault(String methodName, String errorCode, String description, String faultCause) {
        OperationNotSupportedFault fault = new OperationNotSupportedFault();
        fault.setMethodName(methodName);
        fault.setErrorCode(errorCode);
        fault.setDescription(description);
        fault.setFaultCause(faultCause);
        fault.setTimestamp(Calendar.getInstance());

        return fault;
    }
}
