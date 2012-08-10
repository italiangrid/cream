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

package org.glite.ce.cream.authz.argus;

import java.util.Iterator;

import javax.xml.namespace.QName;

import org.glite.authz.common.profile.GLiteAuthorizationProfileConstants;
import org.glite.authz.pep.profile.AbstractAuthorizationProfile;
import org.glite.authz.pep.profile.GridCEAuthorizationProfile;
import org.glite.ce.commonj.authz.AuthZConstants;
import org.glite.ce.commonj.authz.AuthorizationException;
import org.glite.ce.commonj.authz.argus.ActionMappingInterface;

public class ActionMapping
    implements ActionMappingInterface {

    private final static String CREAM_NS = "http://glite.org/2007/11/ce/cream";

    private final static String ES_NS = "http://www.eu-emi.eu/es/2010/12";

    private final static String DELEGATION_NS = "http://www.gridsite.org/namespaces/delegation-2";

    public final static String XACML_PREFIX = "http://glite.org/xacml/";

    public static final String ID_ATTRIBUTE_JOB_ADMIN = XACML_PREFIX + "obligation/local-role/administrator";

    public static final String[] mandProperties = { AuthZConstants.LOCAL_USER_ID, AuthZConstants.LOCAL_GROUP_ID };

    public String getXACMLAction(QName operation) {

        String opName = operation.getLocalPart();
        String opNS = operation.getNamespaceURI();

        if (opNS.startsWith(CREAM_NS)) {

            if (opName.equals("JobRegister") || opName.equals("JobStart") || opName.equals("JobSetLeaseId")) {
                return GridCEAuthorizationProfile.ACTION_JOB_SUBMIT;
            }

            if (opName.equals("JobCancel") || opName.equals("JobPurge")) {
                return GridCEAuthorizationProfile.ACTION_JOB_TERMINATE;
            }

            if (opName.equals("JobInfo") || opName.equals("JobStatus") || opName.equals("JobList")
                    || opName.equals("QueryEvent")) {
                return GridCEAuthorizationProfile.ACTION_JOB_GET_INFO;
            }

            if (opName.equals("JobSuspend") || opName.equals("JobResume")) {
                return GridCEAuthorizationProfile.ACTION_JOB_MANAGE;
            }

            if (opName.equals("getLease") || opName.equals("getLeaseList")) {
                return GridCEAuthorizationProfile.ACTION_LEASE_GET_INFO;
            }

            if (opName.equals("setLease") || opName.equals("deleteLease")) {
                return GridCEAuthorizationProfile.ACTION_LEASE_MANAGE;
            }

            if (opName.equals("getServiceInfo") || opName.equals("getVersion") || opName.equals("getInterfaceVersion")
                    || opName.equals("getServiceMetadata")) {
                return GridCEAuthorizationProfile.ACTION_GET_INFO;
            }

        } else if (opNS.equals(DELEGATION_NS)) {

            if (opName.equals("getTerminationTime")) {
                return GridCEAuthorizationProfile.ACTION_DELEGATION_GET_INFO;
            }

            if (opName.equals("getProxyReq") || opName.equals("getNewProxyReq") || opName.equals("putProxy")
                    || opName.equals("renewProxyReq") || opName.equals("destroy")) {
                return GridCEAuthorizationProfile.ACTION_DELEGATION_MANAGE;
            }

        } else if (opNS.startsWith(ES_NS)) {

            if (opName.equals("CreateActivity")) {
                return GridCEAuthorizationProfile.ACTION_JOB_SUBMIT;
            }

            if (opName.equals("ListActivities") || opName.equals("GetActivityStatus")
                    || opName.equals("GetActivityInfo")) {
                return GridCEAuthorizationProfile.ACTION_JOB_GET_INFO;
            }

            if (opName.equals("CancelActivity") || opName.equals("WipeActivity")) {
                return GridCEAuthorizationProfile.ACTION_JOB_TERMINATE;
            }

            if (opName.equals("PauseActivity") || opName.equals("ResumeActivity") || opName.equals("RestartActivity")
                    || opName.equals("NotifyService")) {
                return GridCEAuthorizationProfile.ACTION_JOB_MANAGE;
            }

            if (opName.equals("InitDelegation") || opName.equals("PutDelegation")) {
                return GridCEAuthorizationProfile.ACTION_DELEGATION_MANAGE;
            }

            if (opName.equals("GetDelegationInfo")) {
                return GridCEAuthorizationProfile.ACTION_DELEGATION_GET_INFO;
            }

            if (opName.equals("GetResourceInfo") || opName.equals("QueryResourceInfo")) {
                return GridCEAuthorizationProfile.ACTION_GET_INFO;
            }
        }

        return null;
    }

    public String getAttributeMapping(String obId, String attrId) {
        if (GLiteAuthorizationProfileConstants.ID_OBLIGATION_POSIX_ENV_MAP.equals(obId)) {
            if (GLiteAuthorizationProfileConstants.ID_ATTRIBUTE_USER_ID.equals(attrId)) {
                return AuthZConstants.LOCAL_USER_ID;
            }

            if (GLiteAuthorizationProfileConstants.ID_ATTRIBUTE_PRIMARY_GROUP_ID.equals(attrId)) {
                return AuthZConstants.LOCAL_GROUP_ID;
            }
        }

        if (ID_ATTRIBUTE_JOB_ADMIN.equals(obId)) {
            return AuthZConstants.JOB_ADMIN;
        }
        return null;
    }

    public String[] getMandatoryProperties() {
        return mandProperties;
    }

    public void checkMandatoryProperties(Iterator<String> props)
        throws AuthorizationException {

        boolean missingUser = true;
        boolean missingGroup = true;

        while (props.hasNext()) {
            String prop = props.next();
            if (prop.equals(AuthZConstants.LOCAL_USER_ID)) {
                missingUser = false;
            } else if (prop.equals(AuthZConstants.LOCAL_GROUP_ID)) {
                missingGroup = false;
            }
        }

        if (missingUser) {
            throw new AuthorizationException("Cannot map grid user onto a local account");
        }

        if (missingGroup) {
            throw new AuthorizationException("Cannot map grid user onto a local group");
        }
    }

    public AbstractAuthorizationProfile getProfile() {
        return GridCEAuthorizationProfile.getInstance();
    }
}
