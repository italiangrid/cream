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

import org.glite.authz.common.model.Attribute;
import org.glite.authz.pep.profile.AbstractAuthorizationProfile;
import org.glite.ce.commonj.authz.AuthZConstants;
import org.glite.ce.commonj.authz.AuthorizationException;
import org.glite.ce.commonj.authz.argus.ActionMappingInterface;

public class ESActionMapping
    implements ActionMappingInterface {

    private final static String ES_NS = "http://www.eu-emi.eu/es/2010/12";

    private final static String DELEGATION_NS = "http://www.gridsite.org/namespaces/delegation-21";

    public static final String[] mandProperties = { AuthZConstants.LOCAL_USER_ID, AuthZConstants.LOCAL_GROUP_ID };

    public String getXACMLAction(QName operation) {
        String opName = operation.getLocalPart();
        String opNS = operation.getNamespaceURI();

        if (opNS.startsWith(ES_NS)) {

            if (opName.equals("CreateActivity")) {
                return ESAuthorizationProfile.ACTION_ES_CREATION;
            }

            if (opName.equals("ListActivities") || opName.equals("GetActivityStatus")
                    || opName.equals("GetActivityInfo")) {
                return ESAuthorizationProfile.ACTION_ES_ACTIVITY;
            }

            if (opName.equals("CancelActivity") || opName.equals("WipeActivity")) {
                return ESAuthorizationProfile.ACTION_ES_ACT_MAN;
            }

            if (opName.equals("PauseActivity") || opName.equals("ResumeActivity")) {
                return ESAuthorizationProfile.ACTION_ES_ACT_MAN;
            }

            if (opName.equals("RestartActivity") || opName.equals("NotifyService")) {
                return ESAuthorizationProfile.ACTION_ES_ACT_MAN;
            }

            if (opName.equals("GetResourceInfo") || opName.equals("QueryResourceInfo")) {
                return ESAuthorizationProfile.ACTION_ES_ACT_MAN;
            }

        } else if (opNS.equals(DELEGATION_NS)) {

            return ESAuthorizationProfile.ACTION_ES_DELEGATE;
        }

        return null;
    }

    public String getAttributeMapping(String obId, String attrId) {

        if (ESAuthorizationProfile.ID_POSIX_ENV_MAP.equals(obId)) {

            if (ESAuthorizationProfile.ID_USERID_ASSIGN.equals(attrId)) {
                return AuthZConstants.LOCAL_USER_ID;
            }

            if (ESAuthorizationProfile.ID_PGROUPID_ASSIGN.equals(attrId)) {
                return AuthZConstants.LOCAL_GROUP_ID;
            }
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
        return new ESAuthorizationProfile();
    }

    public class ESAuthorizationProfile
        extends AbstractAuthorizationProfile {

        public final static String ACTION_ES_CREATION = "http://www.eu-emi.eu/es/2010/12/creation";

        public final static String ACTION_ES_ACTIVITY = "http://www.eu-emi.eu/es/2010/12/activity";

        public final static String ACTION_ES_ACT_MAN = "http://www.eu-emi.eu/es/2010/12/activitymanagement";

        public final static String ACTION_ES_RES_INFO = "http://www.eu-emi.eu/es/2010/12/resourceinfo";

        public final static String ACTION_ES_DELEGATE = "http://www.gridsite.org/namespaces/delegation-21";

        public final static String ID_POSIX_ENV_MAP = "http://dci-sec.org/xacml/obligation/map-local-user/posix";

        public final static String ID_PROFILE = "http://dci-sec.org/xacml/attribute/profile-id";

        public final static String ID_USERID_ASSIGN = "http://dci-sec.org/xacml/attribute/user-id";

        public final static String ID_GROUPID_ASSIGN = "http://dci-sec.org/xacml/attribute/group-id";

        public final static String ID_PGROUPID_ASSIGN = "http://dci-sec.org/xacml/attribute/group-id/primary";

        public final static String ES_XACML_PROFILE = "http://dci-sec.org/xacml/profile/common-authz/1.1";

        public ESAuthorizationProfile() {
            super(ES_XACML_PROFILE);
        }

        public String getProfileIdAttributeIdentifer() {
            return ID_PROFILE;
        }

        protected String getSubjectKeyInfoDatatype() {
            return Attribute.DT_STRING;
        }

        public String getMapUserToPOSIXEnvironmentObligationIdentifier() {
            return ID_POSIX_ENV_MAP;
        }

        public String getUserIdAttributeAssignmentIdentifier() {
            return ID_USERID_ASSIGN;
        }

        public String getGroupIdAttributeAssignmentIdentifier() {
            return ID_GROUPID_ASSIGN;
        }

        public String getPrimaryGroupIdAttributeAssignmentIdentifier() {
            return ID_PGROUPID_ASSIGN;
        }

    }
}