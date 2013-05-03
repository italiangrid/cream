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
package org.glite.ce.cream.activitymanagement;

import java.util.ArrayList;
import java.util.List;

import org.glite.ce.creamapi.cmdmanagement.Command;

public class ActivityCmd extends Command {
    //command category
    public static final String ACTIVITY_MANAGEMENT   = "ACTIVITY_MANAGEMENT";

    //command names
    public static enum ActivityCommandName { CANCEL_ACTIVITY, CREATE_ACTIVITY, GET_ACTIVITY_INFO, GET_ACTIVITY_STATUS, GET_DATABASE_VERSION,
        GET_RESOURCE_INFO, LIST_ACTIVITIES, NOTIFY_SERVICE, PAUSE_ACTIVITY, QUERY_RESOURCE_INFO,
        RESTART_ACTIVITY, RESUME_ACTIVITY, SET_ACTIVITY_STATUS, START_ACTIVITY, WIPE_ACTIVITY;

        public static List<String> toArrayList() {
            List<String> nameList = new ArrayList<String>(values().length);
            
            for (int i=0; i<values().length; i++) {
                nameList.add(values()[i].name());
            }
            
            return nameList;
        }
    }

    //command fields
    public static enum ActivityCommandField { ACTIVITY_CHECK, ACTIVITY_CHECK_ON, ACTIVITY_CHECK_OFF, ACTIVITY_DESCRIPTION, ACTIVITY_GLUE2_ATTRIBUTE_LIST,
        ACTIVITY_ID, ACTIVITY_ID_LIST, ACTIVITY_MANAGER, ACTIVITY_STATUS, ACTIVITY_STATUS_ATTRIBUTE_LIST, ACTIVITY_STATUS_DESCRIPTION, ACTIVITY_STATUS_LIST,
        ACTIVITY_STATUS_TIMESTAMP, COMPUTING_SERVICE, DELEGATION_SANDBOX_FILE_NAME, DELEGATION_SANDBOX_PATH, FROM_DATE, IS_TRUNCATED, LIMIT, LOCAL_USER, LOCAL_USER_GROUP,
        NOTIFY_MESSAGE, SERVICE_URL, SERVICE_GSI_URL, STAGE_IN_URI, STAGE_OUT_URI, TO_DATE, USER_DN_RFC2253, USER_DN_X500, USER_FQAN, VIRTUAL_ORGANISATION, XPATH_QUERY 
    }

    public ActivityCmd(ActivityCommandName commandName) {
        super(commandName.name(), ACTIVITY_MANAGEMENT);
    }

    public void addParameter(ActivityCommandField field, Object value) {
        if (field != null) {
            addParameter(field.name(), value);
        }
    }

    public ActivityCommandName getActivityCommandName() {
        return ActivityCommandName.valueOf(getName());
    }

    public Object getParameter(ActivityCommandField field) {
        if (field != null) {
            return getParameter(field.name());
        }
        return null;
    }

    public String getParameterAsString(ActivityCommandField field) {
        if (field != null) {
            return getParameterAsString(field.name());
        }
        return null;
    }
}
