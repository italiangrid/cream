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

package org.glite.ce.cream.jobmanagement.command;

import java.util.ArrayList;
import java.util.List;

import org.glite.ce.creamapi.eventmanagement.Event;
import org.glite.ce.creamapi.jobmanagement.cmdexecutor.JobCommandConstant;

public final class QueryEventCmd extends JobCmd {
    private static final String EVENT_TYPE = "EVENT_TYPE";
    private static final String FROM_EVENT_ID = "FROM_EVENT_ID";
    private static final String MAX_QUERY_EVENT_RESULT_SIZE = "MAX_QUERY_EVENT_RESULT_SIZE";
    private static final String TO_EVENT_ID = "TO_EVENT_ID";

    public QueryEventCmd(String eventType) {
        super(JobCommandConstant.QUERY_EVENT);
        setAsynchronous(false);
        addParameter(EVENT_TYPE, eventType);
    }

    public List<Event> geEvents() {
        List<Event> eventList =  (List<Event>) getResult().getParameter("EVENT_LIST");
        
        if (eventList == null) {
            eventList = new ArrayList<Event>(0);
        }
        
        return eventList;
    }

    public String geEventType() {
        return getParameterAsString(EVENT_TYPE);
    }

    public String getFromEventId() {
        return getParameterAsString(FROM_EVENT_ID);
    }

    public int getMaxQueryEventResultSize() {
        String size = getParameterAsString(MAX_QUERY_EVENT_RESULT_SIZE);

        if (size != null) {
            return Integer.parseInt(size);
        }

        return -1;
    }

    public String getToEventId() {
        return getParameterAsString(TO_EVENT_ID);
    }

    public void setFromEventId(String eventId) {
        if (eventId != null) {
            addParameter(FROM_EVENT_ID, eventId);
        }
    }

    public void setMaxQueryEventResultSize(int maxQueryEventResultSize) {
        if (maxQueryEventResultSize > 0) {
            addParameter(MAX_QUERY_EVENT_RESULT_SIZE, "" + maxQueryEventResultSize);
        }
    }

    public void setToEventId(String eventId) {
        if (eventId != null) {
            addParameter(TO_EVENT_ID, eventId);
        }
    }
}
