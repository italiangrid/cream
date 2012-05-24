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

package org.glite.ce.cream.blahmanagement;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Hashtable;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

public class BLAHJobStatus {
    private static final Logger logger = Logger.getLogger(BLAHJobStatus.class.getName());
    private int source = -1;
    private int status = -1;
    private String reason = null;
    private String exitCode = null;
    private String lrmsJobId = null;
    private String workerNode = null;
    private String blahJobName = null;
    private String clientJobId = null;
    private Calendar changeTime = null;

    public static final int BLAH_NOTIFIER = 0;
    public static final int BLAH_NOTIFICATION_LISTENER = 1;
    public static final int IDLE = 1;
    public static final int RUNNING = 2;
    public static final int CANCELLED = 3;
    public static final int DONE = 4;
    public static final int HELD = 5;
    public static final int REALLY_RUNNING = 11;

    public BLAHJobStatus(String notification) {
        this(notification, -1);
    }

    public BLAHJobStatus(String notification, int source) {
        if (notification == null) {
            return;
        }

        this.source = source;

        logger.debug(notification);

        Hashtable<String, String> attributeTable = new Hashtable<String, String>(0);

        if (notification.startsWith("[")) {
            notification = notification.substring(1);
        }

        if (notification.endsWith("]")) {
            notification = notification.substring(0, notification.length() - 1);
        }

        StringTokenizer st = new StringTokenizer(notification, ";");
        String attribute = null;
        String value = null;
        int index = -1;

        while (st.hasMoreElements()) {
            attribute = st.nextToken();

            if ((index = attribute.indexOf("=")) != -1) {
                value = attribute.substring(index + 1, attribute.length());
                value = value.trim();

                if (value.startsWith("\"")) {
                    value = value.substring(1);
                }

                if (value.endsWith("\"")) {
                    value = value.substring(0, value.length() - 1);
                }

                attributeTable.put(attribute.substring(0, index).trim(), value.trim());
            }
        }

        if (attributeTable.containsKey("ChangeTime")) {
            changeTime = new GregorianCalendar();
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            dateFormat.setCalendar(changeTime);
            try {
                dateFormat.parse(attributeTable.get("ChangeTime"));
            } catch (ParseException e) {
                logger.error(e.getMessage(), e);
            }
        }

        reason = attributeTable.get("Reason");
        exitCode = attributeTable.get("ExitCode");
        workerNode = attributeTable.get("WorkerNode");
        clientJobId = attributeTable.get("ClientJobId");
        blahJobName = attributeTable.get("BlahJobName");

        if (attributeTable.containsKey("JobStatus")) {
            status = Integer.parseInt(attributeTable.get("JobStatus"));
        }
    }

    public String getBlahJobName() {
        return blahJobName;
    }

    public Calendar getChangeTime() {
        return changeTime;
    }

    public String getClientJobId() {
        return clientJobId;
    }

    public String getExitCode() {
        return exitCode;
    }

    public int getStatus() {
        return status;
    }

    public String getLRMSJobId() {
        return lrmsJobId;
    }

    public String getReason() {
        return reason;
    }

    public int getSource() {
        return source;
    }

    public String getWorkerNode() {
        return workerNode;
    }

    public void setBlahJobName(String blahJobName) {
        this.blahJobName = blahJobName;
    }

    public void setChangeTime(Calendar changeTime) {
        this.changeTime = changeTime;
    }

    public void setClientJobId(String clientJobId) {
        this.clientJobId = clientJobId;
    }

    public void setExitCode(String exitCode) {
        this.exitCode = exitCode;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public void setLRMSJobId(String lrmsJobId) {
        this.lrmsJobId = lrmsJobId;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public void setSource(int source) {
        this.source = source;
    }

    public void setWorkerNode(String workerNode) {
        this.workerNode = workerNode;
    }
    
    public String toString() {
        StringBuffer info = new StringBuffer();

        info.append("[clientJobId=").append(clientJobId);
        
        if (blahJobName != null) {
            info.append("; blahJobName=").append(blahJobName);
        }

        if (blahJobName != null) {
            info.append("; lrmsJobId=").append(lrmsJobId);
        }

        if (status > -1) {
            info.append("; status=").append(status);
        }

        if (reason != null) {
            info.append("; reason=\"").append(reason).append("\"");
        }
        
        if (workerNode != null) {
            info.append("; workerNode=").append(workerNode);
        }

        if (exitCode != null) {
            info.append("; exitCode=").append(exitCode);
        }

        if (changeTime != null) {
            info.append("; changeTime=").append(changeTime.getTime());
        }

        if (source > -1) {
            info.append("; source=").append(source == BLAH_NOTIFIER ? "BLAH_NOTIFIER" : "BLAH_NOTIFICATION_LISTENER");
        }
        
        info.append("]");
        
        return info.toString();
    }
}
