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

package org.glite.ce.cream.activitymanagement.cmdexecutor;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.glite.ce.cream.activitymanagement.db.ActivityDBImplementation;
import org.glite.ce.cream.blahmanagement.BLAHClient;
import org.glite.ce.cream.blahmanagement.BLAHNotifierClient;
import org.glite.ce.creamapi.activitymanagement.ActivityStatus.StatusName;
import org.glite.ce.creamapi.activitymanagement.db.ActivityDBInterface;

public final class ActivityStatusMonitor extends Thread {
    private static Logger logger = Logger.getLogger(ActivityStatusMonitor.class);
    private boolean terminate = false;
    private long rate = 6;      // 30 hours
    private int statusAge = 24; // 24 hours
    private String lrms = null;
    private BLAHClient blahClient = null;

    public ActivityStatusMonitor(BLAHClient blahClient, String lrms, long rate, int statusAge) {
        super("ActivityStatusMonitor");
        setDaemon(true);
        
        this.blahClient = blahClient;
        this.lrms = lrms;
        this.rate = rate;
        this.statusAge = statusAge;

        start();
    }
        
    public void run() {
        logger.info("starting: lrms=" + lrms + "; rate=" + rate + " hour(s); statusAge=" + statusAge + " hour(s)");
        List<String> activityIdList = null;
        final List<StatusName> statusList = new ArrayList<StatusName>(2);
        statusList.add(StatusName.PROCESSING_QUEUED);
        statusList.add(StatusName.PROCESSING_RUNNING);

        ActivityDBInterface activityDB = null;
        try {
            activityDB = new ActivityDBImplementation();
        } catch (Throwable t) {
            logger.error("ActivityDB initialization failed: " + t.getMessage());
        }

        BLAHNotifierClient blahNotifierClient = blahClient.getBLAHNotifierClient(lrms);
       
        if (blahNotifierClient == null) {
            logger.error("BLAHNotifier for " + lrms + " not found!");
            terminate = true;
        }

        while (!terminate) {
            try {
                activityIdList = activityDB.listActivitiesForStatus(statusList, null, statusAge*60);
            } catch (Throwable t) {
                logger.warn(t.getMessage());
            }

            if (activityIdList != null && activityIdList.size() > 0) {                
                try {
                    blahNotifierClient.sendStartNotifyJobListCommand(activityIdList);
                } catch (Throwable t) {
                    logger.error(t.getMessage());
                }
                activityIdList.clear();
            }            
            
            synchronized(this) {
                try {
                    logger.debug("waiting " + rate + " hour(s)...");
                    wait(rate*3600000);
                    logger.debug("waiting " + rate + " hour(s)... done");
                } catch (InterruptedException e) {
                    terminate = true;
                }
            }
        }

        logger.info("exit");
    }

    public void terminate() {
        logger.info("teminate invoked!");
        terminate = true;

        synchronized(this) {
            notifyAll();
        }
        logger.info("teminated!");
    }
}

