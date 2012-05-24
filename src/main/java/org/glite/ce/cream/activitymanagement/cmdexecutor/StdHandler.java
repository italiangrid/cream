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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Calendar;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.glite.ce.cream.activitymanagement.db.ActivityDBImplementation;
import org.glite.ce.creamapi.activitymanagement.Activity;
import org.glite.ce.creamapi.activitymanagement.ActivityStatus;
import org.glite.ce.creamapi.activitymanagement.ActivityStatus.StatusAttributeName;
import org.glite.ce.creamapi.activitymanagement.ActivityStatus.StatusName;
import org.glite.ce.creamapi.activitymanagement.db.ActivityDBInterface;

public final class StdHandler extends Thread {
    private static Logger logger = Logger.getLogger(StdHandler.class);
    private boolean terminate = false;
    private final Queue<String> activityIdQueue = new LinkedBlockingQueue<String>();

    public StdHandler() {
        super("StdHandler");
        setDaemon(true);
        start();
    }

    public boolean add(String activityId) {
        logger.debug("added activityId " + activityId);
        boolean result = activityIdQueue.add(activityId);

        if (result) {
            synchronized(activityIdQueue) {
                activityIdQueue.notifyAll();
            }
        }

        return result;
    }
    
    public boolean remove(String activityId) {
        logger.debug("removed activityId " + activityId);

        return activityIdQueue.remove(activityId);
    }
    
    private String readFile(String filePath, String userId) throws IllegalArgumentException, InterruptedException, IOException {
        String message = "";

        if (filePath == null) {
            throw (new IllegalArgumentException("stdErrorFilePath not specified"));
        }

        if (userId == null) {
            throw (new IllegalArgumentException("userId not specified!"));
        }

        Process proc = null;
        BufferedReader readIn = null;
        
        try {
            proc = Runtime.getRuntime().exec(new String[] { "sudo", "-S", "-n", "-u", userId, "/bin/cat", filePath });

            readIn = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            String strLine = null;
            
            while ((strLine = readIn.readLine()) != null) {
                message += strLine + " ";
            }
        } catch (Throwable e) {
            if (proc != null) {
                proc.destroy();
            }
        } finally {
            if (proc != null) {
                proc.waitFor();
                
                StringBuffer errorMessage = null;

                if(proc.exitValue() != 0) {
                    BufferedReader readErr = new BufferedReader(new InputStreamReader(proc.getErrorStream()));

                    errorMessage = new StringBuffer();
                    String inputLine = null; 

                    try {
                        while ((inputLine = readErr.readLine()) != null) {
                            errorMessage.append(inputLine);
                        }
                    } catch (IOException ioe) {
                        logger.error(ioe.getMessage());
                    } finally {
                        readErr.close();                        
                    }

                    if (errorMessage.length() > 0) {
                        errorMessage.append("\n");
                    }
                }

                try {
                    proc.getInputStream().close();
                } catch (IOException ioe) {}
                
                try {
                    proc.getErrorStream().close();
                } catch (IOException ioe) {}
                
                try {
                    proc.getOutputStream().close();
                } catch (IOException ioe) {}
                
                if(errorMessage != null) {
                    throw new IOException(errorMessage.toString());
                }
            }
            
            try {
                readIn.close();
            } catch (IOException ioe) {}
        }

        if (message.length() == 0) {
            throw (new FileNotFoundException("file \"" + filePath + " not found!"));
        }

        return message.trim();
    }
    
    public void run() {
        logger.debug("BEGIN StdHandler");
        boolean works = true;
        String path = null;
        String exitCode = null;
        String pattern = "activity exit status = ";
        String activityId = null;
        String description = null;
        String stdErrorMessage = null;
        long delay = 0; //0 sec
        long timeElapsed = -1;
        Activity activity = null;

        ActivityDBInterface activityDB = null;
        try {
            activityDB = new ActivityDBImplementation();
        } catch (Throwable t) {
            logger.error("ActivityDB initialization failed: " + t.getMessage());
        }
        
        while (!terminate) {
            while ((activityId = activityIdQueue.peek()) != null && works && !terminate) {
                try {
                    activity = activityDB.getActivity(activityId, null);                    
                } catch (Throwable t) {
                    logger.warn(t.getMessage());
                }
                
                if (activity == null) {
                    continue;
                }

                ActivityStatus status = activity.getStates().last();
                if (status.getStatusName() != StatusName.TERMINAL) {
                    continue;
                }

                timeElapsed = Calendar.getInstance().getTimeInMillis() - status.getTimestamp().toGregorianCalendar().getTimeInMillis();
                if (timeElapsed < 60000) {
                    delay = 60000 - timeElapsed;
                    works = false;
                    continue;
                } else {
                    delay = 0;
                }

                activityIdQueue.remove(activityId);
                
                exitCode = activity.getProperties().get(Activity.EXIT_CODE);
                
                if (!activity.getProperties().containsKey(Activity.EXIT_CODE)) {
                    path = activity.getProperties().get(Activity.SANDBOX_PATH) + File.separator + "StandardOutput";
                    
                    logger.debug("activity " + activity.getId() + ": retrieving the exit code from " + path );
                    
                    try {
                        exitCode = readFile(path, activity.getProperties().get(Activity.LOCAL_USER));
                    } catch (Throwable t) {
                        logger.error("cannot get the exit code from " + path + ": " + t.getMessage());
                        continue;
                    }
                    
                    if (exitCode == null) {
                        exitCode = "N/A";
                    }

                    int index = exitCode.indexOf(pattern);
                    if (index > -1) {
                        exitCode = exitCode.substring(index + pattern.length());
                        exitCode = exitCode.substring(0, exitCode.indexOf(" "));
                        exitCode = exitCode.trim();
                    } else {
                        exitCode = "N/A";
                    }
                    
                    activity.getProperties().put(Activity.EXIT_CODE, exitCode); 

                    logger.debug("activity " + activity.getId() + ": exit code retrieved from " + path + ": " + exitCode );
                    try {
                        activityDB.updateActivity(activity);
                    } catch (Throwable t) {
                        logger.warn("cannot update the activity " + activity.getId() + ":" + t.getMessage());
                    }           
                }

                if (status.getStatusAttributes().contains(StatusAttributeName.PROCESSING_CANCEL) || status.getStatusAttributes().contains(StatusAttributeName.APP_FAILURE)) {
                    path = activity.getProperties().get(Activity.SANDBOX_PATH) + File.separator + "StandardError";

                    logger.debug("activity " + activity.getId() + ": retrieving the failure reason from " + path );

                    try {
                        stdErrorMessage = readFile(path, activity.getProperties().get(Activity.LOCAL_USER));                       
                    } catch (Exception e) {
                        stdErrorMessage = "cannot get the failure reason from " + path + ": " + e.getMessage();
                    } 

                    if (stdErrorMessage == null || stdErrorMessage.equals("")) {
                        stdErrorMessage = "cannot get the failure reason from " + path;                      
                    }

                    description = status.getDescription();

                    if (description == null) {
                        status.setDescription(stdErrorMessage);                                
                    } else if (description.indexOf(stdErrorMessage) < 0) {
                        status.setDescription(description + "; " + stdErrorMessage);
                    }

                    logger.debug("activity " + activity.getId() + ": failure reason retrieved from " + path + ": " + status.getDescription());
                    try {
                        activityDB.updateActivityStatus(status);
                    } catch (Throwable t) {
                        logger.warn("cannot update the activity " + activity.getId() + " status:" + t.getMessage());
                    }                    
                }
            }

            synchronized(activityIdQueue) {
                try {
                    logger.debug("waiting " + delay + "...");
                    activityIdQueue.wait(delay);
                    logger.debug("waiting " + delay + "... done!" );

                    works = true;
                    delay = 0;
                } catch (InterruptedException e) {
                    terminate = true;
                }
            }
        }
        
        logger.debug("END StdHandler");
    }

    public void terminate() {
        logger.info("teminate invoked!");
        terminate = true;
        
        synchronized(activityIdQueue) {
            activityIdQueue.notifyAll();
        }
        logger.info("teminated!");
    }
}
