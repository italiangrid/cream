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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.channels.AlreadyConnectedException;
import java.nio.channels.ConnectionPendingException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;

import org.apache.log4j.Logger;
import org.glite.ce.commonj.utils.BooleanLock;

public class BLAHNotifierClient extends Thread {
    private static final Logger logger = Logger.getLogger(BLAHNotifierClient.class.getName());
    private String lrms = null;
    private String host = null;
    private String prefix = null;
    private int port = -1;
    private int version = -1;
    private int retryCount = 100;
    private int retryDelay = 60000;
    private Socket socket = null;
    private BooleanLock isNTFDATEfinished = null;
    private PrintWriter out;
    private BufferedReader breader;
    private BLAHJobStatusChangeListener jobStatusChangeListener = null;

    private boolean working = true;
    
    public BLAHNotifierClient() {
        this(null);
    }

    public BLAHNotifierClient(BLAHNotifierInfo blahNotifierInfo) {
        super("BLAHNotifierClient");
        setDaemon(true);
        isNTFDATEfinished = new BooleanLock(false);
        
        if (blahNotifierInfo != null) {
            lrms = blahNotifierInfo.getLRMS();
            host = blahNotifierInfo.getHost();
            port = blahNotifierInfo.getPort();
        }        
    }

    private boolean getConnection(int maxRetryCount) {
        int i = maxRetryCount;
        boolean isConnected = false;
        boolean forever = i < 0; // forever is true if only if maxRetryCount is < 0
        isNTFDATEfinished.setValue(false);

        while (!isConnected && (forever || i-- > 0)) {
            logger.info("getting connection with the remote Notifier \"" + lrms + "\" [host:port=" + host + ":" + port + "; retryCount=" + i + "/" + maxRetryCount + "]");
            try {
                socket = new Socket(host, port);

                out = new PrintWriter(socket.getOutputStream(), true);
                breader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                logger.info("Connection with the remote Notifier [host:port=" + host + ":" + port + "] correctly established.");
                isConnected = true;
            } catch (IOException e) {
                logger.error("Connection with the remote Notifier [host:port=" + host + ":" + port + "] cannot be established");
            } catch (AlreadyConnectedException e) {
                logger.error("Connection with the remote Notifier [host:port=" + host + ":" + port + "] already established");
            } catch (ConnectionPendingException e) {
                logger.error("Connection with the remote Notifier [host:port=" + host + ":" + port + "] is pending");
            } catch (SecurityException e) {
                logger.error("Connection with the remote Notifier [host:port=" + host + ":" + port + "] failed: " + e.getMessage());
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("Connection with the remote Notifier [host:port=" + host + ":" + port + "] failed: " + e.getMessage());
            }

            if (!isConnected) {
                try {
                    sleep(retryDelay);
                } catch (InterruptedException ie) {
                    logger.error(ie.getMessage());
                }
            }
        }

        if (!isConnected && maxRetryCount > 0) {
            logger.error("Connection with the remote Notifier [host:port=" + host + ":" + port + "] failed: max retry count reached");
        }
        return isConnected;
    }

    public String getHost() {
        return host;
    }

//    public Calendar getOlderJobTime() throws Exception {
//        Calendar olderJobTime = Calendar.getInstance();
//        int[] jobStatusType = new int[] { JobStatus.HELD, JobStatus.IDLE, JobStatus.REALLY_RUNNING, JobStatus.RUNNING };
//
//        try {
//            String jobId = jobDB.retrieveOlderJobId(jobStatusType, lrms, null);
//
//            if (jobId != null) {
//                JobStatus jobStatus = jobDB.retrieveLastJobStatus(jobId, null);
//                if (jobStatus != null) {
//                    if (jobStatus.getType() == JobStatus.REALLY_RUNNING) {
//                        Job job = jobDB.retrieveJob(jobId, null);
//                        if (job == null) {
//                            logger.error("getOlderJobTime error: jobId " + jobId + " not found!");
//                        } else {
//                            jobStatus = job.getStatusAt(job.getStatusCount() - 2);
//                        }
//                    }
//
//                    if (jobStatus != null && jobStatus.getTimestamp().before(olderJobTime)) {
//                        olderJobTime = jobStatus.getTimestamp();
//                    }
//                }
//            }
//        } catch (IllegalArgumentException e) {
//            e.printStackTrace();
//            throw new CommandException("BLAHNotifierClient error: " + e.getMessage());
//        } catch (DatabaseException e) {
//            e.printStackTrace();
//            throw new CommandException("BLAHNotifierClient error: " + e.getMessage());
//        }
//
//        return olderJobTime;
//    }

    public BLAHJobStatusChangeListener getJobStatusChangeListener() {
        return jobStatusChangeListener;
    }

    public String getLRMS() {
        return lrms;
    }

    public int getPort() {
        return port;
    }

    public String getPrefix() {
        return prefix;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public int getRetryDelay() {
        return retryDelay;
    }

    public int getVersion() {
        return version;
    }

    public boolean isConnected() {
        return (socket != null && !socket.isClosed() && socket.isConnected() && breader != null);
    }

    public boolean isInitialized() {
        return isNTFDATEfinished.isTrue();
    }

    public void run() {
        logger.info("BLAHNotifierClient started!");

        while (working) {
            if (!getConnection(retryCount)) {
                this.interrupt();
                working = false;
            }

            if (!working) {
                continue;
            }

            try {
                sendCREAMFilterCommand();
                version = sendParserVersionCommand();
                logger.info("version = " + version);
                
                //ArrayList list = new ArrayList();                
                sendStartNotifyJobListCommand(null);
            } catch (Exception e) {
                logger.error(e.getMessage());
                this.interrupt();
                working = false;
            }


            logger.info("working = " + working);
            String line = null;
            String notification = "";

            try {
                while (working) {
                    synchronized(breader) {
                        line = breader.readLine();
                    }
                    
                    if (line == null) {
                        throw new Exception("Lost the connection with remote Notifier [host:port=" + host + ":" + port + "]!");
                    }

                    logger.debug("line = " + line);
                    if (line.startsWith("NTFDATE/END")) {
                        isNTFDATEfinished.setValue(true);
                    } else {
                        notification += line;

                        if (line.endsWith("]")) {                            
                            if (jobStatusChangeListener != null) {
                                jobStatusChangeListener.doOnJobStatusChange(new BLAHJobStatus(notification, BLAHJobStatus.BLAH_NOTIFIER));
                            }
                            
                            notification = "";
                        }
                    }
                    line = null;
                }
            } catch (Throwable t) {
                if (working) {
                    logger.error(t.getMessage());
                }
            }
        }
        
        //terminate();
        
        logger.info("BLAHNotifierClient terminated");
    }

    public void sendCREAMFilterCommand() throws Exception {
        if (prefix == null) {
            return;
        }
                
        synchronized(out) {
            logger.debug("sendCREAMFilterCommand() sending CREAMFILTER/" + prefix + " ...");
            out.println("CREAMFILTER/" + prefix);
            logger.debug("sendCREAMFilterCommand() sending CREAMFILTER/" + prefix + " ... done");
        }
        
        String filterResult = null;
        
        synchronized(breader) {
            filterResult = breader.readLine();
        }

        logger.debug("received: " + filterResult);
        if (filterResult.indexOf("ERROR") > 0) {
            throw new Exception("CREAMFILTER error: " + filterResult);
        }
    }

    public int sendParserVersionCommand() throws Exception {
        synchronized(out) {
            logger.debug("sendParserVersionCommand(): sending PARSERVERSION/ ...");
            out.println("PARSERVERSION/");
            logger.debug("sendParserVersionCommand(): sending PARSERVERSION/ ... done");
        }
        
        String parserVersion = null;
        synchronized(breader) {
            parserVersion = breader.readLine();
        }
        
        logger.debug("sendParserVersionCommand() received: " + parserVersion);

        if (parserVersion == null) {
            throw new Exception("PARSERVERSION did not return the version value");
        }

        if (parserVersion.endsWith("__0")) {
            return 0;
        } else if (parserVersion.endsWith("__1")) {
            return 1;
        }

        throw new Exception("PARSERVERSION returned an unsupported version value! (value=" + parserVersion + ")");
    }

    public void sendStartNotifyJobListCommand(List<String> jobList) throws Exception {
        sendStartNotifyJobListCommand(jobList, null);
    }

    public void sendStartNotifyJobListCommand(List<String> jobList, Calendar olderJobTime) throws Exception {
        StringBuffer cmd = new StringBuffer("STARTNOTIFYJOBLIST/");

        if (version == 0) {
            if (olderJobTime == null) {
                olderJobTime = Calendar.getInstance();
            }

            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            cmd.append(dateFormat.format(olderJobTime.getTime())).append(";");
        }

        if (jobList != null && jobList.size() > 0) {
            for (String jobId : jobList) {
                cmd.append(prefix).append(jobId.substring(5, jobId.length())).append(",");
            }

            cmd.deleteCharAt(cmd.length() - 1);

            jobList.clear();
            jobList = null;
        }

        synchronized(out) {
            logger.debug("sendStartNotifyJobListCommand(): sending " + cmd + " ...");
            out.println(cmd);
            logger.debug("sendStartNotifyJobListCommand(): sending STARTNOTIFYJOBLIST/ ... done");

            logger.debug("sendStartNotifyJobListCommand(): sending STARTNOTIFYJOBEND/ ...");
            out.println("STARTNOTIFYJOBEND/");
            logger.debug("sendStartNotifyJobListCommand(): sending STARTNOTIFYJOBEND/ ... done");
        }
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setJobStatusChangeListener(BLAHJobStatusChangeListener jobStatusChangeListener) {
        this.jobStatusChangeListener = jobStatusChangeListener;
    }

    public void setLRMS(String lrms) {
        this.lrms = lrms;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }
    
    public void setRetryDelay(int retryDelay) {
        this.retryDelay = retryDelay;
    }

    public void terminate() {
        logger.info("terminate invoked! [host:port=" + host + ":" + port + "]");
        working = false;

        try {
            if (socket != null) {
                socket.shutdownInput();
                socket.shutdownOutput();
                socket.close();
            }

            if (breader != null) {
                breader.close();
            }

            if (out != null) {
                out.close();
            }

            logger.info("terminated! [host:port=" + host + ":" + port + "]");
        } catch (Throwable t) {
            logger.error("failure on terminating the BLAHNotifierClient [host:port=" + host + ":" + port + "]: " + t.getMessage());
        }

        host = null;
        port = -1;
    }
}
