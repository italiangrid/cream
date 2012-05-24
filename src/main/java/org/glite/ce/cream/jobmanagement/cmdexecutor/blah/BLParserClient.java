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

package org.glite.ce.cream.jobmanagement.cmdexecutor.blah;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.io.*;
import java.nio.channels.AlreadyConnectedException;
import java.nio.channels.ConnectionPendingException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;

import org.apache.log4j.Logger;
import org.glite.ce.commonj.db.DatabaseException;
import org.glite.ce.commonj.utils.BooleanLock;
import org.glite.ce.creamapi.cmdmanagement.CommandException;
import org.glite.ce.creamapi.jobmanagement.Job;
import org.glite.ce.creamapi.jobmanagement.JobStatus;
import org.glite.ce.creamapi.jobmanagement.db.JobDBInterface;

public class BLParserClient extends Thread {
    private static final Logger logger = Logger.getLogger(BLParserClient.class.getName());
    private String lrmsName;
    private String blParserHost = null;
    private String jobIdPrefix = null;
    private int blParserPort = -1;
    private int blParserRetryCount = -1;
    private int blParserRetryDelay = -1;
    private BLAHExecutor blahExec = null;
    private LRMSEventsProcessor processor = null;
    private SocketChannel socketChannel = null;
    private BooleanLock isNTFDATEfinished = null;
    private PrintWriter out;
    private BufferedReader breader;
    private JobDBInterface jobDB = null;
    private boolean working = true;


    public BLParserClient(BLAHExecutor blahExec, LRMSEventsProcessor processor, String jobIdPrefix, String lrmsName, String blParserHost, String blParserPort, int blParserRetryCount, int blParserRetryDelay,
            JobDBInterface jobDB) throws BLAHException {
        super("BLParserClient");
        setDaemon(true);

        if (lrmsName == null) {
            throw (new BLAHException("LRMS name not specified"));
        }
        isNTFDATEfinished = new BooleanLock(false);
        this.blahExec = blahExec;
        this.processor = processor;
        this.jobIdPrefix = jobIdPrefix;
        this.lrmsName = lrmsName;
        this.blParserHost = blParserHost;
        this.blParserRetryCount = blParserRetryCount;
        this.blParserRetryDelay = blParserRetryDelay;

        if (jobDB == null) {
            throw (new BLAHException("jobDB not specified"));
        }

        this.jobDB = jobDB;

        try {
            this.blParserPort = Integer.parseInt(blParserPort);
        } catch (Exception e) {
        }
    }

    public void initBLParserClient() throws Exception {
        start();
        // isNTFDATEfinished.waitUntilStateIs(true, 600000); //wait 10 minutes
    }

    public Calendar getOlderJobTime() throws Exception {
        Calendar olderJobTime = Calendar.getInstance();
        int[] jobStatusType = new int[] { JobStatus.HELD, JobStatus.IDLE, JobStatus.REALLY_RUNNING, JobStatus.RUNNING };

        try {
            String jobId = jobDB.retrieveOlderJobId(jobStatusType, lrmsName, null);

            if (jobId != null) {
                JobStatus jobStatus = jobDB.retrieveLastJobStatus(jobId, null);
                if (jobStatus != null) {
                    if (jobStatus.getType() == JobStatus.REALLY_RUNNING) {
                        Job job = jobDB.retrieveJob(jobId, null);
                        if (job == null) {
                            logger.error("getOlderJobTime error: jobId " + jobId + " not found!");
                        } else {
                            jobStatus = job.getStatusAt(job.getStatusCount() - 2);
                        }
                    }

                    if (jobStatus != null && jobStatus.getTimestamp().before(olderJobTime)) {
                        olderJobTime = jobStatus.getTimestamp();
                    }
                }
            }
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            throw new CommandException("BLParserClient error: " + e.getMessage());
        } catch (DatabaseException e) {
            e.printStackTrace();
            throw new CommandException("BLParserClient error: " + e.getMessage());
        }

        return olderJobTime;
    }

    public boolean isInitialized() {
        return isNTFDATEfinished.isTrue();
    }

    public void terminate() {
        logger.info("terminate invoked! [" + blParserHost + ":" + blParserPort + "]");
        working = false;

        try {
            if (socketChannel != null) {
                if (socketChannel.socket() != null) {
                    socketChannel.socket().close();
                }

//                socketChannel.close();
//                socketChannel = null;
            }

            if (breader != null) {
                breader.close();
            }

            if (out != null) {
                out.close();
            }

            logger.info("terminated! [" + blParserHost + ":" + blParserPort + "]");
        } catch (Throwable t) {
            logger.error("failure on terminating the BLParserClient [" + blParserHost + ":" + blParserPort + "]: " + t.getMessage());
        }

        blParserHost = null;
        blParserPort = -1;
    }

    private void initializeConnection(int maxRetryCount) throws BLAHException {
        int i = maxRetryCount;
        boolean connected = false;
        boolean forever = i < 0; // forever is true if only if maxRetryCount is
        // < 0

        while (blParserHost == null && (forever || i-- > 0)) {
            String[] blahHostPort = blahExec.getBlahHostPort(lrmsName);

            if (blahHostPort != null && blahHostPort[2] != null) {
                // this.lrmsName = blahHostPort[0];
                this.blParserHost = blahHostPort[1];
                this.blParserPort = Integer.parseInt(blahHostPort[2]);
            } else {
                logger.info("initializeConnection: getting info about BLParser (" + lrmsName + ") from BLAH (retry count=" + i + "/" + maxRetryCount + ")");

                try {
                    sleep(blParserRetryDelay);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }
        }

        if (blParserHost == null) {
            throw (new BLAHException("initializeConnection error: cannot get BLParser (" + lrmsName + ") HOST:PORT information from BLAH. Please, be sure that BLAH is properly configured and RESTART the CREAM service."));
        }

        i = maxRetryCount;

        while (!connected && (forever || i-- > 0)) {
            try {
                connected = getBLParserConnection();
            } catch (Exception e) {
                logger.info("Retries left: " + i + ":   " + e.getMessage());

                try {
                    sleep(blParserRetryDelay);
                } catch (InterruptedException ie) {
                    logger.error(ie.getMessage());
                }
            }
        }

        if (!connected) {
            throw (new BLAHException("initializeConnection " + lrmsName + " error: max retry count reached. Please, be sure that remote log supplier is running at " + blParserHost + ":" + blParserPort
                    + " then restart CREAM, or use a local LRMS log."));
        }
    }

    private boolean getBLParserConnection() throws Exception {
        isNTFDATEfinished.setValue(false);
        InetSocketAddress isa = new InetSocketAddress(blParserHost, blParserPort);

        try {
            socketChannel = SocketChannel.open();
            socketChannel.connect(isa);
            Socket sc = socketChannel.socket();

            out = new PrintWriter(sc.getOutputStream(), true);
            breader = new BufferedReader(new InputStreamReader(sc.getInputStream()));

            logger.info("Connection with BLParser (" + lrmsName + ") correctly established.");

            if (sc.isConnected()) {
                if (jobIdPrefix != null) {
                    logger.debug("sending: CREAMFILTER/" + jobIdPrefix);
                    out.println("CREAMFILTER/" + jobIdPrefix);
                    out.flush();
                    String filterResult = breader.readLine();

                    logger.debug("received: " + filterResult);

                    if (filterResult.indexOf("ERROR") > 0) {
                        throw new Exception("getBLParserConnection() error");
                    }
                }

                logger.debug("sending: PARSERVERSION/");
                out.println("PARSERVERSION/");
                out.flush();

                String filterResult = breader.readLine();
                logger.debug("received: " + filterResult);

                if (filterResult == null) {
                    throw new Exception("getBLParserConnection() error: PARSERVERSION command not supported?!?");
                }

                List<String> jobList = jobDB.retrieveJobId(new int[] { JobStatus.HELD, JobStatus.IDLE, JobStatus.REALLY_RUNNING, JobStatus.RUNNING }, null, lrmsName, null);

                if (jobList.size() > 0) {
                    StringBuffer cmd = new StringBuffer("STARTNOTIFYJOBLIST/");

                    if (filterResult.endsWith("__0")) {
                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        cmd.append(dateFormat.format(getOlderJobTime().getTime())).append(";");
                    } else if (!filterResult.endsWith("__1")) {
                        throw new Exception("getBLParserConnection() error: PARSERVERSION returned a wrong parser version!");
                    }

                    for (String jobId : jobList) {
                        cmd.append(jobIdPrefix).append(jobId.substring(5, jobId.length())).append(",");
                    }

                    cmd.deleteCharAt(cmd.length() - 1);

                    jobList.clear();
                    jobList = null;

                    logger.info("sending: " + cmd);
                    out.println(cmd);
                    out.flush();
                }

                logger.info("sending: STARTNOTIFYJOBEND/");
                out.println("STARTNOTIFYJOBEND/");
                out.flush();
            }

            return true;
        } catch (IOException e) {
            throw (new Exception("Connection with remote BLParser at " + blParserHost + ":" + blParserPort + " cannot be established."));
        } catch (AlreadyConnectedException e) {
            throw (new Exception("Connection with remote BLParser at " + blParserHost + ":" + blParserPort + " already established."));
        } catch (ConnectionPendingException e) {
            throw (new Exception("Connection with remote BLParser at " + blParserHost + ":" + blParserPort + " is pending."));
        } catch (SecurityException e) {
            throw (new Exception("Connection with remote BLParser at " + blParserHost + ":" + blParserPort + " error: " + e.getMessage()));
        }
    }


/*
    private void readDataFromSocket() throws BLAHException {
        try {
            String line = null;

            while (!isInterrupted() && isConnected() && working) {
                line = breader.readLine();

                if (line == null) {
                    throw (new IOException("Connection with remote BLParser at " + blParserHost + ":" + blParserPort + " is down!"));
                }

                if (line.startsWith("NTFDATE/END")) {
                    logger.info("done!");
                    isNTFDATEfinished.setValue(true);
                } else {
                    while (!line.endsWith("]")) {
                        line += breader.readLine();
                    }

                    logger.debug(line);
                    if (line == null) {
                        throw (new IOException("Connection with remote BLParser at " + blParserHost + ":" + blParserPort + " is down!"));
                    }
                    processor.processEvent(line);
                }
                line = null;
            }
        } catch (Throwable e) {
            if (e.getMessage() != null && e.getMessage().length() > 0) {
                throw new BLAHException("readDataFromSocket exit with message \"" + e.getMessage() + "\"");
            } else {
                throw new BLAHException("readDataFromSocket end");
            }
        } finally {

            if (breader != null) {
                try {
                    breader.close();
                } catch (IOException e) {
                }
            }

            if (out != null) {
                out.close();
            }

            if (socketChannel.socket() != null) {
                try {
                    socketChannel.socket().close();
                } catch (IOException e) {
                }
            }
        }
    }
*/

    public boolean isConnected() {
        return (socketChannel != null && socketChannel.isOpen() && socketChannel.isConnected() && breader != null && !socketChannel.socket().isClosed());
    }


    public void run() {
        logger.info("BLParserClient started!");
        while (working) {
            try {
                initializeConnection(blParserRetryCount);
            } catch (BLAHException e1) {
                logger.error(e1.getMessage());
                if (e1.getMessage().indexOf("max retry count reached") > 0) {
                    this.interrupt();
                    working = false;
                }
            }
            
            String line = null;
            String event = null;

            try {
                while (working) {
                    line = breader.readLine();

                    if (line == null) {
                        throw new Exception("Connection with remote BLParser at " + blParserHost + ":" + blParserPort + " is down!");
                    }

                    logger.debug("line = " + line);
                    if (line.startsWith("NTFDATE/END")) {
                        logger.info("done!");
                        isNTFDATEfinished.setValue(true);
                    } else {
                        event += line;

                        if (line.endsWith("]")) {
                            logger.debug("event = " + event);
                            processor.processEvent(event);
                            event = null;
                        }
                    }
                    line = null;
                }
            } catch (Throwable t) {
                logger.error(t.getMessage());
            }
        }

        logger.info("BLParserclient interrupted");
    }
            
    public String getBlParserHost() {
        return blParserHost;
    }

    public void setBlParserHost(String blParserHost) {
        this.blParserHost = blParserHost;
    }

    public int getBlParserPort() {
        return blParserPort;
    }

    public void setBlParserPort(int blParserPort) {
        this.blParserPort = blParserPort;
    }

    public int getBlParserRetryCount() {
        return blParserRetryCount;
    }

    public void setBlParserRetryCount(int blParserRetryCount) {
        this.blParserRetryCount = blParserRetryCount;
    }

    public int getBlParserRetryDelay() {
        return blParserRetryDelay;
    }

    public void setBlParserRetryDelay(int blParserRetryDelay) {
        this.blParserRetryDelay = blParserRetryDelay;
    }
}

