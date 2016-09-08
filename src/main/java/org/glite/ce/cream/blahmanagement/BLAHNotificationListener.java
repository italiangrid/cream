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
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

public class BLAHNotificationListener extends Thread {
    private static final Logger logger = Logger.getLogger(BLAHNotificationListener.class.getName());
    private static final int THREAD_COUNT = 10;
    private ServerSocket serverSocket = null;
    private BLAHJobStatusChangeListener jobStatusChangeListener = null;
    private boolean working = true;
    private final ThreadPoolExecutor pool = new ThreadPoolExecutor(THREAD_COUNT, THREAD_COUNT, 3, TimeUnit.SECONDS, new LinkedBlockingQueue());

    public BLAHNotificationListener(int port) {
        super("BLAHNotificationListener");
        setDaemon(true);

        try {
            serverSocket = new ServerSocket(port, 10);
            logger.info("Server listening on port " + port);
        } catch (IOException e) {
            logger.error(e.getMessage());

            pool.shutdownNow();
        }
    }

    public void startListener() {
        working = true;
        start();
    }

    public void stopListener() {
        working = false;

        pool.shutdownNow();
        pool.purge();

        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                logger.error(e);
            } finally {
                serverSocket = null;
            }
        }
    }

    public void run() {
        logger.info("started!");

        while (!isInterrupted() && working) {
            try {
                pool.execute(new Worker(serverSocket.accept()));
            } catch (IOException e) {
                logger.warn("BLAHNotificationListener interrupted: " + e.getMessage());
            }
        }

        pool.shutdownNow();

        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                logger.error(e);
            } finally {
                serverSocket = null;
            }
        }

        logger.info("terminated!");
    }

    private class Worker implements Runnable {
        private Socket clientSocket;

        public Worker(Socket s) {
            this.clientSocket = s;

            try {
                clientSocket.setKeepAlive(false);
                clientSocket.setTcpNoDelay(true);
                clientSocket.setSoLinger(true, 10);
                // clientSocket.setSoTimeout(30000);
            } catch (SocketException e) {
                logger.error(e.getMessage(), e);
            }
        }

        public void run() {
            BufferedReader rd = null;
            StringBuffer msg = new StringBuffer();

            try {
                rd = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                String str = null;

                while ((str = rd.readLine()) != null) {
                    msg.append(str);
                }

                str = null;
            } catch (Exception e) {
                logger.error(e.getMessage());
            } finally {
                try {
                    if (rd != null) {
                        rd.close();
                    }
                } catch (IOException e) {
                    logger.error(e.getMessage());
                }
            }

            try {
                clientSocket.shutdownInput();
            } catch (IOException e) {
            }

            try {
                clientSocket.shutdownOutput();
            } catch (IOException e) {
            }

            try {
                clientSocket.close();
            } catch (IOException e) {
                logger.error(e.getMessage());
            } finally {
                clientSocket = null;
            }

            rd = null;
            
            if (jobStatusChangeListener != null) {
                jobStatusChangeListener.doOnJobStatusChange(new BLAHJobStatus(msg.toString(), BLAHJobStatus.BLAH_NOTIFICATION_LISTENER));
            }
            
            msg = null;
        }
    }

    public void setJobStatusChangeListener(BLAHJobStatusChangeListener jobStatusChangeListener) {
        this.jobStatusChangeListener = jobStatusChangeListener;
    }

    public BLAHJobStatusChangeListener getJobStatusChangeListener() {
        return jobStatusChangeListener;
    }
}
