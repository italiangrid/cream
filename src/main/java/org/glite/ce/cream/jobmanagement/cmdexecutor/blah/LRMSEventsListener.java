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

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.io.*;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

public class LRMSEventsListener extends Thread {
    private static final Logger logger = Logger.getLogger(LRMSEventsListener.class.getName());
    private ServerSocket serverSocket = null;
    private boolean working = true;
    private LRMSEventsProcessor processor = null;
    private static final int THREAD_COUNT = 10;
    private final ThreadPoolExecutor pool = new ThreadPoolExecutor(THREAD_COUNT, THREAD_COUNT, 3, TimeUnit.SECONDS, new LinkedBlockingQueue());

    public LRMSEventsListener(LRMSEventsProcessor processor, int port) {
        super("LRMSEventsListener");
        setDaemon(true);

        this.processor = processor;

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
        while (!isInterrupted() && working) {
            try {
                pool.execute(new Worker(serverSocket.accept(), processor));
            } catch (IOException e) {
                logger.warn("LRMSEventsListener interrupted: " + e.getMessage());
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
    }

    class Worker implements Runnable {
        private Socket clientSocket;
        private LRMSEventsProcessor processor;

        public Worker(Socket s, LRMSEventsProcessor processor) {
            this.clientSocket = s;
            this.processor = processor;
            
            try {
                clientSocket.setKeepAlive(false);
                clientSocket.setTcpNoDelay(true);
                clientSocket.setSoLinger(true, 10);
                //clientSocket.setSoTimeout(30000);
            } catch (SocketException e) {
                e.printStackTrace();
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
                //logger.error(e.getMessage(), e);
            }

            try {
                clientSocket.shutdownOutput();
            } catch (IOException e) {
                //logger.error(e.getMessage(), e);
            }

            try {
                clientSocket.close();
            } catch (IOException e) {
                logger.error(e.getMessage());
            } finally {
                clientSocket = null;
            }

            rd = null;         
            processor.processEvent(msg.toString());
            msg = null;
        }
    }
}

