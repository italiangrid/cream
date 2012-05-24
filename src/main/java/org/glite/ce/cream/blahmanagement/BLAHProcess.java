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
 * Authors: L. Zangrando <zangrando@pd.infn.it>
 *
 */

package org.glite.ce.cream.blahmanagement;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

public class BLAHProcess extends ThreadLocal<Process> {
    private static final Logger logger = Logger.getLogger(BLAHProcess.class.getName());
    private static final int DELAY = 1000;
    private static final String commandResultAsString = (new BLAHCommand(BLAHCommand.RESULTS)).toString();
    private Runtime rt = Runtime.getRuntime();
    private String executablePath = null;
    private String scratchDirPath = null;
    private int commandTimeout = 300000; // 5 min
    private boolean asynchronousMode = false;
    private List<BLAHCommand> pendingCommandList; 

    protected BLAHProcess(String executablePath, String scratchDirPath, int commandTimeout) {
        this.executablePath = executablePath;
        this.scratchDirPath = scratchDirPath;
        this.commandTimeout = commandTimeout;
    }

    protected Process initialValue() {
        return makeBLAHProcess();
    }

    private Process makeBLAHProcess() {
        try {
            Map<String, String> envMap = new HashMap<String, String>(System.getenv());
            envMap.put("TERM", "vanilla");

            String[] env = new String[envMap.size()];
            int index = 0;

            for (String key : envMap.keySet()) {
                env[index++] = key.concat("=").concat(envMap.get(key));
            }

            Process proc = rt.exec(executablePath, env, new File(scratchDirPath));

            BufferedReader readIn = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            readIn.readLine();

            logger.debug("makeBLAHProcess: made a new blah process");

            return proc;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    protected void killBLAHProcess() {
        Process proc = (Process) super.get();
        if (proc == null) {
            return;
        }

        try {
            proc.getInputStream().close();
        } catch (Exception e) {
        }

        try {
            proc.getOutputStream().close();
        } catch (Exception e) {
        }

        try {
            proc.getErrorStream().close();
        } catch (Exception e) {
        }

        try {
            proc.destroy();
        } catch (Exception e) {
        }

        logger.debug("killBLAHProcess: blah process killed");
    }

    public void execute(List<BLAHCommand> commandList) throws BLAHException {
        if (commandList == null) {
            throw new BLAHException("command not specified!");
        }

        String blahOutput = null;
        Process proc = (Process) super.get();
        BufferedReader readIn = new BufferedReader(new InputStreamReader(proc.getInputStream()));
        BufferedReader readErr = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
        BufferedWriter writeOut = new BufferedWriter(new OutputStreamWriter(proc.getOutputStream()));

        for (BLAHCommand command : commandList) {
            if (command == null) {
                continue;
            }

            logger.debug(command.toString());

            if (commandTimeout != -1) {
                Calendar timeout = Calendar.getInstance();
                timeout.add(Calendar.SECOND, commandTimeout);

                command.setTimeout(timeout);
            }

            try {
                String cmdString = command.toString();
                writeOut.write(cmdString);
                writeOut.flush();

                if(BLAHCommand.ASYNC_MODE_ON.equals(command.getName())) {
                    asynchronousMode = true;

                    if (pendingCommandList == null) {
                        pendingCommandList = new ArrayList<BLAHCommand>(0);
                    }
                } else if(BLAHCommand.ASYNC_MODE_OFF.equals(command.getName())) {
                    asynchronousMode = false;

                    if (pendingCommandList != null) {
                        pendingCommandList.clear();
                        pendingCommandList = null;
                    }
                } else if(BLAHCommand.QUIT.equals(command.getName())) {
                    logger.debug("quit");
                    break;
                }

                if (readErr.ready()) {
                    logger.warn(Calendar.getInstance().getTime() + " - BLAH stderr: " + getOutput(readErr, command));
                }

                blahOutput = getOutput(readIn, command);

                if (blahOutput.startsWith("E")) {
                    command.setException(new BLAHException("blah error: " + blahOutput.replaceAll("\\\\", "")));

                } else if (blahOutput.startsWith("F")) {
                    command.setException(new BLAHException("unknown error occurred!"));

                } else if (blahOutput.equalsIgnoreCase("S") && command.getReqId() != null) {
                    if (asynchronousMode) {
                        pendingCommandList.add(command);
                    } else {
                        boolean done = false;

                        while (!done) {
                            writeOut.write(commandResultAsString);
                            writeOut.flush();

                            // STDERR
                            if (readErr.ready()) {
                                logger.warn("BLAH stderr: " + getOutput(readErr, command));
                            }

                            blahOutput = getOutput(readIn, command);
                            if (blahOutput == null) {
                                wait(DELAY);
                            } else {
                                int size = getResultSize(blahOutput);

                                if (size > 0) {
                                    for (int i=0; i < getResultSize(blahOutput); i++) {
                                        blahOutput = getOutput(readIn, command);
                                        parseBLAHOutput(blahOutput, command);
                                    }

                                    done = true;
                                }
                            }
                        }
                    }
                }
            } catch (BLAHException ex) {
                command.setException(ex);
            } catch (Exception e) {
                if (e.getMessage().equals("Broken pipe")) {
                    logger.warn("sendCommand: caught a \"Broken pipe\" exception (maybe the blah process is dead!) I am going to make a new one");

                    super.set(makeBLAHProcess());
                } else {
                    logger.error(e.getMessage(), e);
                    command.setException(new BLAHException(e.getMessage()));
                }
            }

        }
    }

    private int getResultSize(String blahOutput) {
        if (blahOutput == null) {
            return -1;
        }

        int size = 0;
        StringTokenizer strtok = new StringTokenizer(blahOutput);
        if (strtok.countTokens() == 2 && strtok.nextToken().equals("S")) {
            try {
                size = Integer.parseInt(strtok.nextToken());
            } catch (NumberFormatException e) {
                return -1;
            }
        }

        return size;
    }

    private void parseBLAHOutput(String blahOutput, BLAHCommand command) throws BLAHException {
        if (blahOutput == null) {
            throw new BLAHException("blahOutput not specified!");
        }
        
        if (command == null) {
            throw new BLAHException("command not specified!");
        }

        int index = blahOutput.indexOf(" ");
        if (index > -1) {
            String reqId = blahOutput.substring(0, index);
            
            if (reqId != null && !reqId.equals(command.getReqId())) {
                throw new BLAHException("reqId mismatch!");
            }

            int subId = 0;
            blahOutput = blahOutput.substring(index + 1, blahOutput.length());

            index = reqId.indexOf(".");
            if (index > -1) {
                subId = Integer.parseInt(reqId.substring(index + 1, reqId.length()));
                reqId = reqId.substring(0, index);
            }

            index = blahOutput.indexOf(" ");
            
            String errorCode = blahOutput.substring(0, index);
            String value = blahOutput.substring(index + 1, blahOutput.length());

            if (errorCode.startsWith("0")) {
                index = value.indexOf("error");
                if (index > 0 && value.length() >= 10) {
                    value = value.substring(10);
                }

                command.addResult(value.trim());
            } else {
                command.setException(new BLAHException(value.replaceAll("\\\\", "")));
            }
        }
    }

    private void wait(int delay) {
        Object lock = new Object();

        synchronized (lock) {
            try {
                lock.wait(delay);
            } catch (Throwable e) {
            }
        }
    }

    public void waitForResult(BLAHCommand command) {
        if (!asynchronousMode) {
            return;
        }

        while (!command.isTimedOut()) {
            if (pendingCommandList.contains(command)) {
                try {
                    Thread.currentThread().sleep(DELAY);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                break;
            }
        }

        if (command.isTimedOut()) {
            command.setException(new BLAHException("command timed out!"));
            pendingCommandList.remove(command);
        }
    }
    
    private String getOutput(BufferedReader readIn, BLAHCommand command) throws BLAHException {
        try {
            while (!readIn.ready()) {
                if(command.isTimedOut()) {
                    killBLAHProcess();
                    super.set(makeBLAHProcess());
                    throw new BLAHException("command timed out!");
                }

                wait(DELAY);
            }

            String result = readIn.readLine();
            logger.debug("getBlahOutput: " + result);

            return result;
        } catch (Throwable e) {
            throw (new BLAHException("getBlahOutput error: " + e.getMessage()));
        }
    }
}
