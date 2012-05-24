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
 * Authors: Eric Frizziero <eric.frizziero@pd.infn.it>
 * 
 */

package org.glite.ce.cream.client;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
//import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import javax.naming.NamingException;
import javax.sql.DataSource;

//import org.apache.log4j.BasicConfigurator;
//import org.apache.log4j.Logger;
import org.glite.ce.cream.client.CmdLineParser;
import org.glite.ce.cream.configuration.CommandExecutorConfig;
import org.glite.ce.cream.configuration.ServiceConfig;
import org.glite.ce.cream.jobmanagement.db.table.ArgumentTable;
import org.glite.ce.cream.jobmanagement.db.table.EnviromentTable;
import org.glite.ce.cream.jobmanagement.db.table.ExtraAttributeTable;
import org.glite.ce.cream.jobmanagement.db.table.InputFileTable;
import org.glite.ce.cream.jobmanagement.db.table.JobChildTable;
import org.glite.ce.cream.jobmanagement.db.table.JobCommandTable;
import org.glite.ce.cream.jobmanagement.db.table.JobStatusTable;
import org.glite.ce.cream.jobmanagement.db.table.JobTable;
import org.glite.ce.cream.jobmanagement.db.table.JobTableJobStatusTable;
import org.glite.ce.cream.jobmanagement.db.table.OutputFileTable;
import org.glite.ce.cream.jobmanagement.db.table.OutputSandboxDestURITable;
import org.glite.ce.creamapi.cmdmanagement.CommandException;
import org.glite.ce.creamapi.cmdmanagement.Parameter;
import org.glite.ce.creamapi.jobmanagement.Job;
import org.glite.ce.creamapi.jobmanagement.JobStatus;
import org.glite.ce.creamapi.jobmanagement.db.table.ArgumentTableInterface;
import org.glite.ce.creamapi.jobmanagement.db.table.EnviromentTableInterface;
import org.glite.ce.creamapi.jobmanagement.db.table.ExtraAttributeTableInterface;
import org.glite.ce.creamapi.jobmanagement.db.table.InputFileTableInterface;
import org.glite.ce.creamapi.jobmanagement.db.table.JobChildTableInterface;
import org.glite.ce.creamapi.jobmanagement.db.table.JobCommandTableInterface;
import org.glite.ce.creamapi.jobmanagement.db.table.JobStatusTableInterface;
import org.glite.ce.creamapi.jobmanagement.db.table.JobTableInterface;
import org.glite.ce.creamapi.jobmanagement.db.table.JobTableJobStatusInterface;
import org.glite.ce.creamapi.jobmanagement.db.table.OutputFileTableInterface;
import org.glite.ce.creamapi.jobmanagement.db.table.OutputSandboxDestURITableInterface;

public class JobDBAdminPurger {
   // public static final String NAMEDB       = "nameDB";
   // public static final String USERDB       = "userDB";
   // public static final String PSWDB        = "pswDB";
    public static final String STATUS       = "status";
    public static final String JOB_IDS      = "jobIds";
    public static final String CONF_PATH    = "conf";
    public static final String FILE_JOB_IDS = "fileJobIds";
    
    //public static final String CREAMDB_DEFAULT = "creamdb";
    
    //private final static Logger logger = Logger.getLogger(JobDBAdminPurger.class.getName());
    private JobTableInterface jobTable = null;
    private JobTableJobStatusInterface jobTableJobStatusTable = null;
    private EnviromentTableInterface enviromentTable = null;
    private ExtraAttributeTableInterface extraAttributeTable = null;
    private ArgumentTableInterface argumentTable = null;
    private JobChildTableInterface jobChildTable = null;
    private InputFileTableInterface inputFileTable = null;
    private OutputFileTableInterface outputFileTable = null;
    private OutputSandboxDestURITableInterface outputSandboxDestURITable = null;
    private JobCommandTableInterface jobCommandTable = null;
    private JobStatusTableInterface jobStatusTable = null;
    
    private Hashtable<String, Integer> statusTable = null;
    
    private Connection connection = null;
    private String userId = null; //admin 
    private String creamPurgeSandboxBinPath = null;
    
    private List<String> options = null;
    private List<String> jobIdL = new ArrayList<String>(0);
    private int[] jobStatusType = null;
    private int[] jobStatusTimestamp = null;
    private String confPathFromCommandLine = null;
    //private String creamDB = null;
    //private String userDB = null;
    //private String pswDB = null;
    private String fileNameJobIds = null;
    
    
    public JobDBAdminPurger(String[] args) throws IllegalArgumentException, CommandException {
      //BasicConfigurator.configure();
      options = new ArrayList<String>();
      //options.add(NAMEDB);
      //options.add(USERDB);
      //options.add(PSWDB);
      options.add(STATUS);
      options.add(JOB_IDS);
      options.add(CONF_PATH);
      options.add(FILE_JOB_IDS);

      statusTable = new Hashtable<String, Integer>();
      for (int index = 0; index < JobStatus.statusName.length; index++) {
          statusTable.put(JobStatus.statusName[index].toUpperCase().trim(), index);
      }
      
      try{
      this.parseArguments(args, options);
        this.init();
        this.jobAdminPurger(jobIdL, jobStatusType);
      } catch (Exception ne){
          throw new CommandException(ne.getMessage());
      } finally{
          this.destroy();
      }
    }
    
    public void jobAdminPurger(List<String> jobIdList, int[] jobStatusType) throws CommandException {
        //logger.info("Begin jobAdminPurger");
        System.out.println("START jobAdminPurger");
        
        List<String> jobIdListToCancel = new ArrayList<String>(0);
        try{
            jobTable = new JobTable();
            jobTableJobStatusTable    = new JobTableJobStatusTable();
            enviromentTable           = new EnviromentTable();
            extraAttributeTable       = new ExtraAttributeTable();
            argumentTable             = new ArgumentTable();
            jobChildTable             = new JobChildTable();
            inputFileTable            = new InputFileTable();
            outputFileTable           = new OutputFileTable();
            outputSandboxDestURITable = new OutputSandboxDestURITable();
            jobCommandTable           = new JobCommandTable();
            jobStatusTable            = new JobStatusTable();
        } catch (SQLException de){
            throw new CommandException(de.getMessage());
        }
        Calendar endStatusDate   = null;
        
        if (jobStatusType != null) {
            List<String> jobIdListQuery = null;
            for (int index = 0; index < jobStatusType.length; index++) {
              try{
                endStatusDate = null;
                if (jobStatusTimestamp[index] != 0){
                    endStatusDate = GregorianCalendar.getInstance();
                    endStatusDate.add(Calendar.DAY_OF_MONTH, - jobStatusTimestamp[index]);
                }
                jobIdListQuery = jobTableJobStatusTable.executeSelectToRetrieveJobId(userId, jobIdList, null, new int[] {jobStatusType[index]}, null, null, endStatusDate, null, null, connection); 
                if (jobIdListQuery != null) {
                jobIdListToCancel.addAll(jobIdListQuery);
                }
              } catch (SQLException de){
                 //logger.error("Problem to retrieve jobIds from cream database!");
                 throw new CommandException("Problem to retrieve jobIds from cream database! " + de.getMessage());
               }
            }
        } else {
            jobIdListToCancel = jobIdList;
        }
        
        if ((jobIdListToCancel != null) && (jobIdListToCancel.size() > 0)){
            String jobId = null;
            Job job = null;
            for (Iterator<String> i = jobIdListToCancel.iterator(); i.hasNext();){
                jobId = i.next();
                System.out.println("-----------------------------------------------------------");
                //logger.info("Job " + jobId + " is going to be purged ...");
                System.out.println("Job " + jobId + " is going to be purged ...");
                try{
                    try{
                     job = jobTable.executeSelectJobTable(jobId, userId, connection);
                    } catch (SQLException de){
                        throw new CommandException(de.getMessage());
                    }
                  this.purge(job);
                  //logger.info(jobId + " has been purged!");
                  System.out.println(jobId + " has been purged!");
                } catch (CommandException ce){
                    //logger.error("Job " + jobId + ": ERROR -> " + ce.getMessage());
                    System.err.println("Job " + jobId + ": ERROR -> " + ce.getMessage());
                }
                System.out.println("-----------------------------------------------------------");
                System.out.println();
            }
        } else {
            System.out.println("No jobs to purge!");
        }
        System.out.println("STOP jobAdminPurger");
        //logger.info("End jobAdminPurger");
    }
/*
    private Connection getConnection(String userDB, String pswDB, String nameDB) throws Exception{
        Connection connection = null;
        Class.forName("org.gjt.mm.mysql.Driver");
        connection = DriverManager.getConnection("jdbc:mysql://localhost/" + nameDB + "?user=" + userDB + "&password=" + pswDB);
        connection.setAutoCommit(false);
        return connection;
    }
*/  

    //build jobIdL and jobStatusType object.
    private void parseArguments(String[] args, List<String> options) throws IllegalArgumentException{
        if ((args == null) || (args.length == 0)){
            this.printUsage();
        }

        CmdLineParser parser = new CmdLineParser();
        CmdLineParser.Option getHelpOpt = parser.addBooleanOption('h', "help");
        CmdLineParser.Option confPathOpt = null;
        //CmdLineParser.Option creamDBOpt = null;
        //CmdLineParser.Option userDBOpt = null;
        //CmdLineParser.Option pswDBOpt = null;
        CmdLineParser.Option jobIdOpt = null;
        CmdLineParser.Option fileNameJobIdsOpt = null;
        CmdLineParser.Option jobStatusOpt = null;

        if (options.contains(CONF_PATH)) {
            confPathOpt = parser.addStringOption('c', "conf");
        }
/*
        if (options.contains(NAMEDB)) {
            creamDBOpt = parser.addStringOption('d', "nameDB");
        }

        if (options.contains(USERDB)) {
            userDBOpt = parser.addStringOption('u', "userDB");
        }

        if (options.contains(PSWDB)) {
            pswDBOpt = parser.addStringOption('p', "pswDB");
        }
*/
        if (options.contains(JOB_IDS)) {
            jobIdOpt = parser.addStringOption('j', "jobIds");
        }
        
        if (options.contains(FILE_JOB_IDS)) {
            fileNameJobIdsOpt = parser.addStringOption('f', "file");
        }

        if (options.contains(STATUS)) {
            jobStatusOpt = parser.addStringOption('s', "status");
        }

        try {
            parser.parse(args);
        } catch (CmdLineParser.OptionException e) {
            System.err.println(e.getMessage());
            printUsage();
        }

        Boolean getHelp = (Boolean) parser.getOptionValue(getHelpOpt, Boolean.FALSE);

        if (getHelp.booleanValue()) {
            printUsage();
        }

        if (confPathOpt != null) {
            confPathFromCommandLine = (String)parser.getOptionValue(confPathOpt, null);
        }
  /*      
        if (creamDBOpt != null) {
            creamDB = (String)parser.getOptionValue(creamDBOpt, null);
        } 
        if(creamDB == null) {
          creamDB = CREAMDB_DEFAULT;
        }

        if (userDBOpt != null) {
            userDB = (String)parser.getOptionValue(userDBOpt, null);
        } 
        if(userDB == null) {
          throw new IllegalArgumentException("userDB must be specified!");
        }
        
        if (pswDBOpt != null) {
            pswDB = (String)parser.getOptionValue(pswDBOpt, null);
        } 
        if(pswDB == null) {
          throw new IllegalArgumentException("pswDB must be specified!");
        }
    */    
        if (jobIdOpt != null) {
            String jobIds = (String)parser.getOptionValue(jobIdOpt, null);
            if(jobIds != null) {
                StringTokenizer st = new StringTokenizer(jobIds, ":");
                if (st.countTokens() > 0) {
                    while (st.hasMoreTokens()) {
                        jobIdL.add(st.nextToken());
                    }
                }
            }
        }
        
        if (fileNameJobIdsOpt != null) {
            fileNameJobIds = (String)parser.getOptionValue(fileNameJobIdsOpt, null);
            if ((fileNameJobIds != null) && (jobIdL.size() == 0)){
              jobIdL = readFileJobIds(fileNameJobIds);
            }
        } 
        
        if (jobStatusOpt != null) {
            String statuses = (String)parser.getOptionValue(jobStatusOpt, null);
            if(statuses != null) {
                StringTokenizer st = new StringTokenizer(statuses, ":");

                if (st.countTokens() > 0) {
                    jobStatusType = new int[st.countTokens()];
                    jobStatusTimestamp = new int[st.countTokens()];
                    int index = 0;

                    String status = null;
                    Integer statusInt = 0;
                    String statusTimestamp = null;
                    while (st.hasMoreTokens()) {
                      status = st.nextToken();
                      if (status.indexOf(",") != -1) {
                        statusTimestamp = status.substring(status.indexOf(",")+1);
                        status = status.substring(0,status.indexOf(","));
                        try{
                          jobStatusTimestamp[index] = Integer.parseInt(statusTimestamp);                          
                        } catch (NumberFormatException nfe){
                            System.err.println("StatusTimestamp must be integer!");
                            throw new IllegalArgumentException("StatusTimestamp must be integer!");
                        }
                      } else {
                        jobStatusTimestamp[index] = 0;
                      }
                      status = status.toUpperCase().trim();
                      statusInt = statusTable.get(status);
                      if (statusInt != null){
                         jobStatusType[index++] = statusInt;
                      } else {
                          throw new IllegalArgumentException("StatusType " + status + " unknown!");
                      }
                    }
                }
            }
        }
    }
    
    private List<String> readFileJobIds(String fileNameJobIds) throws IllegalArgumentException{
        List<String> jobIdLFromFile = new ArrayList<String>(0);
        FileReader fr = null;
        BufferedReader br = null;
        try {
          fr = new FileReader(fileNameJobIds);
          br = new BufferedReader(fr);
        
          String jobId = br.readLine();
          while (jobId != null){
            jobIdLFromFile.add(jobId.trim());
            jobId = br.readLine();
          }
        } catch (FileNotFoundException fnfe){
            throw new IllegalArgumentException("The " + fileNameJobIds + " file doesn't exist");
        } catch (IOException ioe){
            throw new IllegalArgumentException("Error reading the " + fileNameJobIds + " file.");
        } finally{
          if (br !=null) {
              try{
                br.close();
              } catch (IOException ioe){
                 //do nothing.   
              }
          }
        }
        
        return jobIdLFromFile;
    }
    
    private void printUsage() {
        System.err.println("JobDBAdminPurger\n\n");
        //System.err.println("Usage: JobDBAdminPurger.sh  [-c|--conf CREAMConfPath] [-d|--nameDB creamDB] -u|--userDB userDB -p|--pswDB pswDB [-j|--jobIds jobId1:jobId2:...] | [-f|--filejobIds filenameJobIds] | [-s|--status statusType0,deltaTime:statusType1:...] [-h|--help]");
        System.err.println("Usage: JobDBAdminPurger.sh  [-c|--conf CREAMConfPath] [-j|--jobIds jobId1:jobId2:...] | [-f|--filejobIds filenameJobIds] | [-s|--status statusType0,deltaTime:statusType1:...] [-h|--help]");
        System.err.println(); 
        System.err.println("Examples:");
        System.err.println("JobDBAdminPurger.sh  -j CREAM217901296:CREAM324901232");  
        System.err.println("JobDBAdminPurger.sh  -s registered:pending:idle");
        System.err.println("JobDBAdminPurger.sh  -s registered,3:pending:idle,5");
        System.err.println("JobDBAdminPurger.sh  -c /etc/glite-ce-cream/cream-config.xml --status registered:idle");
        System.err.println("JobDBAdminPurger.sh  --jobIds CREAM217901296:CREAM324901232");
        System.err.println("JobDBAdminPurger.sh  -f /tmp/jobIdsToPurge.txt");
        System.err.println("where for example jobIdsToPurge.txt contains:");
        System.err.println("CREAM217901296");
        System.err.println("CREAM324901232");
        System.err.println();           
        System.err.println();
        System.err.println("Status types:");
        for (int i=0; i<JobStatus.statusName.length; i++){
            //System.err.println(i + " --> " + JobStatus.statusName[i]);
            System.err.println(JobStatus.statusName[i]);
        }
        System.err.println(); 
        System.exit(-1);
    }        
    
    private void init() throws Exception {
        //String confProvider = "org.glite.ce.cream.configuration.basic.CREAMConfigContextFactory";
        String confPath = "/etc/glite-ce-cream/cream-config.xml";
        if (confPathFromCommandLine != null){
            confPath = confPathFromCommandLine;
        }

        System.setProperty("cream.configuration.path", confPath);
        ServiceConfig serviceConfiguration = ServiceConfig.getConfiguration();
        
        if (serviceConfiguration == null){
            throw new NamingException("Problem to retrieve service configuration!");
        }

        Iterator<CommandExecutorConfig> executorIterator = serviceConfiguration.getCommandExecutorList().iterator();
        boolean executorFound = false;
        CommandExecutorConfig executor = null;
        while ((executorIterator.hasNext()) && (!executorFound)) {
            executor = executorIterator.next();
            if ("BLAH executor".equals(executor.getName())) {
                executorFound = true;
                for (Parameter parameter : executor.getParameters()) {
                    if ("CREAM_PURGE_SANDBOX_BIN_PATH".equals(parameter.getName())) {
                        creamPurgeSandboxBinPath = parameter.getValueAsString();
                        break;
                    } 
                }
            }
        }

        //connection = this.getConnection(userDB, pswDB, creamDB);
        DataSource dataSource = serviceConfiguration.getDataSources().get("datasource_creamdb");
        if (dataSource != null) {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
        } else {
            throw new Exception("Datasource for the creamdb database not specified in the configuration file " + confPath);
        }
    }
    
    private void purge(Job job) throws CommandException, IllegalArgumentException {
        if (job == null) {
            throw new CommandException("job not found!");
        }
        String workingDir = job.getWorkingDirectory();
        
        if (!isEmptyField(workingDir)) {
           if (creamPurgeSandboxBinPath == null) {
             throw new CommandException("CREAM_PURGE_SANDBOX_BIN_PATH parameter not found!");
           }

           Process proc = null;
           try {
             String[] cmd = new String[] { creamPurgeSandboxBinPath, workingDir };
             proc = Runtime.getRuntime().exec(cmd);
           } catch (Throwable e) {
             System.err.println(e.getMessage());
             //logger.error(e.getMessage());
           } finally {
             if (proc != null) {
               try {
                 proc.waitFor();
               } catch (InterruptedException e) {
               }
                    
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
                   System.err.println(ioe.getMessage());
                   //logger.error(ioe.getMessage());
                 } finally {
                     try {
                       readErr.close();
                     } catch (IOException ioe) {}
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
                   throw new CommandException(errorMessage.toString());
                }
               }
             }
           }
           try {
            //user= null (admin)
            this.deleteJob(job.getId(), connection); 
           } catch (SQLException e) {
               throw new CommandException(e.getMessage());
           }
    }
    
    private boolean isEmptyField(String field) {
        return (field == null || Job.NOT_AVAILABLE_VALUE.equals(field) || field.length() == 0);        
    }

    private void deleteJob(String jobId, Connection connection) throws SQLException {
        try {
            enviromentTable.executeDelete(jobId, connection);
            extraAttributeTable.executeDelete(jobId, connection);
            argumentTable.executeDelete(jobId, connection);
            jobChildTable.executeDelete(jobId, connection);
            inputFileTable.executeDelete(jobId, connection);
            outputFileTable.executeDelete(jobId, connection);
            outputSandboxDestURITable.executeDelete(jobId, connection);
            jobCommandTable.executeDelete(jobId, connection);
            jobStatusTable.executeDelete(jobId, connection);
            jobTable.executeDelete(jobId, connection);
            connection.commit();
            //logger.debug("job deleted");
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException sqle) {
                throw new SQLException("Rollback is fault. ERRORCODE = " + sqle.getErrorCode() + "Message =  " + sqle.getMessage());
            }
            throw new SQLException("Rollback executed due to: " + e.getMessage());
        } finally {
            
        }
    }
    
    private void destroy(){
        if (connection != null) {
          try {
             connection.close();
          } catch (SQLException sqle) {
           //logger.error(sqle);
          }
        }
    }
    
    public static void main (String[] args) throws IllegalArgumentException {
        try{
          JobDBAdminPurger jobDBAdminPurger = new JobDBAdminPurger(args);
        } catch (CommandException ce){
            System.err.println("JobDBAdminPurger error: " + ce.getMessage());
        }
    }
}
