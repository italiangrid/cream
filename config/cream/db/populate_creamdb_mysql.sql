/************ Drop: Database ***************/
DROP DATABASE IF EXISTS creamdb;

/************ Create: Database ***************/
CREATE DATABASE creamdb;

/************ Use: Database ***************/
USE creamdb;

/************ Update: Tables ***************/

/******************** Add Table: db_info ************************/
/* Build Table Structure */
CREATE TABLE db_info
(
    version       VARCHAR(5) NOT NULL,
    creationTime  TIMESTAMP  NOT NULL,
    startUpTime   TIMESTAMP  NULL,
    submissionEnabled INTEGER NOT NULL 
) ENGINE=InnoDB;

/******************** Add Table: argument ************************/

/* Build Table Structure */
CREATE TABLE argument
(
    value TEXT NOT NULL,
    jobId VARCHAR(14) NOT NULL
)  ENGINE=InnoDB;

/* Table Items: argument */

/******************** Add Table: command_status_type_description ************************/

/* Build Table Structure */
CREATE TABLE command_status_type_description
(
    type INTEGER NOT NULL,
    name VARCHAR(256) NOT NULL,
    PRIMARY KEY (type)
)  ENGINE=InnoDB;

/* Table Items: command_status_type_description */
/* ALTER TABLE command_status_type_description ADD CONSTRAINT pkcommand_status_type_description PRIMARY KEY (type); */

/******************** Add Table: environment ************************/

/* Build Table Structure */
CREATE TABLE environment
(
    name VARCHAR(256) NOT NULL,
    value TEXT NOT NULL,
    jobId VARCHAR(14) NOT NULL
)  ENGINE=InnoDB;

/* Table Items: environment */

/******************** Add Table: extra_attribute ********************/

/* Build Table Structure */
CREATE TABLE extra_attribute
(
    name VARCHAR(256) NOT NULL,
    value TEXT NOT NULL,
    jobId VARCHAR(14) NOT NULL
)  ENGINE=InnoDB;

/* Table Items: extra_attribute */

/******************** Add Table: input_file ************************/

/* Build Table Structure */
CREATE TABLE input_file
(
    value TEXT NOT NULL,
    jobId VARCHAR(14) NOT NULL
)  ENGINE=InnoDB;

/******************** Add Table: job ************************/

/* Build Table Structure */
CREATE TABLE job
(
    creamURL VARCHAR(256) NOT NULL,
    id VARCHAR(14) NOT NULL,
    cerequirements TEXT NULL,
    virtualOrganization VARCHAR(256) NULL,
    userId TEXT NOT NULL,
    batchSystem VARCHAR(256) NOT NULL,
    queue VARCHAR(256) NOT NULL,
    standardInput TEXT NULL,
    standardOutput TEXT NULL,
    standardError TEXT NULL,
    executable TEXT NOT NULL,
    delegationProxyCertPath TEXT NULL,
    authNProxyCertPath TEXT NULL,
    hlrLocation VARCHAR(256) NULL,
    loggerDestURI VARCHAR(256) NULL,
    tokenURL TEXT NULL,
    perusalFilesDestURI TEXT NULL,
    perusalListFileURI TEXT NULL,
    perusalTimeInterval INTEGER NULL,
    nodes INTEGER NULL,
    prologueArguments TEXT NULL,
    prologue TEXT NULL,
    epilogue TEXT NULL,
    epilogueArguments TEXT NULL,
    sequenceCode VARCHAR(256) NULL,
    lrmsJobId VARCHAR(256) NULL,
    lrmsAbsLayerJobId VARCHAR(256) NULL,
    gridJobId VARCHAR(256) NULL,
    iceId TEXT NULL,
    fatherJobId VARCHAR(14) NULL,
    ceId VARCHAR(256) NULL,
    type VARCHAR(256) NOT NULL,
    creamInputSandboxURI TEXT NULL,
    creamOutputSandboxURI TEXT NULL,
    sandboxBasePath TEXT NULL,
    inputSandboxBaseURI TEXT NULL,
    outputSandboxBaseDestURI TEXT NULL,
    workerNode VARCHAR(256) NULL,
    jdl TEXT NOT NULL,
    localUser VARCHAR(256) NOT NULL,
    delegationProxyId TEXT NULL,
    delegationProxyInfo TEXT NULL,
    workingDirectory TEXT NOT NULL,
    leaseId TEXT NULL,
    leaseTime TIMESTAMP NULL,
    myProxyServer TEXT NULL,
    PRIMARY KEY (id)
)  ENGINE=InnoDB;

/* Table Items: job */
/* ALTER TABLE job ADD CONSTRAINT pkjob PRIMARY KEY (id); */

/* Add Indexes for: job */
CREATE INDEX job_proxyDelegationId_Idx ON job (delegationProxyId(256));
CREATE INDEX job_userId_Idx ON job (userId(256));

/******************** Add Table: job_child ************************/

/* Build Table Structure */
CREATE TABLE job_child
(
    id VARCHAR(14) NOT NULL,
    fatherJobId VARCHAR(14) NOT NULL,
    PRIMARY KEY (id(14), fatherJobId(14))
)  ENGINE=InnoDB;

/******************** Add Table: job_command ************************/

/* Build Table Structure */
CREATE TABLE job_command
(
    id BIGINT NOT NULL AUTO_INCREMENT,
    description TEXT NULL,
    statusType INTEGER NOT NULL,
    failureReason TEXT NULL,
    jobId VARCHAR(14) NOT NULL,
    cmdExecutorName VARCHAR(256) NULL,
    userId TEXT NOT NULL,
    startSchedulingTime TIMESTAMP NULL,
    startProcessingTime TIMESTAMP NULL,
    executionCompletedTime TIMESTAMP NULL,
    creationTime TIMESTAMP NOT NULL,
    type INTEGER NOT NULL,
    PRIMARY KEY (id)
)  ENGINE=InnoDB;

/******************** Add Table: job_lease ************************/

/* Build Table Structure */
CREATE TABLE job_lease
(
    leaseId TEXT NOT NULL,
    userId TEXT NOT NULL,
    leaseTime TIMESTAMP NOT NULL,
    PRIMARY KEY (leaseId(256), userId(256))
)  ENGINE=InnoDB;

/* Table Items: job_lease */
/* ALTER TABLE job_lease ADD CONSTRAINT pkjob_lease PRIMARY KEY (leaseId(256), userId(256)); */

/******************** Add Table: command_queue ************************/
/* Build Table Structure */
CREATE TABLE command_queue
(
    id                  BIGINT NOT NULL AUTO_INCREMENT,
    commandGroupId      VARCHAR(256) NULL,
    name                VARCHAR(256) NOT NULL,
    category            VARCHAR(256) NULL,
    userId              TEXT NOT NULL,
    description         TEXT NULL,
    failureReason       TEXT NULL,
    statusType          INTEGER NOT NULL,
    creationtime        TIMESTAMP NULL DEFAULT now(),
    isScheduled         BOOL NOT NULL, 
    priorityLevel       TINYINT UNSIGNED NOT NULL DEFAULT 0,
    executionMode       CHAR(1) NOT NULL,
    PRIMARY KEY (id)
)  ENGINE=InnoDB;

/******************** Add Table: command_queue_parameter ************************/
/* Build Table Structure */
CREATE TABLE command_queue_parameter
(
    id          BIGINT NOT NULL AUTO_INCREMENT,
    commandId   BIGINT NOT NULL,
    name        VARCHAR(256) NOT NULL,
    value       TEXT NOT NULL,
    PRIMARY KEY (id)
)  ENGINE=InnoDB;


/******************** Add Table: job_status ************************/

/* Build Table Structure */
CREATE TABLE job_status
(
    type INTEGER NOT NULL,
    exitCode VARCHAR(256) NULL,
    failureReason TEXT NULL,
    description TEXT NULL,
    time_stamp TIMESTAMP NOT NULL,
    jobId VARCHAR(14) NOT NULL,
    id BIGINT NOT NULL AUTO_INCREMENT,
    PRIMARY KEY (id)
)  ENGINE=InnoDB;

/******************** Add Table: job_status_type_description ************************/

/* Build Table Structure */
CREATE TABLE job_status_type_description
(
    type INTEGER NOT NULL,
    name VARCHAR(256) NOT NULL,
    PRIMARY KEY (type)
)  ENGINE=InnoDB;

/* Table Items: job_status_type_description */
/*ALTER TABLE job_status_type_description ADD CONSTRAINT pkjob_status_type_description PRIMARY KEY (type); */

/******************** Add Table: output_file ************************/

/* Build Table Structure */
CREATE TABLE output_file
(
    value TEXT NOT NULL,
    jobId VARCHAR(14) NOT NULL
) ENGINE=InnoDB;

/******************** Add Table: output_sandbox_dest_URI ************************/

/* Build Table Structure */
CREATE TABLE output_sandbox_dest_URI
(
    value TEXT NOT NULL,
    jobId VARCHAR(14) NOT NULL
)  ENGINE=InnoDB;

/************ Add Foreign Keys to Database ***************/


/************ Foreign Key: fk_argument_jobId_job_id ***************/
ALTER TABLE argument ADD CONSTRAINT fk_argument_jobId_job_id
    FOREIGN KEY (jobId) REFERENCES job (id) ON UPDATE CASCADE ON DELETE CASCADE;

/************ Foreign Key: fk_extra_attribute_jobId_job_id ***************/
ALTER TABLE extra_attribute ADD CONSTRAINT fk_extra_attribute_jobId_job_id
    FOREIGN KEY (jobId) REFERENCES job (id) ON UPDATE CASCADE ON DELETE CASCADE;

/************ Foreign Key: fk_environment_jobId_job_id ***************/
ALTER TABLE environment ADD CONSTRAINT fk_environment_jobId_job_id
    FOREIGN KEY (jobId) REFERENCES job (id) ON UPDATE CASCADE ON DELETE CASCADE;

/************ Foreign Key: fk_inputFile_jobId_job_id ***************/
ALTER TABLE input_file ADD CONSTRAINT fk_inputFile_jobId_job_id
    FOREIGN KEY (jobId) REFERENCES job (id) ON UPDATE CASCADE ON DELETE CASCADE;

/************ Foreign Key: fk_childJob_fatherJobId_job_id ***************/
ALTER TABLE job_child ADD CONSTRAINT fk_childJob_fatherJobId_job_id
    FOREIGN KEY (fatherJobId) REFERENCES job (id) ON UPDATE CASCADE ON DELETE CASCADE;

/************ Foreign Key: fk_job_command_jobId_job_id ***************/
ALTER TABLE job_command ADD CONSTRAINT fk_job_command_jobId_job_id
    FOREIGN KEY (jobId) REFERENCES job (id) ON UPDATE CASCADE ON DELETE CASCADE;

/************ Foreign Key: fk_command_queue_parameter_commandId_command_queue_id ***************/
ALTER TABLE command_queue_parameter ADD CONSTRAINT fk_command_queue_parameter_commandId_command_queue_id
    FOREIGN KEY (commandId) REFERENCES command_queue (id) ON UPDATE CASCADE ON DELETE CASCADE;

/************ Foreign Key: fk_jobStatus_jobId_job_id ***************/
ALTER TABLE job_status ADD CONSTRAINT fk_jobStatus_jobId_job_id
    FOREIGN KEY (jobId) REFERENCES job (id) ON UPDATE CASCADE ON DELETE CASCADE;

/************ Foreign Key: fk_jobStatus_type_jStatus_type ***************/
ALTER TABLE job_status ADD CONSTRAINT fk_jobStatus_type_jStatus_type
    FOREIGN KEY (type) REFERENCES job_status_type_description (type) ON UPDATE CASCADE ON DELETE CASCADE;

/************ Foreign Key: fk_outputFile_jobId_job_id ***************/
ALTER TABLE output_file ADD CONSTRAINT fk_outputFile_jobId_job_id
    FOREIGN KEY (jobId) REFERENCES job (id) ON UPDATE CASCADE ON DELETE CASCADE;

/************ Foreign Key: fk_outputSandboxDestURI_jobId_job_id ***************/
ALTER TABLE output_sandbox_dest_URI ADD CONSTRAINT fk_outputSandboxDestURI_jobId_job_id
    FOREIGN KEY (jobId) REFERENCES job (id) ON UPDATE CASCADE ON DELETE CASCADE;
    
/************ Insert values in command_status_type_description ***************/    
insert into command_status_type_description (type, name) values (0, 'CREATED');
insert into command_status_type_description (type, name) values (1, 'QUEUED');
insert into command_status_type_description (type, name) values (2, 'SCHEDULED');
insert into command_status_type_description (type, name) values (3, 'RESCHEDULED');
insert into command_status_type_description (type, name) values (4, 'PROCESSING');
insert into command_status_type_description (type, name) values (5, 'REMOVED');
insert into command_status_type_description (type, name) values (6, 'SUCCESSFULL');
insert into command_status_type_description (type, name) values (7, 'ERROR');

/************ Insert values in status_type_description ***************/    
insert into job_status_type_description (type, name) values (0, 'REGISTERED');
insert into job_status_type_description (type, name) values (1, 'PENDING');
insert into job_status_type_description (type, name) values (2, 'IDLE');
insert into job_status_type_description (type, name) values (3, 'RUNNING');
insert into job_status_type_description (type, name) values (4, 'REALLY_RUNNING');
insert into job_status_type_description (type, name) values (5, 'CANCELLED');
insert into job_status_type_description (type, name) values (6, 'HELD');
insert into job_status_type_description (type, name) values (7, 'DONE_OK');
insert into job_status_type_description (type, name) values (8, 'DONE_FAILED');
insert into job_status_type_description (type, name) values (9, 'PURGED');
insert into job_status_type_description (type, name) values (10, 'ABORTED');

/************ Insert values in db_info ***************/    
insert into db_info (version, creationTime, startUpTime, submissionEnabled) values ('2.6', now(), null, 0);

commit;   
