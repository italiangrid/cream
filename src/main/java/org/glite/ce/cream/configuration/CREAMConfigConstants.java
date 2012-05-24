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
 * Authors: Paolo Andreetto, <paolo.andreetto@pd.infn.it>
 *
 */

package org.glite.ce.cream.configuration;

public class CREAMConfigConstants {

    public enum ServiceAttribute {
        JOB_DIR_LABEL("cream_scratch_dir"),
        SANDBOX_DIR_LABEL("cream_sandbox_dir"),
        EXECUTOR_DIR_LABEL("command_executor_dir"),
        //EXECUTOR_DEFAULT_COMMAND_WORKER_POLL_SIZE("command_executor_default_command_worker_poll_size"),
        CREAM_DESCRIPTION_LABEL("cream_description"),
        CREAM_INTERFACE_VERSION_LABEL("cream_interface_version"),
        CREAM_SERVICE_VERSION_LABEL("cream_service_version"),
        CREAM_CONCURRENCY_LEVEL("cream_concurrency_level"),
        CREAM_COMMAND_QUEUE_SHARED("cream_command_queue_shared"),
        DELEGATION_STORAGE_LABEL("delegation_storage"),
        DELEGATION_DN_LABEL("delegation_dn"),
        DELEGATION_KEYSIZE_LABEL("delegation_key_size"),
        DELEGATION_FACTORY_LABEL("delegation_factory"),
        DELEGATION_STORAGE_DB_LABEL("delegation_database"),
        DELEGATION_PURGE_RATE_LABEL("delegation_purge_rate"),
        CEMON_URL_LABEL("cemon_url"),
        CREAMDB_DATABASE_VERSION_LABEL("creamdb_database_version"),
        DELEGATIONDB_DATABASE_VERSION_LABEL("delegationdb_database_version");

        private String attrName;

        ServiceAttribute(String name){
        	this.attrName = name;
        }

        public String toString(){
        	return attrName;
        }
    }
}

