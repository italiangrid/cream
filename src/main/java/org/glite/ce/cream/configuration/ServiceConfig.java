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

package org.glite.ce.cream.configuration;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.glite.ce.commonj.configuration.CommonConfigException;
import org.glite.ce.commonj.configuration.CommonServiceConfig;
import org.glite.ce.cream.configuration.CREAMConfigConstants.ServiceAttribute;

public class ServiceConfig
    extends CommonServiceConfig {

    private static Logger logger = Logger.getLogger(ServiceConfig.class.getName());

    protected static ServiceConfig serviceConfiguration = null;

    protected ServiceConfig() throws CommonConfigException {
        super();
    }

    protected String getSysPropertyName() {
        return "cream.configuration.path";
    }

    public String getStringParameter(ServiceAttribute name) {
        return getGlobalAttributeAsString(name.toString());
    }

    public int getIntParameter(ServiceAttribute name, int defValue) {
        return getGlobalAttributeAsInt(name.toString(), defValue);
    }

    public List<CommandExecutorConfig> getCommandExecutorList() {
        ArrayList<CommandExecutorConfig> result = new ArrayList<CommandExecutorConfig>(0);

        Object[] tmpArray = confManager.getConfigurationElements(CommandExecutorConfig.class);
        for (Object obj : tmpArray) {
            result.add((CommandExecutorConfig) obj);
        }

        return result;
    }

    public String getServiceDescription() {
        String result = this.getStringParameter(ServiceAttribute.CREAM_DESCRIPTION_LABEL);
        if (result == "") {
            return "CREAM2";
        }
        return result;
    }

    public String getInterfaceVersion() {
        String result = this.getStringParameter(ServiceAttribute.CREAM_INTERFACE_VERSION_LABEL);
        if (result == "") {
            return "N/A";
        }
        return result;
    }

    public String getServiceVersion() {
        String distrInfo = getDistributionInfo();
        String srvInfo = getStringParameter(ServiceAttribute.CREAM_SERVICE_VERSION_LABEL);
        if (srvInfo == "") {
            srvInfo = "N/A";
        }
        if (distrInfo != "") {
            return srvInfo + " - " + distrInfo;
        }
        return srvInfo;
    }

    public static ServiceConfig getConfiguration() {
        if (serviceConfiguration == null) {

            synchronized (ServiceConfig.class) {

                if (serviceConfiguration == null) {

                    serviceConfiguration = (ServiceConfig) CommonServiceConfig.getConfiguration();

                }
            }
        }

        return serviceConfiguration;
    }

}
