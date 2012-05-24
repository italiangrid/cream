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

package org.glite.ce.cream.configuration.xppm;

import java.io.File;
import java.util.HashMap;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import javax.xml.xpath.XPathFactoryConfigurationException;

import org.apache.log4j.Logger;
import org.glite.ce.commonj.configuration.CommonConfigException;
import org.glite.ce.commonj.configuration.xppm.ConfigurationHandler;
import org.glite.ce.commonj.configuration.xppm.ConfigurationManager;
import org.glite.ce.cream.configuration.CommandExecutorConfig;
import org.glite.ce.creamapi.cmdmanagement.Parameter;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class CommandExecutorConfigHandler
    extends ConfigurationHandler {

    private static Logger logger = Logger.getLogger(CommandExecutorConfigHandler.class.getName());

    private static final String XPATH_STRING = "/service/commandexecutor";

    private static final String ID_ATTR = "id";

    private static final String CATEGORY_ATTR = "category";

    private static final String JARNAME_ATTR = "filename";

    private static final String POOL_SIZE_ATTR = "commandworkerpoolsize";

    private static final String QUEUE_SIZE_ATTR = "commandqueuesize";

    private static final String QUEUE_SHARED_ATTR = "commandqueueshared";

    private static final String PARAMETER_TAG = "parameter";

    private static final String PARA_NAME_ATTR = "name";

    private static final String PARA_VALUE_ATTR = "value";

    private static final String POLICY_TAG = "policy";

    private XPathExpression expr;

    private HashMap<String, CommandExecutorConfig> currentMap;

    private HashMap<String, CommandExecutorConfig> tmpMap;

    public CommandExecutorConfigHandler() 
        throws XPathExpressionException, XPathFactoryConfigurationException {
        XPath xpath = ConfigurationHandler.getXPathFactory().newXPath();
        expr = xpath.compile(XPATH_STRING);

        currentMap = null;
        tmpMap = null;

    }

    public XPathExpression getXPath() {
        return expr;
    }

    public Class<?> getCategory() {
        return CommandExecutorConfig.class;
    }

    public Object[] getConfigurationElement() {
        if (currentMap == null) {
            return null;
        }

        Object[] result = new Object[currentMap.size()];
        int k = 0;
        for (CommandExecutorConfig item : currentMap.values()) {
            result[k] = item;
            k++;
        }

        return result;
    }

    public boolean process(NodeList parsedElements)
        throws CommonConfigException {

        tmpMap = new HashMap<String, CommandExecutorConfig>();

        for (int k = 0; k < parsedElements.getLength(); k++) {
            Element cmdElem = (Element) parsedElements.item(k);
            CommandExecutorConfig cmdConf = new CommandExecutorConfig();

            String id = cmdElem.getAttribute(ID_ATTR);
            cmdConf.setName(id);
            logger.debug("Parsing attributes for : " + id);
            cmdConf.setCategory(cmdElem.getAttribute(CATEGORY_ATTR));
            cmdConf.setJarFileName(cmdElem.getAttribute(JARNAME_ATTR));

            String poolSizeStr = cmdElem.getAttribute(POOL_SIZE_ATTR);
            if (poolSizeStr != "") {
                cmdConf.setCommandWorkerPoolSize(Integer.parseInt(poolSizeStr));
            }

            String queueSizeStr = cmdElem.getAttribute(QUEUE_SIZE_ATTR);
            if (queueSizeStr != "") {
                cmdConf.setCommandQueueSize(Integer.parseInt(queueSizeStr));
            }

            String queueSharedStr = cmdElem.getAttribute(QUEUE_SHARED_ATTR);
            if (queueSharedStr.equalsIgnoreCase("true")) {
                cmdConf.setCommandQueueShared(true);
            }

            NodeList paraElemList = cmdElem.getElementsByTagName(PARAMETER_TAG);
            for (int j = 0; j < paraElemList.getLength(); j++) {
                Element paraElement = (Element) paraElemList.item(j);

                String paraName = paraElement.getAttribute(PARA_NAME_ATTR);
                String paraValue = paraElement.getAttribute(PARA_VALUE_ATTR);
                if (paraName != "") {
                    cmdConf.addParameter(new Parameter(paraName, paraValue));
                    logger.debug("Parameter " + id + "[" + paraName + "] = " + paraValue);
                }
            }

            /*
             * TODO missing definition for policies
             */

            tmpMap.put(id, cmdConf);

        }

        return !tmpMap.equals(currentMap);

    }

    public boolean processTriggers()
        throws CommonConfigException {
        return false;
    }

    public void commit() {
        currentMap = tmpMap;
        tmpMap = null;
    }

    public void rollback() {
        tmpMap = null;
    }

    public File[] getTriggers() {
        return null;
    }

    public void clean() {

    }

    public static void main(String[] args) {

        org.apache.log4j.PropertyConfigurator.configure("/tmp/log4j.properties");

        //String[] pNameList = { CommandExecutorConfigHandler.class.getName() };
        try {
            //ConfigurationManager cMan = new ConfigurationManager(args[0], pNameList);
            ConfigurationManager cMan = new ConfigurationManager(args[0]);
            Object[] tmpArray = cMan.getConfigurationElements(CommandExecutorConfig.class);
            if (tmpArray == null) {
                logger.debug("Missing array");
                return;
            }

            logger.info("Found executors: " + tmpArray.length);

            for (Object obj : tmpArray) {
                CommandExecutorConfig item = (CommandExecutorConfig) obj;
                logger.debug("Found " + item.getName());
                for (Parameter param : item.getParameters()) {
                    logger.debug("Parameter " + item.getName() + "[" + param.getName() + "] = " + param.getValue());
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error(ex.getMessage(), ex);
        }
    }
}
