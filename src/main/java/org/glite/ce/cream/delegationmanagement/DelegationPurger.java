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
 * Authors: Luigi Zangrando <zangrando@pd.infn.it>
 *
 */

package org.glite.ce.cream.delegationmanagement;

import java.util.List;

import org.apache.log4j.Logger;
import org.glite.ce.cream.cmdmanagement.CommandManager;
import org.glite.ce.creamapi.cmdmanagement.CommandException;
import org.glite.ce.creamapi.cmdmanagement.CommandManagerException;
import org.glite.ce.creamapi.delegationmanagement.Delegation;
import org.glite.ce.creamapi.delegationmanagement.DelegationCommand;

public class DelegationPurger extends Thread {
    private static final Logger logger = Logger.getLogger(DelegationPurger.class.getName());
    private static final Object objMutex = new Object();
    private static DelegationPurger delegationPurger = null;
    private boolean terminate = false;
    int rateInMinutes = 720; // 12 hours

    public static DelegationPurger getInstance() {
        if (delegationPurger == null) {
            delegationPurger = new DelegationPurger();
        }

        return delegationPurger;
    }

    private DelegationPurger() {
        super("DelegationPurger");
        this.setDaemon(true);

        start();
    }

    public void terminate() {
        logger.info("terminate invoked!");

        terminate = true;
        delegationPurger = null;

        logger.info("terminated!");
    }

    public void setRate(int rate) {
        if (rate > 0) {
            synchronized (objMutex) {
                rateInMinutes = rate;
                objMutex.notify();

                logger.info("purger rate changed to " + rateInMinutes + " min");
            }
        }
    }

    public int getRate() {
        return rateInMinutes;
    }

    public void run() {
        logger.info("BEGIN DelegationPurger");

        try {
            Thread.sleep(2000); // wait 2 sec before starting the execution
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        }

        while (!terminate) {
            try {
                List<Delegation> expiredDelegationList = DelegationManager.getInstance().getExpiredDelegations();

                for (Delegation delegation : expiredDelegationList) {
                    logger.debug("deleting the expired delegation " + delegation.toString());

                    try {
                        DelegationCommand destroyCmd = new DelegationCommand(DelegationCommand.DESTROY_DELEGATION);
                        destroyCmd.setUserId(delegation.getUserId());
                        destroyCmd.addParameter(DelegationCommand.DELEGATION_ID, delegation.getId());
                        destroyCmd.addParameter(DelegationCommand.USER_DN_RFC2253, delegation.getDN());
                        destroyCmd.addParameter(DelegationCommand.LOCAL_USER, delegation.getLocalUser());
                        destroyCmd.addParameter(DelegationCommand.LOCAL_USER_GROUP, delegation.getLocalUserGroup());

                        CommandManager.getInstance().execute(destroyCmd);

                        logger.info("deleted the expired delegation " + delegation.toString());
                    } catch (CommandException e) {
                        logger.error("DelegationPurger CommandException: " + e.getMessage());
                    } catch (CommandManagerException e) {
                        logger.error("DelegationPurger CommandManagementException: " + e.getMessage());
                    }
                }
            } catch (Throwable t) {
                logger.error(t.getMessage());
            }

            synchronized (objMutex) {
                try {
                    objMutex.wait(rateInMinutes * 60000);
                } catch (InterruptedException e) {
                    logger.warn("purger execution interrupted!");
                    terminate = true;
                }
            }
        }

        logger.info("END DelegationPurger");
    }
}
