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


public class BLAHNotifierInfo {
    private String lrms = null;
    private String host = null;
    private int port = -1;

    public BLAHNotifierInfo(String lrms, String host, int port) {
        this.lrms = lrms;
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public String getLRMS() {
        return lrms;
    }

    public int getPort() {
        return port;
    }
}