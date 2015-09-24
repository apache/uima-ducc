/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
*/
package org.apache.uima.ducc.common.utils;

/**
 * Version updates.  Please try to remember to update the date, reason, and who you are for changing the level.
 *
 * 2012-05-07  0.6.0  First. jrc
 * 2012-05-14  0.6.1  Updated RM share counter. jrc
 * 2012-05-23  0.6.2  Service manager is live for "implicit" services. jrc
 * 2012-05-23  0.6.3  Service manager is ready for UIMA-AS services. jrc
 * 2012-08-27  0.6.4  All coding complete modulo boogs. jrc
 * 2012-10-09  0.7.0  Refactor for org.apache.ducc and add apache copyright. jrc
 * 2012-12-03  0.7.1  Many small bug fixes, lots of web server updates. 
 *                    Initial app debug and console support.  
 *                    Initial arbitrary process support. 
 *                    Many scripting updates. jrc
 * 2013-01-02  0.7.2  AP service support, unified ping for CUSTOM and UIMA services
 * 2013-02-03  0.7.3  First floor system from Apache distro.
 * 2013-02-25  0.8.0  Second floor system from Apache distro. RM defrag, lots of SM updates,
 *                      completed AP support in WS. CLI update, API creation.
 * ...                     
 * 2015-03-13  2.0.0  [beta] improved user code classpath isolation, switch to JD-JP 
 *                    direct communication via pull-model.  ld
 */
public class Version
{
    private static final int major = 2;       // Major version
    private static final int minor = 1;       // Minor - may be API changes, or new function
    private static final int ptf   = 0;       // Fix level, fully compatible with previous, no relevent new function
    private static final String id = null;    // A short decoration, optional

    public final static String version()
    {
        StringBuffer sb = new StringBuffer();

        sb.append(Integer.toString(major));
        sb.append(".");

        sb.append(Integer.toString(minor));
        sb.append(".");

        sb.append(Integer.toString(ptf));

        if (id != null) {
            sb.append("-");
            sb.append(id);
        }

        return sb.toString();
    }
    
    /*
     * This version number is included on each CLI request and checked by DUCC.
     * Change the value when requests are changed in an incompatible way.
     */
    public final static int cliVersion = 1;

    public static void main(String[] args)
    {
        System.out.println(version());
    }

}
