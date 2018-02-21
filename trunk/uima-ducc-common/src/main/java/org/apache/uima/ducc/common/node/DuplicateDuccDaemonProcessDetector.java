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
package org.apache.uima.ducc.common.node;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.Utils;

public class DuplicateDuccDaemonProcessDetector {

    static public String checkForDuplicate() {
        String deployFlag = "ducc.deploy.components";
        String virtualFlag = "ducc.agent.virtual";
        String javaCmd = DuccPropertiesResolver.getInstance().getProperty("ducc.jvm");
        if (javaCmd == null) {
            return "ERROR - this is not a DUCC launched process (ducc.jvm missing)";
        }

        String myType = System.getProperty(deployFlag);
        if (myType == null || myType.equals("jd") || myType.equals("jp")) {
            return null;
        }
        String myPid = Utils.getPID();
        String userid = System.getProperty("user.name");
        boolean isAgent = myType.equals("agent");
        String myVirtualIp = System.getProperty(virtualFlag);

        String[] cmdLine = new String[]{"ps", "--user", userid, "-o", "pid,args", "--no-heading"};
        ProcessBuilder pb = new ProcessBuilder(cmdLine);
        pb.redirectErrorStream(true);

        try {
            Process proc = pb.start();
            BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            String line;
            String dupProcess = null;
            // "pid cmd args ...."
            while ((line = reader.readLine()) != null) {
                String[] toks = line.trim().split("\\s+");
                // Ignore this process and any non-java ones and all others once a duplicate exists
                if (toks.length < 2 || toks[0].equals(myPid) || !toks[1].equals(javaCmd) || dupProcess != null) {
                    continue;
                }
                String virtualIp = null;
                for (String tok : toks) {
                    String[] parts = tok.split("=", 2);
                    if (parts.length == 2) {
                        if (parts[0].equals("-D" + deployFlag)) {
                            if (myType.equals(parts[1])) {
                                dupProcess = line;
                            }
                        } else if (isAgent) {
                            if (parts[0].equals("-D" + virtualFlag)) {
                                virtualIp = parts[1];
                            }
                        }
                    }
                }
                // Ignore duplicate agents with different IPs, unless one is not virtual.
                if (dupProcess != null && isAgent) {
                    if (myVirtualIp != null && virtualIp != null && !virtualIp.equals(myVirtualIp)) {
                        dupProcess = null;
                    }
                }
            }
            proc.waitFor();
            reader.close();
            return dupProcess;

        } catch (Exception e) {
            return "Exception: " + e;
        }
    }

    public static void main(String[] args) {
        if ( Utils.findDuccHome() == null ) {
            System.out.println("ERROR - failed to find DUCC_HOME");
        }
        String dup = checkForDuplicate();
        if (dup == null) {
            System.out.println("Duplicate daeomon NOT found");
        } else {
            System.out.println("Duplicate daemon found: " + dup);
        }
    }
}
