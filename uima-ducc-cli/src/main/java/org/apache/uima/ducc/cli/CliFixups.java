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

package org.apache.uima.ducc.cli;

import java.util.Properties;

/*
 * Sites that have old code built against a pre-release version of DUCC may replace this class
 * by one that corrects any deprecated options, e.g. changing --process_environment to --environment
 * 
 * DUCC 2.0 changed --process_DD to --process_descriptor_DD to match the other descriptor options.
 */

public class CliFixups {

    static void cleanupArgs(String[] args, String className) {
        for (int i = 0; i < args.length; ++i) {
            String arg = args[i];
            if (arg.equals("--process_DD")) {
                args[i] = "--process_descriptor_DD";
                System.out.println("CLI replaced deprecated option: " + arg + " with: " + args[i]);
            }
        }
    }
    
    static void cleanupProps(Properties props, String className) {
        for (String key : props.stringPropertyNames()) {
            if (key.equals("process_DD")) {
                props.put("process_descriptor_DD", props.get(key));
                props.remove(key);
                System.out.println("CLI replaced deprecated property: " + key + " with: classpath");
            }
        }
    }
}
