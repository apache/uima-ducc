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
 * For DUCC 2.0 the following 1.x options have been modified:
 * Changed:
 *   process_DD             ->  process_descriptor_DD
 *   instance_memory_size   ->  memory_size
 * Removed:
 *   classpath_order
 *   number_of_instances	
 *   
 * Additionally since services registered under DUCC 1.x may be relying on UIMA jars that are no longer supplied,
 * their classpath will be augmented ... triggered by the deprecated classpath_order option. 
 * 
 * For DUCC 2.1.0 changed:
 *   process_thread_count   ->  process_pipeline_count
 * Deprecated messages removed -- old options will be treated as aliases
 *   classpath_order will be silently accepted and ignored 
 *   number_of_instances is now rejected
 */

public class CliFixups {
	
    static void cleanupArgs(String[] args, String className) {
        for (int i = 0; i < args.length; ++i) {
            String arg = args[i];
            if (arg.equals("--process_DD")) {
                args[i] = "--process_descriptor_DD";
                //System.out.println("CLI replaced deprecated option: " + arg + " with: " + args[i]);
            } else if (arg.equals("--instance_memory_size")) {
                args[i] = "--memory_size";
                //System.out.println("CLI replaced deprecated option: " + arg + " with: " + args[i]);
            } else if (arg.equals("--classpath_order")) {
                //System.out.println("CLI ignored deprecated option: " + arg);
                args[i] = null;
                if (++i < args.length && !args[i].startsWith("--")) args[i] = null; 
            } else if (arg.equals("--number_of_instances")) {
                // Remove --number_of_instaces only if = 1
                if (++i < args.length && args[i].trim().equals("1")) {
                  args[i] = null; 
                  args[i-1] = null;
                }
            } else if (arg.equals("--process_thread_count")) {
                args[i] = "--process_pipeline_count";
                //System.out.println("CLI replaced deprecated option: " + arg + " with: " + args[i]);
            }
        }
    }
    
    static void cleanupProps(Properties props, String className) {
      
      // >>> Removed the hack that added UIMA jars to the classpath of services registered with DUCC 1.x but 
      // >>> started with 2.0 --- had a bug and just delayed the need for the user to provide a complete classpath.
		
    	changeOption("process_DD", "process_descriptor_DD", props);
    	changeOption("instance_memory_size", "memory_size", props);
    	changeOption("classpath_order", null, props);
    	changeOption("process_thread_count", "process_pipeline_count", props);
    	// Remove number_of_instaces only if = 1 ... leave any other value to be rejected
    	String val = props.getProperty("number_of_instances");
    	if (val != null && val.trim().equals("1")) {
    	  changeOption("number_of_instances", null, props);  
    	}
    }
    
	static private void changeOption(String oldKey, String newKey, Properties props) {
		String val = props.getProperty(oldKey);
		if (val != null) {
			props.remove(oldKey);
			if (newKey == null) {
				//System.out.println("CLI ignored deprecated option: " + oldKey);
			} else {
				props.put(newKey, val);
				//System.out.println("CLI replaced deprecated option: " + oldKey + " with: " + newKey);
			}
		}
	}
    
}
