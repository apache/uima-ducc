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
package org.apache.uima.ducc.transport.event.cli;

import java.util.Properties;

public class PropertiesHelper {

	private static void checkConflict(Properties properties, String key1, String key2) {
		if(properties != null) {
			if(properties.containsKey(key1)) {
				if(properties.containsKey(key2)) {
					throw new IllegalArgumentException("Cannot specify both "+key1+" and "+key2);
				}
			}
		}
	}

	/////
	
	public static void checkConflictDriverClasspath(Properties properties) {
		String key1 = JobSpecificationProperties.key_classpath;
		String key2 = JobSpecificationProperties.key_driver_classpath;
		checkConflict(properties, key1, key2);
		return;
	}
	
	public static String getDriverClasspath(Properties properties) {
		String value = null;
		String key1 = JobSpecificationProperties.key_classpath;
		String key2 = JobSpecificationProperties.key_driver_classpath;
		if(properties != null) {
			if(properties.containsKey(key1)) {
				value = properties.getProperty(key1);
			}
			else if(properties.containsKey(key2)) {
				value = properties.getProperty(key2);
			}
		}
		return value;
	}
	
	public static void checkConflictDriverEnvironment(Properties properties) {
		String key1 = JobSpecificationProperties.key_environment;
		String key2 = JobSpecificationProperties.key_driver_environment;
		checkConflict(properties, key1, key2);
		return;
	}
	
	public static String getDriverEnvironment(Properties properties) {
		String value = null;
		String key1 = JobSpecificationProperties.key_environment;
		String key2 = JobSpecificationProperties.key_driver_environment;
		if(properties != null) {
			if(properties.containsKey(key1)) {
				value = properties.getProperty(key1);
			}
			else if(properties.containsKey(key2)) {
				value = properties.getProperty(key2);
			}
		}
		return value;
	}
	
	public static void checkConflictDriverJvmArgs(Properties properties) {
		String key1 = JobSpecificationProperties.key_jvm_args;
		String key2 = JobSpecificationProperties.key_driver_jvm_args;
		checkConflict(properties, key1, key2);
		return;
	}
	
	public static String getDriverJvmArgs(Properties properties) {
		String value = null;
		String key1 = JobSpecificationProperties.key_jvm_args;
		String key2 = JobSpecificationProperties.key_driver_jvm_args;
		if(properties != null) {
			if(properties.containsKey(key1)) {
				value = properties.getProperty(key1);
			}
			else if(properties.containsKey(key2)) {
				value = properties.getProperty(key2);
			}
		}
		return value;
	}
	
	/////
	
	
	public static void checkConflictProccessClasspath(Properties properties) {
		String key1 = JobSpecificationProperties.key_classpath;
		String key2 = JobSpecificationProperties.key_process_classpath;
		checkConflict(properties, key1, key2);
		return;
	}
	
	public static String getProcessClasspath(Properties properties) {
		String value = null;
		String key1 = JobSpecificationProperties.key_classpath;
		String key2 = JobSpecificationProperties.key_process_classpath;
		if(properties != null) {
			if(properties.containsKey(key1)) {
				value = properties.getProperty(key1);
			}
			else if(properties.containsKey(key2)) {
				value = properties.getProperty(key2);
			}
		}
		return value;
	}
	
	public static void checkConflictProcessEnvironment(Properties properties) {
		String key1 = JobSpecificationProperties.key_environment;
		String key2 = JobSpecificationProperties.key_process_environment;
		checkConflict(properties, key1, key2);
		return;
	}
	
	public static String getProcessEnvironment(Properties properties) {
		String value = null;
		String key1 = JobSpecificationProperties.key_environment;
		String key2 = JobSpecificationProperties.key_process_environment;
		if(properties != null) {
			if(properties.containsKey(key1)) {
				value = properties.getProperty(key1);
			}
			else if(properties.containsKey(key2)) {
				value = properties.getProperty(key2);
			}
		}
		return value;
	}
	
	public static void checkConflictProcessJvmArgs(Properties properties) {
		String key1 = JobSpecificationProperties.key_jvm_args;
		String key2 = JobSpecificationProperties.key_process_jvm_args;
		checkConflict(properties, key1, key2);
		return;
	}
	
	public static String getProcessJvmArgs(Properties properties) {
		String value = null;
		String key1 = JobSpecificationProperties.key_jvm_args;
		String key2 = JobSpecificationProperties.key_process_jvm_args;
		if(properties != null) {
			if(properties.containsKey(key1)) {
				value = properties.getProperty(key1);
			}
			else if(properties.containsKey(key2)) {
				value = properties.getProperty(key2);
			}
		}
		return value;
	}
	
}
