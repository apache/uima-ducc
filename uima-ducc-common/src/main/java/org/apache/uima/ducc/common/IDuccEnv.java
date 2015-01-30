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
package org.apache.uima.ducc.common;

import java.io.File;

public interface IDuccEnv {

	public static final String DUCC_HOME = System.getProperty("DUCC_HOME");
	
	public static final String DUCC_HOME_DIR = DUCC_HOME+File.separator;
	public static final String DUCC_RESOURCES_DIR = DUCC_HOME_DIR+"resources"+File.separator;
	public static final String DUCC_PROPERTIES_FILE = DUCC_RESOURCES_DIR+"ducc.properties";
	public static final String DUCC_NODES_FILE_NAME = "ducc.nodes";
	public static final String DUCC_NODES_FILE_PATH = DUCC_RESOURCES_DIR+DUCC_NODES_FILE_NAME;
	public static final String DUCC_ADMINISTRATORS_FILE = DUCC_RESOURCES_DIR+"ducc.administrators";
	public static final String DUCC_CLASSPATH_FILE = DUCC_RESOURCES_DIR+"jobclasspath.properties";
	
	public static final String DUCC_STATE_DIR = DUCC_HOME_DIR+"state"+File.separator;
	public static final String DUCC_DAEMONS_DIR = DUCC_STATE_DIR+"daemons"+File.separator;
	public static final String DUCC_AGENTS_DIR = DUCC_STATE_DIR+"agents"+File.separator;
	
	public static final String DUCC_HISTORY_DIR = DUCC_HOME_DIR+"history"+File.separator;
	public static final String DUCC_HISTORY_JOBS_DIR = DUCC_HOME_DIR+"history"+File.separator+"jobs"+File.separator;
	public static final String DUCC_HISTORY_RESERVATIONS_DIR = DUCC_HOME_DIR+"history"+File.separator+"reservations"+File.separator;
	public static final String DUCC_HISTORY_SERVICES_DIR = DUCC_HOME_DIR+"history"+File.separator+"services"+File.separator;

	public static final String DUCC_STATE_SERVICES_DIR = DUCC_HOME_DIR+"state"+File.separator+"services"+File.separator;
	
	public static final String DUCC_LOGS_DIR = DUCC_HOME_DIR+"logs"+File.separator;
	public static final String DUCC_LOGS_WEBSERVER_DIR = DUCC_HOME_DIR+"logs"+File.separator+"webserver"+File.separator;
}
