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
package org.apache.uima.ducc.db.management;

import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;

public class DbProperties {

	private static DuccPropertiesResolver propertiesResolver = DuccPropertiesResolver.getInstance();
    
	public static final String ducc_db_home = "ducc.db.home";
    public static final String ducc_db_name = "ducc.db.name";
    public static final String ducc_db_user = "ducc.db.user";
    public static final String ducc_db_password = "ducc.db.password";
    public static final String ducc_db_driver = "ducc.db.driver";
    public static final String ducc_db_protocol = "ducc.db.protocol";
    
    public static final String ducc_db_mode = "ducc.db.mode";
    
    public static String getDbHome() {
		return propertiesResolver.getFileProperty(DbProperties.ducc_db_home);
	}
	
	public static String getDbName() {
		return propertiesResolver.getFileProperty(DbProperties.ducc_db_name);
	}
	
	public static String getDbUser() {
		return propertiesResolver.getFileProperty(DbProperties.ducc_db_user);
	}
	
	public static String getDbPassword() {
		return propertiesResolver.getFileProperty(DbProperties.ducc_db_password);
	}
	
	public static String getDbDriver() {
		return propertiesResolver.getFileProperty(DbProperties.ducc_db_driver);
	}
	
	public static String getDbProtocol() {
		return propertiesResolver.getFileProperty(DbProperties.ducc_db_protocol);
	}
	
	public static String getDbMode() {
		return propertiesResolver.getFileProperty(DbProperties.ducc_db_mode);
	}
}
