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
package org.apache.uima.ducc.common.authentication;

public class AuthenticationManager implements IAuthenticationManager {
	
	private static IAuthenticationManager instance = new AuthenticationManager();
	
	private final String version = "ducc common 1.0 (default)";
	
	public static IAuthenticationManager getInstance() {
		return instance;
	}
	
	/**
	 * Indicate version is default
	 */
	public String getVersion() {
		return version;
	}
	
	/**
	 * Indicate no notes
	 */
	public String getNotes(String userid) {
		return null;
	}
	
	/**
	 * Indicate that password is not checked: false
	 */
	public boolean isPasswordChecked() {
		return false;
	}
	
	/**
	 * Indicate that authentication is successful
	 */
	public IAuthenticationResult isAuthenticate(String userid, String domain, String password) {
		AuthenticationResult authenticationResult = new AuthenticationResult();
		authenticationResult.setSuccess();
		return authenticationResult;
	}

	/**
	 * Indicate that role is permitted
	 */
	public IAuthenticationResult isGroupMember(String userid, String domain, Role role) {
		AuthenticationResult authenticationResult = new AuthenticationResult();
		authenticationResult.setSuccess();
		return authenticationResult;
	}

}
