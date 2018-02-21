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

public interface IAuthenticationManager {
	
	/**
	 * This method is expected to return AuthenticationManager implementation version information.  
	 * It is nominally displayed by the DUCC webserver on the Login/Logout pages.
	 * 
	 * Example return value: Acme Authenticator 1.0
	 * 
	 * @return The version of the AuthenticationManager implementation.
	 */
	public String getVersion();
	
	/**
	 * This method is expected to return AuthenticationManager implementation notes, if any.
	 * 
	 * Example return value: "User your intranet login id and password."
	 * 
	 * @return The implementation notes.
	 */
	public String getNotes(String userid);
	
	/**
	 * This method is expected to return password checking information.  
	 * It is nominally employed by the DUCC webserver to enable/disable password input area on the Login/Logout pages.
	 * 
	 * @return True if the AuthenticationManager implementation checks passwords; false otherwise.
	 */
	public boolean isPasswordChecked();
	
	/**
	 * This method is expected to perform authentication.
	 * It is nominally employed by the DUCC webserver for submitted Login pages.
	 * 
	 * @param userid
	 * @param domain
	 * @param password
	 * @return True if authentic userid+domain+password; false otherwise.
	 */
	public IAuthenticationResult isAuthenticate(String userid, String domain, String password);
	
	/**
	 * This method is expected to perform role validation.
	 * It is nominally employed by the DUCC webserver for submitted Login pages.
	 * 
	 * @param userid
	 * @param domain
	 * @param role
	 * @return True if authentic userid+domain+role; false otherwise.
	 */
	public IAuthenticationResult isGroupMember(String userid, String domain, Role role);
	
	/**
	 * The supported Roles
	 */
	public enum Role {
		User,
		Admin
	}
}
