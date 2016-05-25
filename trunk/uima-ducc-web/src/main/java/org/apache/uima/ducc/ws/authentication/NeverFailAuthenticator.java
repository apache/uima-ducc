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
package org.apache.uima.ducc.ws.authentication;

import org.apache.uima.ducc.common.authentication.AuthenticationResult;
import org.apache.uima.ducc.common.authentication.IAuthenticationManager;
import org.apache.uima.ducc.common.authentication.IAuthenticationResult;

public class NeverFailAuthenticator implements IAuthenticationManager {

	private String version = "Never Fail 1.0";
	
	@Override
	public String getVersion() {
		return version;
	}

	@Override
	public boolean isPasswordChecked() {
		return true;
	}

	@Override
	public IAuthenticationResult isAuthenticate(String userid, String domain, String password) {
		IAuthenticationResult result = new AuthenticationResult();
		result.setSuccess();
		return result;
	}

	@Override
	public IAuthenticationResult isGroupMember(String userid, String domain, Role role) {
		IAuthenticationResult result = new AuthenticationResult();
		result.setSuccess();
		return result;
	}

}
