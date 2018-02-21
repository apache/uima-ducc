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

public class LinuxAuthenticationManager extends AbstractAuthenticator {
	
	private static IAuthenticationManager instance = new LinuxAuthenticationManager();
	
	private String version = "ducc linux 1.0";

	public static IAuthenticationManager getInstance() {
		return instance;
	}
	
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
		IAuthenticationResult ar = new AuthenticationResult(IAuthenticationResult.SUCCESS);
		try {
			ar = checkUserExcluded(userid);
			if(ar.isSuccess()) {
				ar = checkUserNotIncluded(userid);
				if(ar.isSuccess()) {
					String[] args = { userid, password };
					UserAuthenticate instance = new UserAuthenticate();
					String result = instance.launch(args);
					// success groups = [group1, group2]
					if(result.startsWith("success")) {
						result = result.trim();
						result = result.replace("success groups =", "");
						result = result.replace("[", "");
						result = result.replace("]", "");
						result = result.replace(" ", "");
						String[] groups = result.split(",");
						if(groups != null) {
							userGroupsCache.put(userid, groups);
						}
						else {
							userGroupsCache.remove(userid);
						}
					}
					// failure pam_authenticate failed: Authentication failure
					else {
						ar.setFailure();
						result = result.replace("failure pam", "pam");
						ar.setReason(result);
					}
				}
			}
		}
		catch(Exception e) {
			ar.setFailure();
			ar.setException(e);
		}
		return ar;
	}

	@Override
	public IAuthenticationResult isGroupMember(String userid, String domain, Role role) {
		return super.isGroupMember(userid, domain, role);
	}
	
}
