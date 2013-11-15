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

import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;

public class LinuxAuthenticationManager implements IAuthenticationManager {
	
	private static IAuthenticationManager instance = new LinuxAuthenticationManager();
	
	private String version = "ducc linux 1.0";
	
	private DuccPropertiesResolver duccPropertiesResolver = DuccPropertiesResolver.getInstance();
	
	private ConcurrentHashMap<String,String[]> userGroupsCache = new ConcurrentHashMap<String,String[]>();
	
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

	private String getFileProperty(String key) {
		String retVal = duccPropertiesResolver.getFileProperty(key);
		return retVal;
	}

	private String getProperty(String key) {
		return getFileProperty(key);
	}
	
	private String removeDelimiters(String string) {
		String retVal = string;
		if(retVal == null) {
			retVal = "";
		}
		else {
			retVal = retVal.replace(',', ' ');
			retVal = retVal.replace(';', ' ');
			retVal = retVal.replace(':', ' ');
		}
		return retVal.trim();
	}
	
	private String transform(String string) {
		String retVal = removeDelimiters(string);
		return(retVal);
	}
	
	private boolean finder(String rawNeedle, String rawHaystack) {
		boolean retVal = false;
		if(rawNeedle != null) {
			if(rawHaystack != null) {
				String needle = " "+rawNeedle+" ";
				String haystack = " "+rawHaystack+" ";
				if(haystack.contains(needle)) {
					retVal = true;
				}
			}
		}
		return retVal;
	}
	
	private IAuthenticationResult checkUserExcluded(String userid) {
		IAuthenticationResult retVal = new AuthenticationResult(IAuthenticationResult.SUCCESS);
		if(userid == null) {
			retVal.setFailure();
			retVal.setReason("userid missing");
		}
		else {
			String uid = transform(userid);
			String excludeString = transform(getProperty(DuccPropertiesResolver.ducc_authentication_users_exclude));
			if(excludeString.trim().length() > 0) {
				if(finder(uid,excludeString)) {
					retVal.setFailure();
					retVal.setReason("userid excluded");
				}
			}
		}
		return retVal;
	}
	
	private IAuthenticationResult checkUserNotIncluded(String userid) {
		IAuthenticationResult retVal = new AuthenticationResult(IAuthenticationResult.SUCCESS);
		if(userid == null) {
			retVal.setFailure();
			retVal.setReason("userid missing");
		}
		else {
			String uid = transform(userid);
			String includeString = transform(getProperty(DuccPropertiesResolver.ducc_authentication_users_include));
			if(includeString.trim().length() > 0) {
				if(!finder(uid,includeString)) {
					retVal.setFailure();
					retVal.setReason("userid not included");
				}
			}
		}
		return retVal;
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
	
	private IAuthenticationResult checkUserGroupExcluded(String userid) {
		IAuthenticationResult retVal = new AuthenticationResult(IAuthenticationResult.SUCCESS);
		if(userid == null) {
			retVal.setFailure();
			retVal.setReason("userid missing");
		}
		else {
			String excludeString = transform(getProperty(DuccPropertiesResolver.ducc_authentication_groups_exclude));
			if(excludeString.trim().length() > 0) {
				String[] userGroups = userGroupsCache.get(userid);
				if(userGroups == null) {
					retVal.setFailure();
					retVal.setReason("userid has no groups?");
				}
				else {
					for(String userGroup : userGroups) {
						if(finder(userGroup,excludeString)) {
							retVal.setFailure();
							retVal.setReason("userid group "+userGroup+" excluded");
							break;
						}
					}
				}
			}
		}
		return retVal;
	}
	
	private IAuthenticationResult checkUserGroupNotIncluded(String userid) {
		IAuthenticationResult retVal = new AuthenticationResult(IAuthenticationResult.SUCCESS);
		if(userid == null) {
			retVal.setFailure();
			retVal.setReason("userid missing");
		}
		else {
			String includeString = transform(getProperty(DuccPropertiesResolver.ducc_authentication_groups_include));
			if(includeString.trim().length() > 0) {
				String[] userGroups = userGroupsCache.get(userid);
				if(userGroups == null) {
					retVal.setFailure();
					retVal.setReason("userid has no groups?");
				}
				else {
					retVal.setFailure();
					retVal.setReason("userid has no group included");
					for(String userGroup : userGroups) {
						if(finder(userGroup,includeString)) {
							retVal = new AuthenticationResult(IAuthenticationResult.SUCCESS);
							break;
						}
					}
				}
			}
		}
		return retVal;
	}
	
	@Override
	public IAuthenticationResult isGroupMember(String userid, String domain, Role role) {
		IAuthenticationResult ar = new AuthenticationResult(IAuthenticationResult.SUCCESS);
		try {
			ar = checkUserGroupExcluded(userid);
			if(ar.isSuccess()) {
				ar = checkUserGroupNotIncluded(userid);
			}
		}
		catch(Exception e) {
			ar.setFailure();
			ar.setException(e);
		}
		return ar;
	}
	
}
