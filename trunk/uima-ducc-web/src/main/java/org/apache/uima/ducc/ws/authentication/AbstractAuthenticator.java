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

import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.common.authentication.AuthenticationResult;
import org.apache.uima.ducc.common.authentication.IAuthenticationManager;
import org.apache.uima.ducc.common.authentication.IAuthenticationResult;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;

public abstract class AbstractAuthenticator implements IAuthenticationManager {
	
	private DuccPropertiesResolver duccPropertiesResolver = DuccPropertiesResolver.getInstance();

	protected ConcurrentHashMap<String,String[]> userGroupsCache = new ConcurrentHashMap<String,String[]>();
	
	public String getNotes(String userid) {
		return null;
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
	
	protected String transform(String string) {
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

	protected IAuthenticationResult checkUserExcluded(String userid) {
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
	
	protected IAuthenticationResult checkUserNotIncluded(String userid) {
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
	
	protected IAuthenticationResult checkUserGroupExcluded(String userid) {
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
	
	protected IAuthenticationResult checkUserGroupNotIncluded(String userid) {
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
