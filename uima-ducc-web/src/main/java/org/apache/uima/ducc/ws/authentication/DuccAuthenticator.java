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

import org.apache.uima.ducc.common.authentication.IAuthenticationManager;
import org.apache.uima.ducc.common.authentication.IAuthenticationResult;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.id.DuccId;

public class DuccAuthenticator extends AbstractAuthenticator {
	
	private static DuccLogger duccLogger = DuccLogger.getLogger(DuccAuthenticator.class);
	private static DuccId jobid = null;
	
	private static DuccAuthenticator instance = new DuccAuthenticator();
	
	private DuccPropertiesResolver duccPropertiesResolver = null;

	private IAuthenticationManager iAuthenticationManager = null;
	
	public static DuccAuthenticator getInstance() {
		return instance;
	}
	
	public DuccAuthenticator() {
		duccPropertiesResolver = DuccPropertiesResolver.getInstance();
		initializeAuthenticator();
	}
	
	private void initializeAuthenticator() {
		String methodName = "initializeAuthenticator";
		try {
			String key = DuccPropertiesResolver.ducc_authentication_implementer;
			String value = duccPropertiesResolver.getProperty(key);
			duccLogger.info(methodName, jobid, value);
			String cp = System.getProperty("java.class.path");
			String[] cplist = cp.split(":");
			if(cplist != null) {
				for(String item : cplist) {
					duccLogger.debug(methodName, null, item);
				}
			}
			Class<?> authenticationImplementer = Class.forName(value);
			iAuthenticationManager = (IAuthenticationManager)authenticationImplementer.newInstance();
			duccLogger.info(methodName, jobid, iAuthenticationManager.getVersion());
		}
		catch(Throwable t) {
			duccLogger.error(methodName, jobid, t);
		}
	}

	@Override
	public String getVersion() {
		String methodName = "getVersion";
		String retVal = null;
		try {
			retVal = iAuthenticationManager.getVersion();
			duccLogger.debug(methodName, jobid, retVal);
		}
		catch(Throwable t) {
			duccLogger.error(methodName, jobid, t);
		}
		return retVal;
	}

	@Override
	public String getNotes(String userid) {
		String methodName = "getNotes";
		String retVal = null;
		try {
			retVal = iAuthenticationManager.getNotes(userid);
			duccLogger.debug(methodName, jobid, retVal);
		}
		catch(Throwable t) {
			duccLogger.debug(methodName, jobid, "no notes available - legacy implementer?");
			duccLogger.debug(methodName, jobid, t);
		}
		return retVal;
	}
	
	@Override
	public boolean isPasswordChecked() {
		String methodName = "isPasswordChecked";
		boolean retVal = false;
		try {
			retVal = iAuthenticationManager.isPasswordChecked();
			duccLogger.debug(methodName, jobid, retVal);
		}
		catch(Throwable t) {
			duccLogger.error(methodName, jobid, t);
		}
		return retVal;
	}

	@Override
	public IAuthenticationResult isAuthenticate(String userid, String domain, String password) {
		String methodName = "isAuthenticate";
		IAuthenticationResult retVal = null;
		try {
			retVal = iAuthenticationManager.isAuthenticate(userid, domain, password);
			duccLogger.debug(methodName, jobid, userid+" "+domain+" "+retVal);
		}
		catch(Throwable t) {
			duccLogger.error(methodName, jobid, t);
		}
		return retVal;
	}

	@Override
	public IAuthenticationResult isGroupMember(String userid, String domain, Role role) {
		String methodName = "isGroupMember";
		IAuthenticationResult retVal = null;
		try {
			retVal = iAuthenticationManager.isGroupMember(userid, domain, role);
			duccLogger.debug(methodName, jobid, userid+" "+domain+" "+retVal);
		}
		catch(Throwable t) {
			duccLogger.error(methodName, jobid, t);
		}
		return retVal;
	}
	
}
