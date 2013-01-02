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

import org.apache.log4j.Logger;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;


public class AuthenticationManager implements IAuthenticationManager {
	
	private static final IAuthenticationResult default_authenticationResult = new AuthenticationResult();
	
	private static IAuthenticationManager instance = new AuthenticationManager();
	
	private final String version = "common 1.0";
	
	public static IAuthenticationManager getInstance() {
		return instance;
	}
	
	@Override
	public String getVersion() {
		return version;
	}
	
	@Override
	public IAuthenticationResult isAuthenticate(String userid, String domain, String password) {
		IAuthenticationResult authenticationResult = default_authenticationResult;
		String ducc_pw = DuccPropertiesResolver.getInstance().getFileProperty(DuccPropertiesResolver.ducc_ws_authentication_pw);
		if(ducc_pw != null) {
			ducc_pw = ducc_pw.trim();
			if(ducc_pw.equals(password)) {
				authenticationResult = new AuthenticationResult();
				authenticationResult.setSuccess();
			}
		}
		return authenticationResult;
	}

	@Override
	public IAuthenticationResult isGroupMember(String userid, String domain, Role role) {
		AuthenticationResult authenticationResult = new AuthenticationResult();
		authenticationResult.setSuccess();
		return authenticationResult;
	}

	/*
	 * <test cases>
	 */
	
	public static final String user = System.getProperty("user.name");
	
	private static void testUser(Logger logt, String userid, String password)
	{
		testUser(logt,userid,null,password);
	}
	
	private static void testUser(Logger logt, String userid, String domain, String password)
	{
		IAuthenticationResult ar = AuthenticationManager.getInstance().isAuthenticate(userid, domain, password);
		logt.info(userid+" "+ar.isSuccess());
	}
	
	private static void testGroup(Logger logt, String userid, String domain, Role role)
	{
		IAuthenticationResult ar = AuthenticationManager.getInstance().isGroupMember(userid,domain,role);
		logt.info(userid+" "+" "+role+" "+ar.isSuccess());
	}
	
	private static void test(String[] args) 
	{
		Logger logt = Logger.getLogger("Test");
		logt.info("test.enter");
		testUser(logt,user,"xxxxxxxx");
		testUser(logt,user,"foobar");
		testUser(logt,user,"watson.ibm.com","xxxxxxxx");
		testUser(logt,user,"watson.ibm.com","foobar");
		testGroup(logt,user,"",Role.User);
		testGroup(logt,user,"",Role.Admin);
		testGroup(logt,"foobar","",Role.User);
		logt.info("test.exit");
	}
	
	public static void main(String[] args) 
	{
		try {
			test(args);
			System.out.println("Successs!");
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	/*
	 * </test cases>
	 */
}
