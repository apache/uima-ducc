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

import java.io.File;

import org.apache.uima.ducc.common.authentication.AuthenticationResult;
import org.apache.uima.ducc.common.authentication.IAuthenticationResult;
import org.apache.uima.ducc.common.utils.AlienFile;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.ws.utils.HandlersHelper;

/*
 * This authenticator controls ducc-mon login access when specified in site.ducc.properties:
 * 
 * ducc.authentication.implementer=org.apache.uima.ducc.ws.authentication.SecureFileAuthenticator
 * 
 * The user specified password is checked against the password generated and stored in
 * the <ducc.security.home>/.ducc/.login.pw.  The DUCC security home is specified in
 * ducc.properties:
 * 
 * ducc.security.home =
 * 
 * If not specified (as above) then the user's home directory is used.
 * 
 * Each password is single use, as each login attempt causes a new generated password.
 * 
 * Password generation is accomplished by executing the following Python script:
 * 
 * <ducc_home>/admin/duccmon_pwgen.py
 */
public class SecureFileAuthenticator extends AbstractAuthenticator {
	
	private static DuccLogger duccLogger = DuccLoggerComponents.getWsLogger(SecureFileAuthenticator.class.getName());
	private static DuccId jobid = null;
	
	private String version = "Secure File 1.0";
	
	private String pwfile = ".ducc/.login.pw";
	
	public SecureFileAuthenticator() {
	}
	
	@Override
	public String getVersion() {
		return version;
	}

	@Override
	public String getNotes(String userid) {
		String location = "getNotes";
		String retVal = null;
		String filename = getPwFile(userid);
		if(filename != null) {
			retVal = "Specify password found here: "+filename;
		}
		duccLogger.debug(location, jobid, "userid:"+userid+" "+"filename:"+filename);
		return retVal;
	}
	
	@Override
	public boolean isPasswordChecked() {
		return true;
	}

	private String getPwFile(String userid) {
		String location = "getPwFile";
		String retVal = null;
		String home = null;
		if(userid != null) {
			home = HandlersHelper.getSecurityHome(userid);
			if(home != null) {
				StringBuffer sb = new StringBuffer();
				sb.append(home);
				if(!home.endsWith(File.separator)) {
					sb.append(File.separator);
				}
				sb.append(pwfile);
				retVal = sb.toString();
			}
		}
		duccLogger.debug(location, jobid, "userid:"+userid+" "+"home:"+home+" "+"file:"+retVal);
		return retVal;
	}
	
	@Override
	public IAuthenticationResult isAuthenticate(String userid, String domain, String password) {
		String methodName = "isAuthenticate";
		IAuthenticationResult ar = new AuthenticationResult();
		ar.setFailure();
		try {
			String filename = getPwFile(userid);
			if(filename != null) {
				if( password != null) {
					password = password.trim();
					duccLogger.info(methodName, jobid, "password: "+password);
					AlienFile af = new AlienFile(userid, filename);
					String contents = af.getString();
					if(contents != null) {
						contents = contents.trim();
					}
					duccLogger.info(methodName, jobid, filename+": "+contents);
					if(password.equals(contents)) {
						ar.setSuccess();
					}
					else {
						duccLogger.info(methodName, jobid, contents+"!="+password);
					}
					String DUCC_HOME = System.getProperty("DUCC_HOME");
					String duccmon_pwgen = DUCC_HOME+"/admin/duccmon_pwgen.py";
					String[] args =  { "-q", "-u", userid, "--", duccmon_pwgen };
					DuccAsUser.duckling(userid, args);
				}
			}
		}
		catch(Exception e) {
			duccLogger.warn(methodName, jobid, e);
		}
		duccLogger.info(methodName, jobid, "failure:"+ar.isFailure()+" "+"success:"+ar.isSuccess());
		return ar;
	}

	@Override
	public IAuthenticationResult isGroupMember(String userid, String domain, Role role) {
		return super.isGroupMember(userid, domain, role);
	}

}
