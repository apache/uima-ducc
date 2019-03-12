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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.uima.ducc.common.authentication.AuthenticationResult;
import org.apache.uima.ducc.common.authentication.IAuthenticationResult;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.id.DuccId;

public class GSAAuthenticator extends AbstractAuthenticator {

	private static DuccLogger duccLogger = DuccLogger.getLogger(GSAAuthenticator.class);
	private static DuccId jobid = null;
	
	private String version = "GSA 1.0";
	
	@Override
	public String getVersion() {
		return version;
	}

	@Override
	public String getNotes(String userid) {
		String retVal = "Specify your GSA login password.";
		return retVal;
	}
	
	@Override
	public boolean isPasswordChecked() {
		return true;
	}

	private String get_ducc_ling() throws Exception {
		DuccPropertiesResolver dpr = DuccPropertiesResolver.getInstance();
		String key = "ducc.agent.launcher.ducc_spawn_path";
		String value = dpr.getFileProperty(key);
		if(value == null) {
			throw new Exception("missing ducc_ling in ducc.properties?");
		}
		return value;
	}
	
	private String gsa_login = "/usr/bin/gsa_login";
	private String gsa_arg1 = "-p";
	private String gsa_response_prefix = "Successfully authenticated";
	
	private int login(String user, String pw) throws Exception {
		String location = "login";
		int rc_success = 0;
		int rc_fail = 1;
		int rc = rc_fail;
		String ducc_ling = get_ducc_ling();
		ProcessBuilder builder = new ProcessBuilder(ducc_ling,"-u",user,"--",gsa_login,gsa_arg1);
		builder.redirectErrorStream(true);
		Process p = builder.start();
		BufferedWriter w = new BufferedWriter(new OutputStreamWriter(p.getOutputStream()));
		w.write(pw);
		w.flush();
		w.close();
		BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()));
		String line;
		while (true) {
			line = r.readLine();
			if (line == null) {
				break;
			}
			duccLogger.debug(location, jobid, line);
			if (line.startsWith(gsa_response_prefix)) {
				rc = rc_success;
				break;
			}
		}
		r.close();
		return rc;
	}
	
	@Override
	public IAuthenticationResult isAuthenticate(String userid, String domain, String password) {
		String location = "isAuthenticate";
		IAuthenticationResult ar = new AuthenticationResult();
		ar.setFailure();
		try {
			int rc = login(userid,password);
			if(rc == 0) {
				ar.setSuccess();
			}
			duccLogger.info(location, jobid, "failure:"+ar.isFailure()+" "+"success:"+ar.isSuccess());
		}
		catch(Exception e) {
			duccLogger.warn(location, jobid, e);
		}
		duccLogger.info(location, jobid, "failure:"+ar.isFailure()+" "+"success:"+ar.isSuccess());
		return ar;
	}

}
