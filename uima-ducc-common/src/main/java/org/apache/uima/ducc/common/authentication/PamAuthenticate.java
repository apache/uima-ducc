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

import org.jvnet.libpam.PAM;
import org.jvnet.libpam.UnixUser;

public class PamAuthenticate {

	private enum Result { success, failure };
	
	private void info(Result result, String text) {
		System.out.println(result.name()+" "+text);
	}
	
	private void launch(String[] args) {
		try {
			if(args == null) {
				info(Result.failure, "args==null");
			}
			else if(args.length != 2) {
				info(Result.failure, "args.length!=2");
			}
			else if(args[0] == null) {
				info(Result.failure, "args[0]==null");
			}
			else if(args[1] == null) {
				info(Result.failure, "args[1]==null");
			}
			else {
				String userid = args[0];
				String password = args[1];
				UnixUser u = new PAM("sshd").authenticate(userid, password);
				info(Result.success, "groups = "+u.getGroups().toString());
			}
			
		}
		catch(Throwable t) {
			info(Result.failure,t.getMessage());
			//t.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		PamAuthenticate instance = new PamAuthenticate();
		instance.launch(args);
	}

}
