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
package org.apache.uima.ducc.user.jd;

import java.io.PrintWriter;
import java.io.StringWriter;

public class JdUserException extends Exception {

	private static final long serialVersionUID = 1L;

	private String userException = null;
	
	public JdUserException(Exception userException) {
		try {
			if(userException == null) {
				setUserException("exception is null?");
			}
			else {
				StringWriter sw = new StringWriter();
				userException.printStackTrace(new PrintWriter(sw));
				setUserException(sw.toString());
			}
		}
		catch(Exception e) {
			setUserException("exception obtaining stack trace?");
		}
	}
	
	public String getUserException() {
		return userException;
	}
	
	private void setUserException(String value) {
		userException = value;
	}
}
