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
	
public class AuthenticationResult implements IAuthenticationResult {
	
	private boolean result = true;
	private int code = -1;
	private String reason = null;
	private Exception exception = null;
	
	public AuthenticationResult() {
	}
	
	public AuthenticationResult(boolean value) {
		this.result = value;
	}
	
	public AuthenticationResult(String reason, Exception exception) {
		setFailure();
		setReason(reason);
		setException(exception);
	}
	
	public void setSuccess() {
		this.result = true;
	}
	
	public void setFailure() {
		this.result = false;
	}
	
	public boolean isSuccess() {
		return this.result;
	}
	
	public boolean isFailure() {
		return !this.result;
	}
	
	public void setReason(String reason) {
		this.reason = reason;
	}
	
	public String getReason() {
		return this.reason;
	}
	
	public void setException(Exception exception) {
		this.exception = exception;
	}
	
	public Exception getException() {
		return this.exception;
	}
	
	public void setCode(int code) {
		this.code = code;
	}

	public int getCode() {
		return code;
	}
}

