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
package org.apache.uima.ducc.common.launcher.ssh;

import java.io.ByteArrayOutputStream;

public class ProcessLaunchException extends Exception {
	ByteArrayOutputStream errorStream = new ByteArrayOutputStream();
//	ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    String host;
    String command;
    
	private static final long serialVersionUID = 1296096486632219333L;
	
//	public ProcessLaunchException(Exception e, ByteArrayOutputStream errorStream, ByteArrayOutputStream outputStream, String host, String command ) {
	public ProcessLaunchException(Exception e, ByteArrayOutputStream errorStream, String host, String command ) {
		super(e);
		this.errorStream = errorStream;
//		this.outputStream = outputStream;
		this.host = host;
		this.command = command;
	}
	
	public String getHost() {
		return host;
	}
	public String getCommand() {
		return command;
	}
	public ByteArrayOutputStream getErrorStream() {
		return errorStream;
	}
//	public ByteArrayOutputStream getOutputStream() {
//		return outputStream;
//	}
}
