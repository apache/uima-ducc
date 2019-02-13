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
package org.apache.uima.ducc.common.utils;

import java.io.Closeable;

import org.apache.uima.ducc.common.utils.id.DuccId;

public abstract class AlienAbstract {
	
	private static DuccLogger duccLogger = DuccLogger.getLogger(AlienAbstract.class);
	
	protected DuccId duccId = null;
	
	protected String encoding = "UTF-8";
	
	protected String ducc_ling;
	protected String q_parameter = "-q";
	protected String u_parameter = "-u";
	
	protected String user;
	
	protected void set_encoding(String value) {
		encoding = value;
	}
	
	protected String get_encoding() {
		return encoding;
	}
	
	protected void set_ducc_ling(String value) {
		ducc_ling = value;
	}
	
	protected String get_ducc_ling() {
		return ducc_ling;
	}
	
	protected void set_u_parameter(String value) {
		u_parameter = value;
	}
	
	protected String get_u_parameter() {
		return u_parameter;
	}
	
	protected void set_q_parameter(String value) {
		q_parameter = value;
	}
	
	protected String get_q_parameter() {
		return q_parameter;
	}

	protected void set_user(String value) {
		user = value;
	}
	
	protected String get_user() {
		return user;
	}
	
	protected void echo(String[] command) {
		String methodName = "echo";
		try {
			StringBuffer sb = new StringBuffer();
			for(String token : command) {
				sb.append(" ");
				sb.append(token);
			}
			String text = sb.toString().trim();
			duccLogger.debug(methodName, duccId, text);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	protected void closer(Closeable c) {
		try {
			c.close();
		}
		catch(Exception e) {
		}
	}
	
}
