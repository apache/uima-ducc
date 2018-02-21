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

import java.io.IOException;
import java.io.InputStreamReader;

import java.io.BufferedReader;

public class LinuxUtils {
	
	public static String getUserHome(String userName) {
		String userHome = null;
		try {
			userHome = new BufferedReader(new InputStreamReader(Runtime.getRuntime().exec(new String[]{"sh", "-c", "echo ~" + userName}).getInputStream())).readLine();
		} 
		catch (IOException e) {
		}
		return userHome;
	}
	
	public static final String user = System.getProperty("user.name");
	
	public static void main(String[] args) {
		System.out.println(getUserHome("root"));
		System.out.println(getUserHome(user));
	}

}
