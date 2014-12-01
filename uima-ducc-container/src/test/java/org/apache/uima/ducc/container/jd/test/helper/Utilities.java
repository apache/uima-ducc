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
package org.apache.uima.ducc.container.jd.test.helper;

import java.net.URL;

public class Utilities {
	
	private static Utilities instance = new Utilities();
	
	private String userCP = null;
	
	public static Utilities getInstance() {
		return instance;
	}
	
	private String resource(String name) {
		String retVal = "";
		URL url = this.getClass().getResource(name);
		retVal = url.getFile();
		return retVal;
	}
	
	private Utilities() {
		userCP = 
			resource("/") +
			":" +
			resource("/uima-ducc-user.jar") +
			":" +
			resource("/uimaj-as-core.jar") +
			":" +
			resource("/uimaj-core.jar") +
			":" +
			resource("/xstream-1.3.1.jar") +
			":" +
			resource("/spring-core.jar") +
			":" +
			resource("/xmlbeans.jar") +
			""
			;
	}
	
	public String getUserCP() {
		return userCP;
	}
	
	public String getUserCP(boolean value) {
		String retVal = userCP;
		if(value) {
			listToConsole(userCP);
		}
		return retVal;
	}
	
	public void listToConsole(String userCP) {
		if(userCP != null) {
			String[] segments = userCP.split(":");
			for(String segment : segments) {
				System.out.println(segment);
			}
		}
	}
}
