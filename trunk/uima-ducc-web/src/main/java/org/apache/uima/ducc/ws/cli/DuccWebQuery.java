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
package org.apache.uima.ducc.ws.cli;

import java.net.InetAddress;

import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.Utils;

public class DuccWebQuery {
	
	protected DuccPropertiesResolver dpr = DuccPropertiesResolver.getInstance();
	
	protected String DUCC_HOME = Utils.findDuccHome();
	protected String ws_scheme = "http";
	protected String ws_host = "localhost";
	protected String ws_port = "42133";
	protected String ws_servlet = null;;
	
	protected boolean showURL = true;
	
	protected DuccWebQuery(String servlet) {
		assert_ducc_home();
		determine_host();
		determine_port();
		ws_servlet = servlet;
	}
	
	protected void assert_ducc_home() {
		if(DUCC_HOME == null) {
			throw new RuntimeException("DUCC_HOME not specified");
		}
	}
	
	protected void determine_host() {
		try {
			ws_host = java.net.InetAddress.getLocalHost().getHostName();
		}
		catch(Exception e) {
		}
		String host = dpr.getProperty(DuccPropertiesResolver.ducc_ws_host);
		if(host != null) {
			ws_host = host;
		}
		if(ws_host != null) {
			if(ws_host.length() > 0) {
				if(!ws_host.contains(".")) {
					try {
						InetAddress addr = InetAddress.getLocalHost();
						String canonicalHostName = addr.getCanonicalHostName();
						if(canonicalHostName.startsWith(ws_host)) {
							ws_host = canonicalHostName;
						}
					}
					catch(Exception e) {
						
					}
				}
			}
		}
	}
	
	protected void determine_port() {
		String port = dpr.getProperty(DuccPropertiesResolver.ducc_ws_port);
		if(port != null) {
			ws_port = port;
		}
	}
	
	protected String getUrlString() {
		String urlString = ws_scheme+"://"+ws_host+":"+ws_port+ws_servlet;
		if(showURL) {
			System.out.println(urlString);
		}
		return urlString;
	}
}
