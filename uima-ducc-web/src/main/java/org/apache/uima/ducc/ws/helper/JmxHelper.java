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
package org.apache.uima.ducc.ws.helper;

import java.io.IOException;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

public class JmxHelper {
	
	private String jmxHost = "localhost";
	private int jmxPort = -1;
	private String jmxUrl = null;
	
	private JMXServiceURL url;
	private JMXConnector jmxc;
	private MBeanServerConnection mbsc;
	
	protected void setJmxHost(String value) {
		jmxHost = value;
	}
	
	public String getJmxHost() {
		return jmxHost;
	}
	
	protected void setJmxPort(int value) {
		jmxPort = value;
	}
	
	public int getJmxPort() {
		return jmxPort;
	}
	
	protected void setJmxUrl(String value) {
		jmxUrl = value;
	}
	
	public String getJmxUrl() {
		return jmxUrl;
	}
	
	public MBeanServerConnection getMBSC() {
		return mbsc;
	}
	
	protected void jmxConnect() throws IOException {
		url = new JMXServiceURL(getJmxUrl());
		jmxc = JMXConnectorFactory.connect(url, null);
		mbsc = jmxc.getMBeanServerConnection();
	}
	
}
