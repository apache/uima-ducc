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

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;

public abstract class JmxHelper {
	
	private static DuccLogger logger = DuccLogger.getLogger(JmxHelper.class);
	private static DuccId jobid = null;
	
	private String jmxHost = "localhost";
	private int jmxPort = -1;
	
	private JMXServiceURL url = null;
	private JMXConnector jmxc = null;
	private MBeanServerConnection mbsc = null;
	
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
	
	public String getJmxUrl() {
		return "service:jmx:rmi:///jndi/rmi://"+getJmxHost()+":"+getJmxPort()+"/jmxrmi";
	}
	
	public MBeanServerConnection getMBSC() {
		return mbsc;
	}
	
	protected void connect() throws Exception {
		String location = "connect";
		try {
			url = new JMXServiceURL(getJmxUrl());
			jmxc = JMXConnectorFactory.connect(url, null);
			mbsc = jmxc.getMBeanServerConnection();
			String id = jmxc.getConnectionId();
			logger.debug(location, jobid, id);
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
			throw e;
		}
		
	}

	protected void disconnect() throws Exception {
		String location = "disconnect";
		try {
			if(jmxc != null) {
				String id = jmxc.getConnectionId();
				jmxc.close();
				jmxc = null;
				logger.debug(location, jobid, id);
			}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
			throw e;
		}
	}
	
	protected String getJmxData() throws Exception {
		Object o = null;
		MBeanServerConnection mbsc = null;
		try {
			mbsc = getMBSC();
			o = mbsc.getAttribute(new ObjectName("java.lang:type=Runtime"), "Name");
		} 
		catch(Exception e) {
			connect();
			mbsc = getMBSC();
			o = mbsc.getAttribute(new ObjectName("java.lang:type=Runtime"), "Name");
			disconnect();
		}
		String data = (String) o;
		return data;
	}
	
	public Long getPID() {
		String location = "getPID";
		Long retVal = new Long(0);
		try {
			String data = getJmxData();
			String[] address = data.split("@");
			Long pid = Long.parseLong(address[0]);
			retVal = pid;
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
		return retVal;
	}

}
