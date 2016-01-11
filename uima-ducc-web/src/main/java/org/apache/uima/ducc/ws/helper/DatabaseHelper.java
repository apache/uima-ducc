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
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.id.DuccId;

public class DatabaseHelper {
	
	private static DuccLogger logger = DuccLoggerComponents.getWsLogger(DatabaseHelper.class.getName());
	private static DuccId jobid = null;
	
	private static DatabaseHelper instance = new DatabaseHelper();
	
	public static DatabaseHelper getInstance() {
		return instance;
	}

	protected boolean enabled = false;
	protected String host = null;
	protected String jmxHost = "localhost";
	protected int jmxPort = 7199;
	protected String jmxUrl = null;
	
	private JMXServiceURL url;
	private JMXConnector jmxc;
	private MBeanServerConnection mbsc;
	
	private DatabaseHelper() {
		init();
	}
	
	private void init() {
		String location = "init";
		try {
			DuccPropertiesResolver dpr = DuccPropertiesResolver.getInstance();
			String value;
			value = dpr.getProperty(DuccPropertiesResolver.ducc_database_host);
			if(value != null) {
				setHost(value);
				if(!value.equalsIgnoreCase(DuccPropertiesResolver.ducc_database_disabled)) {
					enabled = true;
				}
			}
			value = dpr.getProperty(DuccPropertiesResolver.ducc_database_jmx_host);
			if(value != null) {
				try {
					setJmxHost(value);
				}
				catch(Exception e) {
					logger.error(location, jobid, e);
				}
			}
			value = dpr.getProperty(DuccPropertiesResolver.ducc_database_jmx_port);
			if(value != null) {
				try {
					setJmxPort(Integer.parseInt(value));
				}
				catch(Exception e) {
					logger.error(location, jobid, e);
				}
			}
			value = "service:jmx:rmi:///jndi/rmi://"+getJmxHost()+":"+getJmxPort()+"/jmxrmi";
			setJmxUrl(value);
			url = new JMXServiceURL(getJmxUrl());
			jmxc = JMXConnectorFactory.connect(url, null);
			mbsc = jmxc.getMBeanServerConnection();
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
	}
	
	public boolean isEnabled() {
		return enabled;
	}
	
	public boolean isDisabled() {
		return !enabled;
	}
	
	private void setHost(String value) {
		host = value;
	}
	
	public String getHost() {
		return host;
	}
	
	private void setJmxHost(String value) {
		jmxHost = value;
	}
	
	public String getJmxHost() {
		return jmxHost;
	}
	
	private void setJmxPort(int value) {
		jmxPort = value;
	}
	
	public int getJmxPort() {
		return jmxPort;
	}
	
	private void setJmxUrl(String value) {
		jmxUrl = value;
	}
	
	public String getJmxUrl() {
		return jmxUrl;
	}
	
	// Runtime Info //
	
	public boolean isAlive() {
		boolean retVal = false;
		if(getPID() != 0) {
			retVal = true;
		}
		return retVal;
	}
	
	public Long getStartTime() {
		String location = "getStartTime";
		Long retVal = new Long(0);
		try {
			Object o = mbsc.getAttribute(new ObjectName("java.lang:type=Runtime"), "StartTime");
			retVal = (Long) o;
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
		return retVal;
	}
	
	public Long getPID() {
		String location = "getPID";
		Long retVal = new Long(0);
		try {
			Object o = mbsc.getAttribute(new ObjectName("java.lang:type=Runtime"), "Name");
			String data = (String) o;
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
