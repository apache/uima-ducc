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

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.ConnectionMetaData;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.id.DuccId;

public class BrokerHelper {
	
	private static DuccLogger duccLogger = DuccLoggerComponents.getWsLogger(BrokerHelper.class.getName());
	private static DuccId jobid = null;

	private static BrokerHelper instance = new BrokerHelper();
	
	public static BrokerHelper getInstance() {
		return instance;
	}
	
	public enum BrokerAttribute { BrokerVersion, MemoryPercentUsage, Uptime };
	public enum FrameworkAttribute { ConsumerCount, QueueSize, MaxEnqueueTime, AverageEnqueueTime, MemoryPercentUsage };

	private String host = "?";
	private int port = 1100;
	
	private String jmxUrl = null;
	
	private JMXServiceURL url;
	private JMXConnector jmxc;
	private MBeanServerConnection mbsc;

	private OperatingSystemMXBean remoteOperatingSystem;
	private ThreadMXBean remoteThread;
	// 
	private boolean useNewJmxQueryString = false;
	private BrokerHelper() {
		init();
	}
	
	private void init() {
		String location = "init";
		try {
			DuccPropertiesResolver duccPropertiesResolver = DuccPropertiesResolver.getInstance();
			String key;
			String value;
			key = "ducc.broker.hostname";
			value = duccPropertiesResolver.getCachedProperty(key);
			setHost(value);
			key = "ducc.broker.jmx.port";
			value = duccPropertiesResolver.getCachedProperty(key);
			setPort(value);
			value = "service:jmx:rmi:///jndi/rmi://"+getHost()+":"+getPort()+"/jmxrmi";
			setJmxUrl(value);
			url = new JMXServiceURL(getJmxUrl());
			jmxc = JMXConnectorFactory.connect(url, null);
			mbsc = jmxc.getMBeanServerConnection();
			key = "ducc.broker.protocol";
			value = duccPropertiesResolver.getCachedProperty(key);
			String brokerUrl = value.trim();
			key = "ducc.broker.hostname";
			value = duccPropertiesResolver.getCachedProperty(key);
			brokerUrl += "://"+value;
			key = "ducc.broker.port";
			value = duccPropertiesResolver.getCachedProperty(key);
			brokerUrl += ":"+value;
			
			ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerUrl);
			Connection connection = connectionFactory.createConnection(); 
	        connection.start(); 
	        ConnectionMetaData connectionMetaData = connection.getMetaData();
	        int majorVersion = connectionMetaData.getProviderMajorVersion();
	        int minorVersion = connectionMetaData.getProviderMinorVersion();
	        duccLogger.info(location, jobid, "Broker Version:"+majorVersion+"."+minorVersion);
	        if ( majorVersion > 5 || ( majorVersion == 5 && minorVersion >= 8 )) {
	        	useNewJmxQueryString = true;
	        }
			remoteOperatingSystem = 
	                ManagementFactory.newPlatformMXBeanProxy(
	                    mbsc,
	                    ManagementFactory.OPERATING_SYSTEM_MXBEAN_NAME,
	                    OperatingSystemMXBean.class);
			remoteThread = 
	                ManagementFactory.newPlatformMXBeanProxy(
	                    mbsc,
	                    ManagementFactory.THREAD_MXBEAN_NAME,
	                    ThreadMXBean.class);
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
	}
	
	private void setHost(String value) {
		host = value;
	}
	
	public String getHost() {
		return host;
	}
	
	private void setPort(String value) {
		String location = "setPort";
		try {
			port = Integer.parseInt(value);
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
	}
	
	public int getPort() {
		return port;
	}
	
	private void setJmxUrl(String value) {
		jmxUrl = value;
	}
	
	public String getJmxUrl() {
		return jmxUrl;
	}
	
	// Memory Info //
	
	public Long getMemoryUsed() {
		String location = "getMemoryUsed";
		Long retVal = new Long(0);
		try {
			Object o = mbsc.getAttribute(new ObjectName("java.lang:type=Memory"), "HeapMemoryUsage");
			CompositeData cd = (CompositeData) o;
			retVal = (Long) cd.get("used");
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
		return retVal;
	}
	
	public Long getMemoryMax() {
		String location = "getMemoryMax";
		Long retVal = new Long(0);
		try {
			Object o = mbsc.getAttribute(new ObjectName("java.lang:type=Memory"), "HeapMemoryUsage");
			CompositeData cd = (CompositeData) o;
			retVal = (Long) cd.get("max");
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
		return retVal;
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
			duccLogger.error(location, jobid, e);
		}
		return retVal;
	}
	
	public Long getPID() {
		String location = "getPID";
		Long retVal = new Long(0);
		try {
			Object o = mbsc.getAttribute(new ObjectName("java.lang:type=Runtime"), "ProcessID");
			retVal = (Long) o;
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
		return retVal;
	}
	
	// Threads Info //
	
	public int getThreadsLive() {
		String location = "getThreadsLive";
		int retVal = 0;
		try {
			retVal = remoteThread.getThreadCount();
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
		return retVal;
	}
	
	public int getThreadsPeak() {
		String location = "getThreadsPeak";
		int retVal = 0;
		try {
			retVal = remoteThread.getPeakThreadCount();
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
		return retVal;
	}
	
	// Operating System Info //
	
	public double getSystemLoadAverage() {
		String location = "getSystemLoadAverage";
		double retVal = 0;
		try {
			retVal = remoteOperatingSystem.getSystemLoadAverage();
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
		return retVal;
	}
	
	/////
	
	private boolean isFrameworkEntity(ObjectName objectName) {
		String location = "isFrameworkEntity";
		boolean retVal = false;
		String key = "Destination";
	    String value = objectName.getKeyProperty(key);
	    if(value != null) {
	    	if(value.startsWith("ducc.")) {
	    		retVal = true;
	    		duccLogger.debug(location, jobid, key+"="+value);
	      	}
	    }
		return retVal;
	}
	
	private String getName(ObjectName objectName) {
		String retVal = "";
		String key = "Destination";
	    String value = objectName.getKeyProperty(key);
	    retVal = value;
		return retVal;
	}
	
	private String getType(ObjectName objectName) {
		String retVal = "";
		String key = "Type";
	    String value = objectName.getKeyProperty(key);
	    retVal = value;
		return retVal;
	}
	
	private boolean isBrokerInfo(ObjectName objectName) {
		boolean retVal = false;
		String key = "type";
	    String value = objectName.getKeyProperty(key);
	    if(value != null) {
	    	if(value.startsWith("Broker")) {
	    		retVal = true;
	       	}
	    }
		return retVal;
	}
	
	public String getAttribute(BrokerAttribute qa) {
		String location = "getAttribute";
		String retVal = "";
		try {
			Set<ObjectName> objectNames = new TreeSet<ObjectName>(mbsc.queryNames(null, null));
			for (ObjectName objectName : objectNames) {
				if(isBrokerInfo(objectName)) {
					String[] attrNames = { qa.name() };
					AttributeList  attributeList = mbsc.getAttributes(objectName, attrNames);
				    for(Object object : attributeList) {
				    	Attribute attribute = (Attribute) object;
						retVal = ""+attribute.getValue();
				   	}
				    break;
				}
			}
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
		return retVal;
	}
	
	public TreeMap<String,String> getAttributes(String name, String[] attrNames) {
		String location = "getAttributes";
		TreeMap<String,String> retVal = new TreeMap<String,String>();
		try {
			Set<ObjectName> objectNames = new TreeSet<ObjectName>(mbsc.queryNames(null, null));
			for (ObjectName objectName : objectNames) {
				String topicName = getName(objectName);
				if(topicName != null) {
					if(topicName.equals(name)) {
						AttributeList  attributeList = mbsc.getAttributes(objectName, attrNames);
					    for(Object object : attributeList) {
					    	Attribute attribute = (Attribute) object;
					    	String attrName = attribute.getName();
							String attrValue = ""+attribute.getValue();
							retVal.put(attrName, attrValue);
							duccLogger.debug(location, jobid, attrName+"="+attrValue);
					   	}
					    break;
					}
				}
			}
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
		return retVal;
	}

	public ArrayList<EntityInfo> getFrameworkEntities() {
		String location = "getFrameworkTopicNames";
		ArrayList<String> list = new ArrayList<String>();
		ArrayList<EntityInfo> retVal = new ArrayList<EntityInfo>();
		try {
			Set<ObjectName> objectNames = new TreeSet<ObjectName>(mbsc.queryNames(null, null));
			for (ObjectName objectName : objectNames) {
			    if(isFrameworkEntity(objectName)) {
			    	String name = getName(objectName);
			    	String type = getType(objectName);
			    	EntityInfo entityInfo = new EntityInfo(name,type);
			    	String key = entityInfo.getKey();
			    	if(!list.contains(key)) {
			    		retVal.add(entityInfo);
			    	}
			    }
			}
		}
		catch(Exception e) {
			duccLogger.error(location, jobid, e);
		}
		return retVal;
	}

}