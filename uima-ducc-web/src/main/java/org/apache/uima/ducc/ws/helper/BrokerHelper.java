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
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadMXBean;
import java.util.Hashtable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.InstanceNotFoundException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.openmbean.CompositeData;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.ws.DuccDaemonsData;

public class BrokerHelper extends JmxHelper {

	private static DuccLogger logger = DuccLogger.getLogger(BrokerHelper.class);
	private static DuccId jobid = null;

	public static BrokerHelper getInstance() {
		return new BrokerHelper();
	}

	private OperatingSystemMXBean remoteOperatingSystem;
	private ThreadMXBean remoteThread;

	private int threadsLive = 0;
	private int threadsPeak = 0;
	private double systemLoadAverage = 0;
	private long memoryUsed = 0;
	private long memoryMax = 0;
	private String brokerVersion = "?";
	private String brokerUptime = "?";
	
	private long startTime = 0;
	
	private Map<String,Map<String,String>> entityAttributes = null;
	
	public enum JmxKeyWord { Destination, Type, Topic, Queue };
	
	public enum FrameworkAttribute { ConsumerCount, QueueSize, MaxEnqueueTime, AverageEnqueueTime, MemoryPercentUsage };
	
	private enum BrokerAttribute { BrokerVersion, Uptime };
	
	private String[] topicAttributeNames = {
			FrameworkAttribute.ConsumerCount.name(),
			FrameworkAttribute.QueueSize.name(),
			FrameworkAttribute.MaxEnqueueTime.name(),
			FrameworkAttribute.AverageEnqueueTime.name(),
			FrameworkAttribute.MemoryPercentUsage.name(),
	};
	
	private String[] brokerAttributeNames = {
			BrokerAttribute.BrokerVersion.name(),
			BrokerAttribute.Uptime.name(),
	};
	
	private BrokerHelper() {
		initProperties();
		init();
	}

	private BrokerHelper(String host, String port) {
		setHost(host);
		setPort(port);
		init();
	}
	
	private void initProperties() {
		DuccPropertiesResolver duccPropertiesResolver = DuccPropertiesResolver.getInstance();
		String key;
		String value;
		//
		key = "ducc.broker.name";
		value = duccPropertiesResolver.getCachedProperty(key);
		setHost(value);
		//
		key = "ducc.broker.jmx.port";
		value = duccPropertiesResolver.getCachedProperty(key);
		setPort(value);
	}
	
	private void init() {
		String location = "init";
		try {
			connect();
			populate();
			disconnect();
		} 
		catch (Exception e) {
			logger.error(location, jobid, e);
		}
	}
	
	private void populateRemoteOperatingSystem() {
		String location = "populateRemoteOperatingSystem";
		try {
			remoteOperatingSystem = ManagementFactory.newPlatformMXBeanProxy(
				getMBSC(),
				ManagementFactory.OPERATING_SYSTEM_MXBEAN_NAME,
				OperatingSystemMXBean.class);
		} 
		catch (Exception e) {
			logger.error(location, jobid, e);
		}
	}
	
	private void populateRemoteThread() {
		String location = "populateRemoteThread";
		try {
			remoteThread = ManagementFactory.newPlatformMXBeanProxy(
				getMBSC(),
				ManagementFactory.THREAD_MXBEAN_NAME,
				ThreadMXBean.class);
		} 
		catch (Exception e) {
			logger.error(location, jobid, e);
		}
	}
	
	private void populateOperatingSystem() {
		String location = "populateOperatingSystem";
		try {
			systemLoadAverage = remoteOperatingSystem.getSystemLoadAverage();
		} 
		catch (Exception e) {
			logger.error(location, jobid, e);
		}
	}
	
	private void populateThreads() {
		String location = "populateThreads";
		try {
			threadsLive = remoteThread.getThreadCount();
			threadsPeak = remoteThread.getPeakThreadCount();
		} 
		catch (Exception e) {
			logger.error(location, jobid, e);
		}
	}
	
	private void populateMemory() {
		String location = "populateMemory";
		try {
			Object o = getMBSC().getAttribute(new ObjectName("java.lang:type=Memory"), "HeapMemoryUsage");
			CompositeData cd = (CompositeData) o;
			memoryUsed = (Long) cd.get("used");
			memoryMax = (Long) cd.get("max");
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
	}
	
	private void populateRuntime() {
		String location = "populateRuntime";
		try {
			Object o;
			o = getMBSC().getAttribute(new ObjectName("java.lang:type=Runtime"), "StartTime");
			startTime = (Long) o;
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
	}
	
	private void populateAttributes() {
		String location = "populateAttributes";
		try {
			entityAttributes = search();
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
	}
	
	private void populate() {
		populateRemoteOperatingSystem();
		populateRemoteThread();
		//
		populateOperatingSystem();
		populateThreads();
		populateMemory();
		populateRuntime();
		populateAttributes();
	}
	
	//
	
	private boolean match(String s0, String s1) {
		boolean retVal = false;
		if(s0 != null) {
			if(s1 != null) {
				retVal = s0.equals(s1);
			}
		}
		return retVal;
	}
	
	private boolean start(String s0, String s1) {
		boolean retVal = false;
		if(s0 != null) {
			if(s1 != null) {
				retVal = s0.startsWith(s1);
			}
		}
		return retVal;
	}

	private boolean isQueue(Hashtable<String, String> plist) {
		boolean retVal = false;
		if(plist != null) {
			String text = plist.get("type");
			if(text == null) {
				text = plist.get("Type");
			}
			retVal = match("Queue",text);
		}
		return retVal;
	}
	
	private boolean isTopic(Hashtable<String, String> plist) {
		boolean retVal = false;
		if(plist != null) {
			String text = plist.get("type");
			if(text == null) {
				text = plist.get("Type");
			}
			retVal = match("Topic",text);
		}
		return retVal;
	}
	
	private boolean isEligible(Hashtable<String, String> plist) {
		boolean retVal = isTopic(plist) || isQueue(plist);
		return retVal;
	}
	
	private void conditionalAdd(Map<String,Map<String,String>> map, ObjectName objectName) throws InstanceNotFoundException, ReflectionException, IOException {
		String key = getBrokerVersion();
		if(key != null) {
			if(key.equals("5.7.0")) {
				conditionalAdd5_7_0(map, objectName);
			}
			else {
				conditionalAdd5_13_2(map, objectName);
			}
		}
		else {
			conditionalAdd5_13_2(map, objectName);
		}
	}
	
	private void conditionalAdd5_13_2(Map<String,Map<String,String>> map, ObjectName objectName) throws InstanceNotFoundException, ReflectionException, IOException {
		String location = "conditionalAdd5_13_2";
		if(map != null) {
			if(objectName != null) {
				String dName = null;
				String dType = objectName.getKeyProperty("destinationType");
				if(dType != null) {
					if(dType.equals("Topic") || dType.equals("Queue")) {
						dName = objectName.getKeyProperty("destinationName");
						if(dName != null) {
							if(dName.startsWith("ducc.")) {
								logger.trace(location, jobid, dType+": "+dName);
								Map<String,String> attributes = new TreeMap<String,String>();
								AttributeList  attributeList = getMBSC().getAttributes(objectName, topicAttributeNames);
								for(Object object : attributeList) {
								   	Attribute attribute = (Attribute) object;
								   	String attrName = attribute.getName();
									String attrValue = ""+attribute.getValue();
									attributes.put(attrName, attrValue);
									logger.trace(location, jobid, attrName+"="+attrValue);
							   	}
								String key = JmxKeyWord.Type.name();
								String value = dType;
								attributes.put(key, value);
								map.put(dName, attributes);
							}
							else {
								logger.trace(location, jobid, dType+": "+dName+" "+"skip");
							}
						}
					}
				}
			}
		}
	}
	
	@Deprecated
	private void conditionalAdd5_7_0(Map<String,Map<String,String>> map, ObjectName objectName) throws InstanceNotFoundException, ReflectionException, IOException {
		String location = "conditionalAdd5_7_0";
		if(map != null) {
			if(objectName != null) {
				Hashtable<String, String> plist = objectName.getKeyPropertyList();
				if(isEligible(plist)) {
					String name = plist.get(JmxKeyWord.Destination.name());
					String prefix = "ducc.";
					if(start(name,prefix)) {
						Map<String,String> attributes = new TreeMap<String,String>();
						AttributeList  attributeList = getMBSC().getAttributes(objectName, topicAttributeNames);
						for(Object object : attributeList) {
						   	Attribute attribute = (Attribute) object;
						   	String attrName = attribute.getName();
							String attrValue = ""+attribute.getValue();
							attributes.put(attrName, attrValue);
							logger.trace(location, jobid, attrName+"="+attrValue);
					   	}
						String key = JmxKeyWord.Type.name();
						String value = plist.get(key);
						attributes.put(key, value);
						map.put(name, attributes);
					}
				}
				else {
					logger.trace(location, jobid, "skip: "+objectName);
				}		
			}
		}
	}
	
	private Map<String,Map<String,String>> search() throws IOException, InstanceNotFoundException, ReflectionException {
		Map<String,Map<String,String>> map = new TreeMap<String,Map<String,String>>();
		Set<ObjectName> objectNames = new TreeSet<ObjectName>(getMBSC().queryNames(null, null));
		for (ObjectName objectName : objectNames) {
			brokerAdd(objectName);
		}
		for (ObjectName objectName : objectNames) {
			conditionalAdd(map,objectName);
		}
		return map;
	}
	
	private void brokerAdd(ObjectName objectName) throws InstanceNotFoundException, ReflectionException, IOException  {
		if(objectName != null) {
			Hashtable<String, String> plist = objectName.getKeyPropertyList();
			if(plist != null) {
				String s0 = plist.get("type");
				if(s0 == null) {
					s0 = plist.get("Type");
				}
				String s1 = "Broker";
				if(match(s0,s1)) {
					AttributeList  attributeList = getMBSC().getAttributes(objectName, brokerAttributeNames);
					for(Object object : attributeList) {
						Attribute attribute = (Attribute) object;
					   	String attrName = attribute.getName();
						String attrValue = ""+attribute.getValue();
						if(attrName.equals(BrokerAttribute.BrokerVersion.name())) {
							brokerVersion = attrValue;
						}
						else if(attrName.equals(BrokerAttribute.Uptime.name())) {
							brokerUptime = attrValue;
						}
					}
				}
			}
		}
	}
	
	//
	
	private void setHost(String value) {
		setJmxHost(value);
	}

	public String getHost() {
		return getJmxHost();
	}

	private void setPort(String value) {
		String location = "setPort";
		try {
			setJmxPort(Integer.parseInt(value));
		} 
		catch (Exception e) {
			logger.error(location, jobid, e);
		}
	}

	public int getPort() {
		return getJmxPort();
	}


	
	// Operating System Info //

	public double getSystemLoadAverage() {
		return systemLoadAverage;
	}
	
	// Threads Info //
	
	public int getThreadsLive() {
		return threadsLive;
	}
	
	public int getThreadsPeak() {
		return threadsPeak;
	}
	
	// JVM
	
	public Long getMemoryUsed() {
		return memoryUsed;
	}
	
	public Long getMemoryMax() {
		return memoryMax;
	}
	
	// Broker
	
	public String getBrokerVersion() {
		return brokerVersion;
	}
	
	public String getBrokerUptime() {
		return brokerUptime;
	}
	
	// Topics & Queues
	
	public Map<String,Map<String,String>> getEntityAttributes() {
		return entityAttributes;
	}
	
	// Runtime Info //
	
	public long getStartTime() {
		return startTime;
	}
	
	/**
	 * @return true if regular self-publications via broker are being received, 
	 *         false otherwise
	 */
	public boolean isAlive() {
		boolean retVal = DuccDaemonsData.getInstance().isWsPublicationOntime();
		return retVal;
	}
	
	// Command Line
	
	private static String parse(String[] args, String key) {
		String retVal = null;
		if(args != null) {
			for(String arg:args) {
				String[] pair = arg.trim().split("=");
				if(pair.length == 2) {
					if(pair[0].equals(key)) {
						retVal = pair[1];
					}
				}
			}
		}
		return retVal;
	}
	
	public static void main(String[] args) {
		String host = parse(args, "host");
		if(host == null) {
			System.out.println("host=?");
			return;
		}
		String port = parse(args, "port");
		if(port == null) {
			System.out.println("port=?");
			return;
		}
		BrokerHelper bh = new BrokerHelper(host,port);
		System.out.println("host="+bh.getHost());
		System.out.println("port="+bh.getPort());
		System.out.println("BrokerVersion="+bh.getBrokerVersion());
		System.out.println("BrokerUptime="+bh.getBrokerUptime());
		System.out.println("MemoryUsed(MB)="+bh.getMemoryUsed());
		System.out.println("MemoryMax(MB)="+bh.getMemoryMax());
		System.out.println("ThreadsLive="+bh.getThreadsLive());
		System.out.println("ThreadsPeak="+bh.getThreadsPeak());
		System.out.println("SystemLoadAverage="+bh.getSystemLoadAverage());
		Map<String, Map<String, String>> map = bh.getEntityAttributes();
		if(map != null) {
			if(!map.isEmpty()) {
				for(Entry<String, Map<String, String>> entry : map.entrySet()) {
					System.out.println(entry.getKey()+":");
					Map<String, String> attributes = entry.getValue();
					for(Entry<String, String> attribute : attributes.entrySet()) {
						System.out.println(attribute.getKey()+"="+attribute.getValue());
					}
				}
			}
			else {
				System.out.println("map=empty");
			}
		}
		else {
			System.out.println("map=null");
		}
	}

}