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
package org.apache.uima.ducc.orchestrator.maintenance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;

import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.uima.ducc.common.exception.DuccConfigurationException;
import org.apache.uima.ducc.common.exception.DuccRuntimeException;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.DuccPropertiesHelper;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.id.DuccId;

public class MqHelper {
	
	private static final DuccLogger logger = DuccLoggerComponents.getOrLogger(MqReaper.class.getName());

	private static DuccId jobid = null;

	private String broker_hostname = null;
	private String broker_jmx_port = null;
	private String broker_name = null;
	private String broker_url = null;
	private String objectName = null;
	private ObjectName activeMQ = null;
	private BrokerViewMBean mbean = null;
	private JMXServiceURL url = null;
	private JMXConnector jmxc = null;
	private MBeanServerConnection conn = null;
	
	private DuccPropertiesResolver duccPropertiesResolver = null;
	
	public MqHelper() throws IOException, MalformedObjectNameException, NullPointerException, DuccConfigurationException {
		resolve();
		init();
		connect();
	}
	
	public MqHelper(String broker_hostname, String broker_jmx_port, String broker_name) throws DuccRuntimeException, DuccConfigurationException, MalformedObjectNameException, NullPointerException, IOException {
		nonempty("broker_host_name", broker_hostname);
		nonempty("broker_jmx_port", broker_jmx_port);
		nonempty("broker_name", broker_name);
		init();
		connect();
	}
	
	public String get_broker_hostname() {
		return broker_hostname;
	}
	
	public String get_broker_jmx_port() {
		return broker_jmx_port;
	}
	
	public String get_broker_name() {
		return broker_name;
	}
	
	public String get_broker_url() {
		return broker_url;
	}
	
	private void nonempty(String key, String value) throws DuccRuntimeException {
		if(value == null) {
			throw new DuccRuntimeException("missing value for "+key);
		}
	}
	
	private void init() throws DuccConfigurationException {
		String location = "init";
		broker_url = "service:jmx:rmi:///jndi/rmi://"+broker_hostname+":"+broker_jmx_port+"/jmxrmi";
		logger.info(location, jobid, broker_url);
		objectName = "org.apache.activemq:BrokerName="+broker_name+",Type=Broker";
		logger.info(location, jobid, objectName);
	}
	
	private void resolve() throws DuccConfigurationException {
		if(broker_hostname == null) {
			broker_hostname = getDuccProperty(DuccPropertiesResolver.ducc_broker_hostname);
		}
		if(broker_jmx_port == null) {
			broker_jmx_port = getDuccProperty(DuccPropertiesResolver.ducc_broker_jmx_port);
		}
		if(broker_name == null) {
			broker_name = getDuccProperty(DuccPropertiesResolver.ducc_broker_name);
		}
	}
	
	private void configure() {
		duccPropertiesResolver = DuccPropertiesHelper.configure();
	}
	
	private String getDuccProperty(String key) throws DuccConfigurationException {
		if(duccPropertiesResolver == null) {
			configure();
		}
		String value = duccPropertiesResolver.getFileProperty(key);
		if(value == null) {
			throw new DuccConfigurationException("ducc.properties missing "+key);
		}
		value = value.trim();
		if(value.length() == 0) {
			throw new DuccConfigurationException("ducc.properties undefined "+key);
		}
		return value;
	}
	
	private void connect() throws IOException, MalformedObjectNameException, NullPointerException {
		url = new JMXServiceURL(broker_url);
		jmxc = JMXConnectorFactory.connect(url);
		conn = jmxc.getMBeanServerConnection();
		activeMQ = new ObjectName(objectName);
		mbean = (BrokerViewMBean) MBeanServerInvocationHandler.newProxyInstance(conn, activeMQ, BrokerViewMBean.class, true);
	}
	
	private void reconnect() {
		String location = "reconnect";
		try {
			if(jmxc != null) {
				jmxc.close();
			}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
		try {
			connect();
			logger.info(location, jobid, "");
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
	}
	
	public String getBrokerUrl() {
		return broker_url;
	}
	
	private boolean isEqual(String a, String b) {
		boolean retVal = false;
		if(a != null) {
			if(b != null) {
				if(a.equals(b)) {
					retVal = true;
				}
			}
		}
		return retVal;
	}
	
	private ObjectName[] getQueues() {
		ObjectName[] queues = null;
		try {
			queues = mbean.getQueues();
		}
		catch(Throwable t) {
			reconnect();
			queues = mbean.getQueues();
		}
		return queues;
	}
	
	public ArrayList<String> getQueueList() {
		ArrayList<String> qNames = new ArrayList<String>();
		ObjectName[] queues = getQueues();
		if(queues != null) {
			for( ObjectName queue : queues ) {
				Hashtable<String, String> propertyTable = queue.getKeyPropertyList();
				if(propertyTable != null) {
					String type = propertyTable.get("Type");
					String destination = propertyTable.get("Destination");
					if(isEqual(type, "Queue")) {
						qNames.add(destination);
					}
				}
			}
		}
		return qNames;
	}
	
	public boolean removeQueue(String qName) throws Exception {
		boolean retVal = false;
		mbean.removeQueue(qName);
		retVal = true;
		return retVal;
	}
	
	public static void main(String[] args) {
		try {
			MqHelper mqManager;
			mqManager = new MqHelper();
			System.out.println(mqManager.getBrokerUrl());
			ArrayList<String> qList = mqManager.getQueueList();
			for(String qName : qList) {
				System.out.println(qName);
			}
		} catch (MalformedObjectNameException e) {
			e.printStackTrace();
		} catch (NullPointerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (DuccConfigurationException e) {
			e.printStackTrace();
		}
	}
}
