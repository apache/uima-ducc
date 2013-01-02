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
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;

import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;


public class MqReaper {
	
	private static final DuccLogger logger = DuccLoggerComponents.getOrLogger(MqReaper.class.getName());
	
	private static MqReaper mqReaper = new MqReaper();
	
	public static MqReaper getInstance() {
		return mqReaper;
	}
	
	private String ducc_broker_name = DuccPropertiesResolver.getInstance().getFileProperty(DuccPropertiesResolver.ducc_broker_name);
	private String ducc_broker_jmx_port = DuccPropertiesResolver.getInstance().getFileProperty(DuccPropertiesResolver.ducc_broker_jmx_port);
	private String ducc_broker_url = "service:jmx:rmi:///jndi/rmi://"+ducc_broker_name+":"+ducc_broker_jmx_port+"/jmxrmi";
	private String ducc_jd_queue_prefix = DuccPropertiesResolver.getInstance().getFileProperty(DuccPropertiesResolver.ducc_jd_queue_prefix);
	private String objectName = "org.apache.activemq:BrokerName="+ducc_broker_name+",Type=Broker";
	
	private JMXServiceURL url;
	private JMXConnector jmxc;
	private MBeanServerConnection conn;
	private ObjectName activeMQ;
	private BrokerViewMBean mbean;
	
	private boolean mqConnected = false;
	
	public MqReaper() {
		init();
	}
	
	private void init() {
		String location = "init";
		if(ducc_broker_name == null) {
			ducc_broker_name = "localhost";
		}
		if(ducc_broker_jmx_port == null) {
			ducc_broker_jmx_port = "1099";
		}
		if(ducc_jd_queue_prefix == null) {
			ducc_broker_jmx_port = "ducc.jd.queue.";
		}
		logger.info(location,null,DuccPropertiesResolver.ducc_broker_name+":"+ducc_broker_name);
		logger.info(location,null,DuccPropertiesResolver.ducc_broker_jmx_port+":"+ducc_broker_jmx_port);
		logger.info(location,null,DuccPropertiesResolver.ducc_broker_url+":"+ducc_broker_url);
		logger.info(location,null,DuccPropertiesResolver.ducc_jd_queue_prefix+":"+ducc_jd_queue_prefix);
		logger.info(location,null,"objectName"+":"+objectName);
	}
	
	private boolean mqConnect() {
		String location = "mqConnect";
		if(!mqConnected) {
			try {
				url = new JMXServiceURL(ducc_broker_url);
			} 
			catch (MalformedURLException e) {
				logger.error(location, null, e);
				return mqConnected;
			}
			try {
				jmxc = JMXConnectorFactory.connect(url);
			} 
			catch (IOException e) {
				logger.error(location, null, e);
				return mqConnected;
			}
			try {
				conn = jmxc.getMBeanServerConnection();
			} 
			catch (IOException e) {
				logger.error(location, null, e);
				return mqConnected;
			}
			try {
				activeMQ = new ObjectName(objectName);
			} 
			catch (MalformedObjectNameException e) {
				logger.error(location, null, e);
				return mqConnected;
			} 
			catch (NullPointerException e) {
				logger.error(location, null, e);
				return mqConnected;
			}
			mbean = (BrokerViewMBean) MBeanServerInvocationHandler.newProxyInstance(conn, activeMQ, BrokerViewMBean.class, true);
			mqConnected = true;
		}
		return mqConnected;
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
	
	private boolean isStartsWith(String a, String b) {
		boolean retVal = false;
		if(a != null) {
			if(b != null) {
				if(a.startsWith(b)) {
					retVal = true;
				}
			}
		}
		return retVal;
	}
	
	public ArrayList<String> getJdQueues() {
		String location = "getJdQueues";
		ArrayList<String> jdQueues = new ArrayList<String>();
		if(mqConnect()) {
			try {
			ObjectName[] queues = mbean.getQueues();
				for( ObjectName queue : queues ) {
					Hashtable<String, String> propertyTable = queue.getKeyPropertyList();
					if(propertyTable != null) {
						String type = propertyTable.get("Type");
						String destination = propertyTable.get("Destination");
						if(isEqual(type, "Queue")) {
							if(isStartsWith(destination, ducc_jd_queue_prefix)) {
								logger.trace(location, null, "consider:"+destination);
								jdQueues.add(destination);
							}
							else {
								logger.trace(location, null, "skip:"+destination);
							}
						}
						else {
							logger.trace(location, null, "type:"+type+" "+"destination:"+destination);
						}
					}
					else {
						logger.trace(location, null, "propertyTable:"+propertyTable);
					}
				}
			}
			catch(Throwable t) {
				logger.trace(location, null, t);
			}
		}
		return jdQueues;
	}
	
	public void removeUnusedJdQueues(DuccWorkMap workMap) {
		String location = "removeUnusedJdQueues";
		try {
			ArrayList<String> queues = getJdQueues();
			Iterator<DuccId> iterator = workMap.getJobKeySet().iterator();
			while( iterator.hasNext() ) {
				DuccId jobId = iterator.next();
				String jqKeep = ducc_jd_queue_prefix+jobId.getFriendly();
				if(queues.remove(jqKeep)) {
					logger.debug(location, null, "queue keep:"+jqKeep);
				}
				else {
					logger.trace(location, null, "queue not found:"+jqKeep);
				}
			}
			for( String jqDiscard : queues ) {
				logger.info(location, null, "queue discard:"+jqDiscard);
				try {
					mbean.removeQueue(jqDiscard); 
				}
				catch(Throwable t) {
					logger.error(location, null, t);
					mqConnected = false;
				}
			}
		}
		catch(Throwable t) {
			logger.error(location, null, t);
		}
	}
	
	public static void main(String[] args) {
		MqReaper mqr = MqReaper.getInstance();
		DuccWorkMap workMap = new DuccWorkMap();
		mqr.removeUnusedJdQueues(workMap);
	}
	
}
