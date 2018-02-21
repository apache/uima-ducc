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
package org.apache.uima.ducc.agent.metrics.collectors;

import java.util.Set;

import javax.management.MBeanServerConnection;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.uima.ducc.common.node.metrics.ProcessGarbageCollectionStats;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;

public class DuccGarbageStatsCollector {
	MBeanServerConnection connection = null;
	DuccLogger logger = null;
	IDuccProcess process = null;
	JmxUrl jmxUrl = null;
	private boolean reportJmxUrl = true;

	public DuccGarbageStatsCollector(DuccLogger logger, IDuccProcess process) {
		this.logger = logger;
		this.process = process;
		try {
			jmxUrl = new JmxUrl(process);
		} catch (Exception e) {
			logger.error("DuccGarbageStatsCollector.ctor", null, e);
		}

	}

	private MBeanServerConnection getServerConnection() throws Exception {
		logger.info("DuccGarbageStatsCollector.getServerConnection()", null,
				"Connecting GC collector to remote child process - URL:"
						+ jmxUrl.get());
		JMXServiceURL url = new JMXServiceURL(jmxUrl.get()); 
		JMXConnector jmxc = JMXConnectorFactory.connect(url, null);
		return jmxc.getMBeanServerConnection();
	}

	public ProcessGarbageCollectionStats collect() {
		ProcessGarbageCollectionStats gcStats = new ProcessGarbageCollectionStats();
		try {
			if (connection == null) {
				// if there is a problem here, an exception will be thrown.
				// The connection should never be null if getServerConnection()
				// returns without an exception.
				connection = getServerConnection();
			}
			Set<ObjectInstance> mbeans = connection.queryMBeans(
					new ObjectName("java.lang:type=GarbageCollector,*"),
					null);
			Long totalCollectionCount = new Long(0);
			Long totalCollectionTime = new Long(0);

			for (ObjectInstance gcObject : mbeans) {
				String gcCollectorName = gcObject.getObjectName()
						.getCanonicalKeyPropertyListString();
				ObjectName memoryManagerMXBean = new ObjectName(
						"java.lang:" + gcCollectorName);
				totalCollectionCount += (Long) connection.getAttribute(
						memoryManagerMXBean, "CollectionCount");
				totalCollectionTime += (Long) connection.getAttribute(
						memoryManagerMXBean, "CollectionTime");
			}
			// Returns the total number of collections that have occurred.
			gcStats.setCollectionCount(totalCollectionCount);
			// Returns the approximate accumulated collection elapsed time
			// in milliseconds.
			gcStats.setCollectionTime(totalCollectionTime);
			logger.debug("DuccGarbageStatsCollector.collect()", null,
					"GC Collector Fetch Stats For PID:" + process.getPID()
							+ " GC Count:" + gcStats.getCollectionCount()
							+ " GC Time:" + gcStats.getCollectionTime());

		} catch (Throwable e) {
			// will retry this connection again
			connection = null;
			logger.error("", null, "Failed to Fetch JMX GC Stats From PID:"
					+ process.getPID() + " Reason:\n" + e);
			
			logger.info("DuccGarbageStatsCollector.collect()", null,
					"GC Collector Failed to Connect via JMX to child process PID:"
							+ process.getPID() + " JmxUrl:" + jmxUrl.get()
							+ " Will try to reconnect later");
		}
		
		return gcStats;
	}

	private class JmxUrl {
		IDuccProcess process = null;;

		public JmxUrl(IDuccProcess remoteProcess) {
			this.process = remoteProcess;
		}

		public boolean isAvailable() {
			if (process != null && process.getProcessJmxUrl() != null
					&& process.getProcessJmxUrl().trim().length() > 0) {
				return true;
			}
			return false;
		}

		public String get() {
			if (isAvailable()) {
				if (reportJmxUrl) {
					reportJmxUrl = false; // report this once
					logger.info("JmxUrl.get()", null, "Remote Process JMX URL:"
							+ process.getProcessJmxUrl());
				}
				return process.getProcessJmxUrl();
			} else {
				return "";
			}
		}
	}
}
