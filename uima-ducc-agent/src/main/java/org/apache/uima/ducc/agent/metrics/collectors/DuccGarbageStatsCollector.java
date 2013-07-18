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
  DuccLogger logger=null;
  IDuccProcess process=null;
	public DuccGarbageStatsCollector(DuccLogger logger,IDuccProcess process)  {
	  this.logger = logger;
	  this.process = process;
	  try {
		  if (process != null && process.getProcessJmxUrl() != null && process.getProcessJmxUrl().trim().length() > 0 ) {
			    connection = getServerConnection();
		  }
	  } catch( Exception e) {
		  logger.error("DuccGarbageStatsCollector.ctor", null,e);
		  logger.error("DuccGarbageStatsCollector.ctor", null, "Failed to Connect via JMX to PID:"+process.getPID()+" Reason:\n"+e);
	  }
	  
	}
  private MBeanServerConnection getServerConnection() throws Exception {
    System.out.println("Connecting Monitor To Broker - URL:"+process.getProcessJmxUrl());
    JMXServiceURL url = new JMXServiceURL(process.getProcessJmxUrl());
    JMXConnector jmxc = JMXConnectorFactory.connect(url, null);
    return jmxc.getMBeanServerConnection();
  }
	public ProcessGarbageCollectionStats collect() {
    ProcessGarbageCollectionStats gcStats =
            new ProcessGarbageCollectionStats();
	  
	  try {
	    Set<ObjectInstance> mbeans= 
	            connection.queryMBeans(new ObjectName("java.lang:type=GarbageCollector,*"),null );
	    Long totalCollectionCount= new Long(0);
	    Long totalCollectionTime=new Long(0);
	    
	    for( ObjectInstance gcObject : mbeans) {
	      String gcCollectorName = gcObject.getObjectName().getCanonicalKeyPropertyListString();
	      ObjectName memoryManagerMXBean = 
	              new ObjectName("java.lang:" + gcCollectorName);
	      totalCollectionCount =+ (Long) connection.getAttribute(memoryManagerMXBean,"CollectionCount");
	      totalCollectionTime =+ (Long) connection.getAttribute(memoryManagerMXBean,"CollectionTime");
	    }
      // Returns the total number of collections that have occurred.
      gcStats.setCollectionCount(totalCollectionCount);
      // Returns the approximate accumulated collection elapsed time in milliseconds.
      gcStats.setCollectionTime(totalCollectionTime);

	    
	  } catch( Exception e) {
	    logger.error("", null, "Failed to Fetch JMX GC Stats From PID:"+process.getPID()+" Reason:\n"+e);
	  }
	  
	  
//	   List<GarbageCollectorMXBean> gcmb = ManagementFactory.getGarbageCollectorMXBeans();
//	   for( GarbageCollectorMXBean gcBean : gcmb ) {
//		  gcStats.setMemoryManagerName(gcBean.getName());
//		  // Returns the total number of collections that have occurred.
//		  gcStats.setCollectionCount(gcBean.getCollectionCount());
//		  // Returns the approximate accumulated collection elapsed time in milliseconds.
//		  gcStats.setCollectionTime(gcBean.getCollectionTime());
	  //}
	  return gcStats;
	}
}
