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

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.uima.ducc.common.agent.metrics.memory.DuccNodeMemory;
import org.apache.uima.ducc.common.agent.metrics.memory.NodeMemory;


public class DefaultNodeMemoryCollector implements CallableMemoryCollector{
	private static final String TOTAL_MEMORY_SIZE="getTotalPhysicalMemorySize";
	private static final String FREE_PHYSICAL_MEMORY_SIZE="getFreePhysicalMemorySize";
	private static final String FREE_SWAP_MEMORY_SIZE="getFreeSwapSpaceSize";
	private static final String TOTAL_SWAP_SPACE_SIZE="getTotalSwapSpaceSize";
	private long fakeMemorySize = -1;

	public DefaultNodeMemoryCollector() {
		String tmp;
		if ((tmp = System
				.getProperty("ducc.agent.node.metrics.fake.memory.size")) != null) {
			try {
				fakeMemorySize = Long.parseLong(tmp);
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
		}
	}
	  public NodeMemory call() throws Exception {
		    return collect();
	  }
	  private void setMetric(String metricName, long metricValue, DuccNodeMemory nodeMemory) {
		  try {
              if ( fakeMemorySize != -1 ) {
                  nodeMemory.setMemTotal(fakeMemorySize);
              } else {
                  nodeMemory.setMemTotal(Runtime.getRuntime().totalMemory());
              }
		  } catch( Exception e) {
			  e.printStackTrace();
		  }
		  if ( metricName.equalsIgnoreCase(FREE_PHYSICAL_MEMORY_SIZE)) {
			  nodeMemory.setMemFree(metricValue);
		  } else if ( metricName.equalsIgnoreCase(FREE_SWAP_MEMORY_SIZE)) {
			  nodeMemory.setSwapFree(metricValue);
		  } else if ( metricName.equalsIgnoreCase(TOTAL_SWAP_SPACE_SIZE)) {
			  nodeMemory.setSwapTotal(metricValue);
		  } 
	  }
	  private DuccNodeMemory collect() {
		  DuccNodeMemory nodeMemory = new DuccNodeMemory();
		  OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();
		  for (Method method : operatingSystemMXBean.getClass().getDeclaredMethods()) {
		    method.setAccessible(true);
		    if (method.getName().startsWith("get") 
		        && Modifier.isPublic(method.getModifiers())) {
		            Long value;
		        try {
		            value = (Long)method.invoke(operatingSystemMXBean);
		        } catch (Exception e) {
		            value = 0L;
		        } // try
		        setMetric(method.getName(), value, nodeMemory);
		    } // if
		  } // for
		  return nodeMemory;
		}

	  public static void main(String[] args) {
		  try {
			  DefaultNodeMemoryCollector collector = new DefaultNodeMemoryCollector();
			  ExecutorService pool = Executors.newFixedThreadPool(1);
		      Future<NodeMemory> nmiFuture = pool.submit(collector);
		      NodeMemory nodeMemory = nmiFuture.get();
		      System.out.println("Total Memory:"+nodeMemory.getMemTotal());
		      System.out.println("Total Free Memory:"+nodeMemory.getMemFree());
		      System.out.println("Total Swap Space Size:"+nodeMemory.getSwapTotal());
		      System.out.println("Total Free Swap Space Size:"+nodeMemory.getSwapFree());
		      
		  } catch( Exception e) {
			  e.printStackTrace();
		  }
	  }
}
