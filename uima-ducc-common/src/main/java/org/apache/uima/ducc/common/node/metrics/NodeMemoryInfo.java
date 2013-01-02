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
package org.apache.uima.ducc.common.node.metrics;

import org.apache.uima.ducc.common.agent.metrics.memory.NodeMemory;


public class NodeMemoryInfo 
implements NodeMemory {
  /**
   * 
   */
  private static final long serialVersionUID = 4251234672684113872L;

  //	holds fake total memory of a node. This is used for testing only Resource Manager
  private long memTotal=-1;
  private long memInfoValues[];
  
  public NodeMemoryInfo(long memInfoValues[], long fakeMemTotal) {
	  this.memInfoValues = memInfoValues;
	  memTotal = fakeMemTotal;
  }
  public long getMemTotal() {
	  // memTotal is typically set to -1 unless the agent is run in a test mode and when
	  // it is configured to use fake memory total
	  if ( memTotal > 0 ) { // this should be true only for testing
		  return memTotal;
	  } 
	  return memInfoValues[0]; 
  }
  public long getMemFree() {
	  return memInfoValues[1]; 
  }
  public long getBuffers() {
	  return -1;
  }
  public long getCached() {
	  return -1;
  }
  public long getSwapCached() {
	  return -1;
  }
  public long getActive() {
	  return -1;
  }
  public long getInactive() {
	  return -1;
  }
  public long getSwapTotal() {
	  return memInfoValues[2]; 
  }
  public long getSwapFree() {
	  return memInfoValues[3]; 
  }

  public long getHighTotal() {
	  return -1;
  }

  public long getLowTotal() {
	  return -1;
  }

  public long getLowFree() {
	  return -1;
  }
}
