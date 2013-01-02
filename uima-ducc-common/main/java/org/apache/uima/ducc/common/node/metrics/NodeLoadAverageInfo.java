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

import java.io.Serializable;


public class NodeLoadAverageInfo  extends ByteBufferParser 
implements NodeLoadAverage, Serializable{

  private static final long serialVersionUID = -8709992836807543890L;
  private static final int LOADAVG1=0;
  private static final int LOADAVG5=1;
  private static final int LOADAVG15=2;
  private static final int CURRENTRUNNABLECOUNT=3;
  
  public NodeLoadAverageInfo(byte[] buffer, int[] procLoadAvgFieldOffsets, int[]  procLoadAvgFieldLengths) {
    super(buffer, procLoadAvgFieldOffsets, procLoadAvgFieldLengths);
  }
  public String getLoadAvg1() {
    return super.getFieldAsString(LOADAVG1);
  }
  public String getLoadAvg5() {
    return super.getFieldAsString(LOADAVG5);
  }
  public String getLoadAvg15() {
    return super.getFieldAsString(LOADAVG15);
  }
  public String getCurrentRunnableProcessCount() {
    String[] tokens = super.getFieldAsString(CURRENTRUNNABLECOUNT).split("/"); 
    return tokens[0];
  }
  public String getTotalProcessCount() {
    String[] tokens = super.getFieldAsString(CURRENTRUNNABLECOUNT).split("/"); 
    return tokens[1];
  }

}
