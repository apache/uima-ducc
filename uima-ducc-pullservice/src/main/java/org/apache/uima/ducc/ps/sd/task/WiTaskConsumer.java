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

package org.apache.uima.ducc.ps.sd.task;

import org.apache.uima.ducc.ps.sd.task.iface.TaskConsumer;
import org.apache.uima.ducc.ps.net.iface.IMetaTaskRequester;

public class WiTaskConsumer implements TaskConsumer {

  private static final long serialVersionUID = 1L;

  private String serviceType;

  private String host;

  private String pid;

  private String threadId;

  public WiTaskConsumer(IMetaTaskRequester metaTaskRequester) {
    this.serviceType = metaTaskRequester.getRequesterProcessName();
    this.host = metaTaskRequester.getRequesterNodeName();
    this.pid = String.valueOf(metaTaskRequester.getRequesterProcessId());
    this.threadId = String.valueOf(metaTaskRequester.getRequesterThreadId());
  }

  public String getType() {
    return serviceType;
  }

  public String getHostName() {
    return host;
  }

  public String getPid() {
    return pid;
  }

  public String getThreadId() {
    return threadId;
  }

  public String toString() {
    StringBuffer sb = new StringBuffer();
    if (host != null) {
      sb.append(host);
      sb.append(".");
    }
    sb.append(pid);
    sb.append(".");
    sb.append(threadId);
    return sb.toString();
  }

  public int hashCode() {
    final int prime = 31;
    int result = 1;
    String thisNodeName = this.host;
    Integer thisPid = new Integer(this.pid);
    Integer thisTid = new Integer(this.threadId);
    result = prime * result + ((thisNodeName == null) ? 0 : thisNodeName.hashCode());
    result = prime * result + ((thisPid == null) ? 0 : thisPid.hashCode());
    result = prime * result + ((thisTid == null) ? 0 : thisTid.hashCode());
    return result;
  }

  public boolean equals(Object obj) {
    boolean retVal = false;
    try {
      if (obj != null) {
        if (this == obj) {
          retVal = true;
        } else {
          WiTaskConsumer that = (WiTaskConsumer) obj;
          if (this.compareTo(that) == 0) {
            retVal = true;
          }
        }
      }
    } catch (Exception e) {
    }
    return retVal;
  }

  public int compareTo(Object o) {
    int retVal = 0;
    try {
      if (o != null) {
        WiTaskConsumer that = (WiTaskConsumer) o;
        if (retVal == 0) {
          retVal = compareHost(that);
        }
        if (retVal == 0) {
          retVal = comparePid(that);
        }
        if (retVal == 0) {
          retVal = compareThreadId(that);
        }
      }
    } catch (Exception e) {
    }
    return retVal;
  }

  private int compareHost(WiTaskConsumer that) {
    int retVal = 0;
    String thisNodeName = this.getHostName();
    String thatNodeName = that.getHostName();
    if (thisNodeName != null) {
      if (thatNodeName != null) {
        retVal = thisNodeName.compareTo(thatNodeName);
      }
    }
    return retVal;
  }

  private int comparePid(WiTaskConsumer that) {
    int retVal = 0;
    Integer thisPid = new Integer(this.pid);
    Integer thatPid = new Integer(that.pid);
    retVal = thisPid.compareTo(thatPid);
    return retVal;
  }

  private int compareThreadId(WiTaskConsumer that) {
    int retVal = 0;
    Integer thisTid = new Integer(this.threadId);
    Integer thatTid = new Integer(that.threadId);
    retVal = thisTid.compareTo(thatTid);
    return retVal;
  }
}
