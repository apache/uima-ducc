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
package org.apache.uima.ducc.agent.launcher;

import java.io.Serializable;

public class ManagedServiceInfo implements Serializable {
  public enum ServiceState {
    STARTING, INITIALIZING, READY, FAILED, STOPPING, STOPPED, KILLED
  };

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  private String pid;

  private ServiceState state;

  /**
   * @return the state
   */
  public ServiceState getState() {
    return state;
  }

  /**
   * @param state
   *          the state to set
   */
  public void setState(ServiceState state) {
    this.state = state;
  }

  /**
   * @return the pid
   */
  public String getPid() {
    return pid;
  }

  /**
   * @param pid
   *          the pid to set
   */
  public void setPid(String pid) {
    this.pid = pid;
  }

  public String toString() {
    return "PID:" + getPid() + " State:" + getState();
  }
}
