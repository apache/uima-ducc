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
package org.apache.uima.ducc.agent;

import java.io.Serializable;
import java.util.Properties;

public class NodeAgentInfo implements Serializable {
  // UNREACHABLE is set if the agent fails to send a ping within a window
  public enum AgentState {
    INITIALIZING, READY, STOPPED, UNREACHABLE, FAILED
  };

  public static final String OSName = "os.name";

  public static final String OSVersion = "os.version";

  public static final String OSArchitecture = "os.arch";

  public static final String NodeCpus = "node.cpus";

  public static final String JavaVendor = "java.vendor";

  public static final String JavaVersion = "java.version";

  private static final long serialVersionUID = 1L;

  private String hostname;

  private String ip = "N/A";

  private int jmxPort;

  private Properties properties = new Properties();

  private String id;

  // private ProcessGroup[] groups;
  private AgentState status = AgentState.INITIALIZING;

  private String agentLog = "N/A";

  private boolean firstHeartbeat = true;

  public NodeAgentInfo(String hostname, String id) {
    super();
    this.hostname = hostname;
    this.id = id;
  }

  public boolean isFirstHeartbeat() {
    return firstHeartbeat;
  }

  /**
   * @return the status
   */
  public AgentState getStatus() {
    return status;
  }

  public String getAgentLog() {
    return agentLog;
  }

  public void setAgentLog(String agentLog) {
    this.agentLog = agentLog;
  }

  public String getIp() {
    return ip;
  }

  public void setIp(String ip) {
    this.ip = ip;
  }

  /**
   * @param status
   *          the status to set
   */
  public void setStatus(AgentState status) {
    this.status = status;
  }

  public Properties getProperties() {
    return properties;
  }

  public void setProperties(Properties properties) {
    this.properties = properties;
  }

  public String getProperty(String key) {
    return properties.getProperty(key);
  }

  public void setProperty(String key, String value) {
    properties.put(key, value);
  }

  /**
   * @return the jmxPort
   */
  public int getJmxPort() {
    return jmxPort;
  }

  /**
   * @param jmxPort
   *          the jmxPort to set
   */
  public void setJmxPort(int jmxPort) {
    this.jmxPort = jmxPort;
  }

  /**
   * @return the hostname
   */
  public String getHostname() {
    return hostname;
  }

  /**
   * @return the id
   */
  public String getId() {
    return id;
  }

  // public void setProcessGroups(ProcessGroup[] groups) {
  // this.groups = groups;
  // }
  // public ProcessGroup[] getProcessGroups() {
  // return groups;
  // }
  public void dump() {
    if (firstHeartbeat) {
      firstHeartbeat = false;
      System.out.println("+++++++++++ Controller Received Agent Info. \n\tNode:" + getHostname()
              + "\n\tAgent Node IP:" + getIp() + "\n\tAgent Node ID:" + getId() + "\n\tAgent Log:"
              + getAgentLog() + "\n\tAgent Jmx Port:" + getJmxPort() + "\n\tAgent Node OS:"
              + getProperty(NodeAgentInfo.OSName) + "\n\tAgent Node OS Level:"
              + getProperty(NodeAgentInfo.OSVersion) + "\n\tAgent Node OS Architecture:"
              + getProperty(NodeAgentInfo.OSArchitecture) + "\n\tAgent Node CPU Count:"
              + getProperty(NodeAgentInfo.NodeCpus) + "\n\tAgent Node Java Vendor:"
              + getProperty(NodeAgentInfo.JavaVendor) + "\n\tAgent Node Java Version:"
              + getProperty(NodeAgentInfo.JavaVersion));
    }
  }

}
