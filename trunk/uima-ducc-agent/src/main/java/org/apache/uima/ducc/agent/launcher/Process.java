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

import java.io.OutputStream;
import java.io.Serializable;
import java.util.List;

public interface Process extends Serializable {
	public String getLogPath();
	public void setLogPath(String logPath);
	public String getPid();
	public void setPid(String pid);
	public List<String> getCommand();
	public String getProcessId();
	public void setProcessId(String processId);
	public String getPort();
	public void setPort(String port);
	public String getNodeIp();
	public void setNodeIp( String nodeIp);
	public boolean isAgentProcess();
//	public void setOSProcess(java.lang.Process target );
	public void setLogStream(OutputStream ps);
	public String getNodeName();
	public void setNodeName(String nodeName);
	public void setAgentLogPath(String logPath);
	public String getAgentLogPath();
	public boolean isAttached();
	public void setAttached();
    public void setClientId(String clientId);
    public String getClientId();
  public String getAbsoluteLogPath();
  public void setAbsoluteLogPath(String absoluteLogPath);
	public String getDescription();
	public void setDescription(String description);
	
	public void setParent( String parent);
	public String getParent();
  public Throwable getExceptionStackTrace();
  public void setExceptionStackTrace(Throwable exceptionStackTrace);
  

}
