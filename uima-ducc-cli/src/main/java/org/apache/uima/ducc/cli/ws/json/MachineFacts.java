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
package org.apache.uima.ducc.cli.ws.json;

import java.io.Serializable;
import java.util.List;

public class MachineFacts implements Serializable {

	private static final long serialVersionUID = 1L;
	
	public String status;
	public String statusReason;
	public String ip;
	public String name;
	public String memTotal;
	public String memFree;
	public String memReserve;
	public String quantum;
	public String swapInuse;
	public String swapDelta;
	public String swapFree;
	public double cpu;
	public boolean cgroups = false;
	public List<String> aliens;
	public String heartbeat;
	
	public MachineFacts(String status, 
				   String ip,
				   String name,
				   String memTotal,
				   String memFree,
				   String swapInuse,
				   String swapDelta,
				   String swapFree,
				   double cpu,
				   boolean cgroups,
				   List<String> aliens,
				   String heartbeat
				   ) 
	{
		this.status = status;
		this.statusReason = "";
		this.ip = ip;
		this.name = name;
		this.memTotal = memTotal;
		this.memFree = memFree;
		this.memReserve = "0";
		this.quantum = "";
		this.swapInuse = swapInuse;
		this.swapDelta = swapDelta;
		this.swapFree = swapFree;
		this.cpu = cpu;
		this.cgroups = cgroups;
		this.aliens = aliens;
		this.heartbeat = heartbeat;
	}
}
