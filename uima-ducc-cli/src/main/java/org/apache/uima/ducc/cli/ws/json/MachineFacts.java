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
	public String ip;
	public String name;
	public String memoryEffective;
	public String memoryTotal;
	public String swapInuse;
	public String swapDelta;
	public String swapFree;
	public List<String> aliens;
	public String sharesTotal;
	public String sharesInuse;
	public String heartbeat;
	
	public MachineFacts(String status, 
				   String ip,
				   String name,
				   String memoryEffective,
				   String memoryTotal,
				   String swapInuse,
				   String swapDelta,
				   String swapFree,
				   List<String> aliens,
				   String sharesTotal,
				   String sharesInuse,
				   String heartbeat
				   ) 
	{
		this.status = status;
		this.ip = ip;
		this.name = name;
		this.memoryEffective = memoryEffective;
		this.memoryTotal = memoryTotal;
		this.swapInuse = swapInuse;
		this.swapDelta = swapDelta;
		this.swapFree = swapFree;
		this.aliens = aliens;
		this.sharesTotal = sharesTotal;
		this.sharesInuse = sharesInuse;
		this.heartbeat = heartbeat;
	}
}
