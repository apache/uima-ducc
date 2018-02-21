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
package org.apache.uima.ducc.transport.event;

import java.util.Map;

import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.cmdline.ICommandLine;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccStandardInfo;


public class ProcessStartDuccEvent extends AbstractDuccEvent {
	private static final long serialVersionUID = 4366171169403416863L;
	private Map<DuccId,IDuccProcess> processMap;
	private ICommandLine commandLine;
//	private String user;
//	private String workingDirectory;
	private DuccId duccId;
	private IDuccStandardInfo info;
	
	public ProcessStartDuccEvent(Map<DuccId,IDuccProcess> processMap, ICommandLine commandLine,DuccId duccId, IDuccStandardInfo info) {
		super(EventType.START_PROCESS);
		this.processMap = processMap;
		this.commandLine = commandLine;
//		this.user = user;
		this.duccId = duccId;
		this.info = info;
	}
	public Map<DuccId,IDuccProcess> getProcessMap() {
		return processMap;
	}
	public ICommandLine getCommandLine() {
		return commandLine;
	}
	public IDuccStandardInfo getStandardInfo() {
		return info;
	}
	public DuccId getDuccWorkId() {
		return duccId;
	}
}
