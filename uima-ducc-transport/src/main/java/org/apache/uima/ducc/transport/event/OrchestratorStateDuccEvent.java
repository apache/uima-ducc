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

import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.cmdline.ICommandLine;
import org.apache.uima.ducc.transport.event.common.DuccWorkJob;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.IDuccWork;

public class OrchestratorStateDuccEvent extends AbstractDuccEvent  {

	private static final long serialVersionUID = 3637372507135841728L;

	private DuccWorkMap workMap;
	private ConcurrentHashMap<ICommandLine, ArrayList<DuccId>> serviceCmdLineMap = new ConcurrentHashMap<ICommandLine, ArrayList<DuccId>>();
	
	public OrchestratorStateDuccEvent() {
		super(EventType.ORCHESTRATOR_STATE);
	}
	
	public void setWorkMap(DuccWorkMap value) {
		this.workMap = value.deepCopy();
		compress(this.workMap);
	}
	
	public DuccWorkMap getWorkMap() {
		DuccWorkMap value = this.workMap.deepCopy();
		uncompress(value);
		return value;
	}
	
	private void compress(DuccWorkMap map) {
		for(Entry<DuccId, IDuccWork> entry : map.getMap().entrySet()) {
			IDuccWork dw = entry.getValue();
			switch(dw.getDuccType()) {
			case Service:
				DuccWorkJob dwj = (DuccWorkJob) dw;
				ICommandLine cl = dwj.getCommandLine();
				if(!serviceCmdLineMap.containsKey(cl)) {
					ArrayList<DuccId> list = new ArrayList<DuccId>();
					serviceCmdLineMap.put(cl, list);
				}
				ArrayList<DuccId> list = serviceCmdLineMap.get(cl);
				list.add(dw.getDuccId());
				dwj.setCommandLine(null);
				break;
			default:
				break;
			}
		}
	}
	
	private void uncompress(DuccWorkMap map) {
		for(Entry<ICommandLine, ArrayList<DuccId>> entry : serviceCmdLineMap.entrySet()) {
			ICommandLine cl = entry.getKey();
			ArrayList<DuccId> list = entry.getValue();
			for(DuccId duccId : list) {
				IDuccWork dw = map.findDuccWork(duccId);
				DuccWorkJob dwj = (DuccWorkJob) dw;
				dwj.setCommandLine(cl);
			}
		}
	}
}
