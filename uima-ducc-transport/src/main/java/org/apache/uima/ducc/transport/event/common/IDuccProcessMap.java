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
package org.apache.uima.ducc.transport.event.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;

import org.apache.uima.ducc.common.utils.id.DuccId;


public interface IDuccProcessMap extends Map<DuccId,IDuccProcess>, Serializable {
	
	public void addProcess(IDuccProcess process);
	public IDuccProcess getProcess(DuccId duccId);
	public void removeProcess(DuccId duccId);
	public IDuccProcess findProcess(String nodeId, String processId);
	
	public int getReadyProcessCount();
	//public int getFailedProcessCount();
	public int getFailedUnexpectedProcessCount();
	public int getAliveProcessCount();
	public int getUsableProcessCount();
	
	public ArrayList<DuccId> getFailedInitialization();
	public ArrayList<DuccId> getFailedNotInitialization();
	
	public int getFailedInitializationCount();
	public int getFailedNotInitializationCount();
	
	public Map<DuccId,IDuccProcess> getMap();
	public IDuccProcessMap deepCopy();
	
	public long getPgInCount();
	public double getSwapUsageGb();
	public double getSwapUsageGbMax();
}
