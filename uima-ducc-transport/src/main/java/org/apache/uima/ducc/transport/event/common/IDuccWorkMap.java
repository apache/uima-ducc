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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccTypes.DuccType;

@SuppressWarnings("rawtypes")
public interface IDuccWorkMap extends Serializable, Map {
	
	public IDuccWorkMap deepCopy();
	
	public Set<DuccId> getJobKeySet();
	public Set<DuccId> getServiceKeySet();
	public Set<DuccId> getReservationKeySet();
	public Set<DuccId> getManagedReservationKeySet();
	
	public int getJobCount();
	public int getServiceCount();
	public int getReservationCount();
	
	public IDuccWork findDuccWork(DuccId duccId);
	public IDuccWork findDuccWork(String duccId);
	public IDuccWork findDuccWork(DuccType duccType, String id);
	
	public List<DuccWorkJob> getServicesList(List<String> implementors);
	public Map<Long,DuccWorkJob> getServicesMap(List<String> implementors);
	
	public boolean isJobDriverMinimalAllocateRequirementMet();
	
	public Map<DuccId,IDuccWork> getMap();
	
	public void addDuccWork(IDuccWork duccWork);
	public void removeDuccWork(DuccId duccId);
	
	public long getMemoryInuseJobs();
	public long getMemoryInuseServices();
	public long getMemoryInuseReservations();
	public long getMemoryInuse();
}
