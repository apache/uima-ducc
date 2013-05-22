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
package org.apache.uima.ducc.ws;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.DuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcessMap;
import org.apache.uima.ducc.transport.event.common.IDuccWorkService.ServiceDeploymentType;
import org.apache.uima.ducc.ws.registry.IServicesRegistry;
import org.apache.uima.ducc.ws.registry.ServicesRegistry;
import org.apache.uima.ducc.ws.registry.ServicesRegistryMap;
import org.apache.uima.ducc.ws.registry.ServicesRegistryMapPayload;

public class DuccDataHelper {

	private static DuccDataHelper duccDataHelper = new DuccDataHelper();
	
	public static DuccDataHelper getInstance() {
		return duccDataHelper;
	}
	
	public TreeMap<String,ArrayList<DuccId>> getServiceToJobsUsageMap() {
		TreeMap<String,ArrayList<DuccId>> map = new TreeMap<String,ArrayList<DuccId>>();
		DuccData duccData = DuccData.getInstance();
		ConcurrentSkipListMap<JobInfo, JobInfo> jobs = duccData.getSortedJobs();
		for(JobInfo jobInfo : jobs.descendingKeySet()) {
			DuccWorkJob job = jobInfo.getJob();
			if(job.isOperational()) {
				DuccId duccId = job.getDuccId();
				String[] dependencies = job.getServiceDependencies();
				if(dependencies != null) {
					for(String dependency : dependencies) {
						if(!map.containsKey(dependency)) {
							map.put(dependency, new ArrayList<DuccId>());
						}
						ArrayList<DuccId> duccIds = map.get(dependency);
						if(!duccIds.contains(duccId)) {
							duccIds.add(duccId);
						}
					}
				}
			}
		}
		return map;
	}
	
	private String getServiceId(DuccId serviceId) {
		String retVal = null;
		ServicesRegistry servicesRegistry = new ServicesRegistry();
		ServicesRegistryMap map = servicesRegistry.getMap();
		for(Integer key : map.getDescendingKeySet()) {
			ServicesRegistryMapPayload payload = map.get(key);
			Properties meta = payload.meta;
			String implementors = meta.getProperty(IServicesRegistry.implementors);
			if(implementors != null) {
				String[] implementorsArray = implementors.trim().split(" ");
				for(String implementor : implementorsArray) {
					if(implementor.trim().equals(""+serviceId.getFriendly()));
					retVal = meta.getProperty(IServicesRegistry.numeric_id);
					break;
				}
			}
		}
		return retVal;
	}
	
	public TreeMap<String,ArrayList<String>> getServiceToServicesUsageMap() {
		TreeMap<String,ArrayList<String>> map = new TreeMap<String,ArrayList<String>>();
		DuccData duccData = DuccData.getInstance();
		ConcurrentSkipListMap<JobInfo, JobInfo> jobs = duccData.getSortedServices();
		for(JobInfo jobInfo : jobs.descendingKeySet()) {
			DuccWorkJob service = jobInfo.getJob();
			if(service.isOperational()) {
				ServiceDeploymentType type = service.getServiceDeploymentType();
				if(type != null) {
					switch(type) {
					case uima:
					case custom:
						DuccId duccId = service.getDuccId();
						String serviceId = getServiceId(duccId);
						if(serviceId != null) {
							String[] dependencies = service.getServiceDependencies();
							if(dependencies != null) {
								for(String dependency : dependencies) {
									if(!map.containsKey(dependency)) {
										map.put(dependency, new ArrayList<String>());
									}
									ArrayList<String> serviceIds = map.get(dependency);
									if(!serviceIds.contains(serviceId)) {
										serviceIds.add(serviceId);
									}
								}
							}
						}
						break;
					default:
						break;
					}
				}
			}
		}
		return map;
	}
	
	public TreeMap<String,ArrayList<DuccId>> getServiceToReservationsUsageMap() {
		TreeMap<String,ArrayList<DuccId>> map = new TreeMap<String,ArrayList<DuccId>>();
		DuccData duccData = DuccData.getInstance();
		ConcurrentSkipListMap<JobInfo, JobInfo> services = duccData.getSortedServices();
		for(JobInfo jobInfo : services.descendingKeySet()) {
			DuccWorkJob service = jobInfo.getJob();
			if(service.isOperational()) {
				ServiceDeploymentType type = service.getServiceDeploymentType();
				if(type != null) {
					switch(type) {
					case other:
						DuccId duccId = service.getDuccId();
						String[] dependencies = service.getServiceDependencies();
						if(dependencies != null) {
							for(String dependency : dependencies) {
								if(!map.containsKey(dependency)) {
									map.put(dependency, new ArrayList<DuccId>());
								}
								ArrayList<DuccId> duccIds = map.get(dependency);
								if(!duccIds.contains(duccId)) {
									duccIds.add(duccId);
								}
							}
						}
						break;
					default:
						break;
					}
				}
			}
		}
		return map;
	}

	public ArrayList<JobProcessInfo> getJobProcessInfoList(String nodeName) {
		ArrayList<JobProcessInfo> list = new ArrayList<JobProcessInfo>();
		if(nodeName != null) {
			DuccData duccData = DuccData.getInstance();
			ConcurrentSkipListMap<JobInfo, JobInfo> jobs = duccData.getSortedJobs();
			for(JobInfo jobInfo : jobs.descendingKeySet()) {
				DuccWorkJob job = jobInfo.getJob();
				if(job.isOperational()) {
					DuccId jobid = job.getDuccId();
					IDuccProcessMap map = job.getProcessMap();
					Iterator<DuccId> procids = map.keySet().iterator();
					while(procids.hasNext()) {
						DuccId procid = procids.next();
						IDuccProcess proc = map.get(procid);
						if(!proc.isComplete()) {
							NodeIdentity nodeIdentity = proc.getNodeIdentity();
							String procNodeName = nodeIdentity.getName();
							if(procNodeName != null) {
								if(nodeName.equals(procNodeName)) {
									JobProcessInfo jpi = new JobProcessInfo();
									jpi.jobId = jobid;
									jpi.procid = procid;
									list.add(jpi);
								}
							}
						}
					}
				}
			}
		}
		return list;
	}

	public ArrayList<JobProcessInfo> getJobProcessIds(ArrayList<String> nodes) {
		ArrayList<JobProcessInfo> list = new ArrayList<JobProcessInfo>();
		if(nodes != null) {
			Iterator<String> iterator = nodes.iterator();
			while(iterator.hasNext()) {
				String node = iterator.next();
				ArrayList<JobProcessInfo> listForNode = getJobProcessInfoList(node);
				for(JobProcessInfo jpi : listForNode) {
					list.add(jpi);
				}
			}
		}
		return list;
	}
}
