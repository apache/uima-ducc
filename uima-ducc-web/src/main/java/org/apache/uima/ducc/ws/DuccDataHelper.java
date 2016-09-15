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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
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

    public static String[] parseWorkInstances(Properties meta)
    {
        String stringArray = meta.getProperty(IServicesRegistry.work_instances);
        return parseStringArray(stringArray);
    }

    public static String[] parseImplementors(Properties meta)
    {
        String stringArray = meta.getProperty(IServicesRegistry.implementors);
        return parseStringArray(stringArray);
    }
	
    public static String[] parseStringArray(String stringArray)
    {
        
        String[] ret = new String[0];
        if(stringArray != null) {
        	stringArray = stringArray.trim();
        	if(stringArray.length() > 0) {
        		String[] tempArray = stringArray.trim().split("\\s+");
                ret = new String[tempArray.length];
                int i = 0;
                for (String s : tempArray) {
                    // Back compatibility for the shadow web servers, if no inst id then
                    // just return the 's'
                    if ( s.indexOf(".") > 0 ) {
                        String[] id_inst = s.split("\\.");
                        ret[i++] = id_inst[0].trim();
                    } else {
                        ret[i++] = s;
                    }
                }
        	}
        }
        return ret;
    }

    public static ArrayList<String> parseImplementorsAsList(Properties meta)
    {
        String[] impls = parseImplementors(meta);

        ArrayList<String> ret = new ArrayList<String>();
        for ( String s : impls ) {
            ret.add(s);
        }
        return ret;
    }

    public static ArrayList<String> parseWorkInstancesAsList(Properties meta)
    {
        String[] impls = parseWorkInstances(meta);

        ArrayList<String> ret = new ArrayList<String>();
        for ( String s : impls ) {
            ret.add(s);
        }
        return ret;
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
