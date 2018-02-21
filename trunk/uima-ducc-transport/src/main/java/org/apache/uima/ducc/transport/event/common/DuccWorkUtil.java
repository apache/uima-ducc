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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.utils.id.DuccId;


public class DuccWorkUtil {

	private static void put(List<String> list, NodeIdentity nodeIdentity) {
		if(list != null) {
			if(nodeIdentity != null) {
				String name = nodeIdentity.getName();
				if(name != null) {
					if(!list.contains(name)) {
						list.add(name);
					}
				}
			}
		}
	}
	
	private static void putProcessMapNodes(List<String> list, IDuccProcessMap processMap) {
		if(list != null) {
			if(processMap != null) {
				Map<DuccId, IDuccProcess> map = processMap.getMap();
				if(map != null) {
					Set<Entry<DuccId, IDuccProcess>> entrySet = map.entrySet();
					if(entrySet != null) {
						Iterator<Entry<DuccId, IDuccProcess>> iterator = entrySet.iterator();
						while(iterator.hasNext()) {
							Entry<DuccId, IDuccProcess> entry = iterator.next();
							if(entry != null) {
								IDuccProcess process = entry.getValue();
								if(process != null) {
									NodeIdentity nodeIdentity = process.getNodeIdentity();
									put(list, nodeIdentity);
								}
							}
						}
					}
				}
			}
		}
	}
	
	private static void putJobDriverNode(List<String> list, IDuccWorkJob dwJob) {
		if(dwJob != null) {
			DuccWorkPopDriver driver = dwJob.getDriver();
			if(driver != null) {
				IDuccProcessMap processMap = driver.getProcessMap();
				putProcessMapNodes(list, processMap);
			}
		}
	}
	
	private static void putJobProcessNodes(List<String> list, IDuccWorkJob dwJob) {
		if(dwJob != null) {
			IDuccProcessMap processMap = dwJob.getProcessMap();
			putProcessMapNodes(list, processMap);
		}
	}
	
	private static void putReservationNodes(List<String> list, IDuccWorkReservation dwReservation) {
		if(dwReservation != null) {
			if(list != null) {
				IDuccReservationMap reservationMap = dwReservation.getReservationMap();
				if(reservationMap != null) {
					Set<Entry<DuccId, IDuccReservation>> entrySet = reservationMap.entrySet();
					if(entrySet != null) {
						Iterator<Entry<DuccId, IDuccReservation>> iterator = entrySet.iterator();
						while(iterator.hasNext()) {
							Entry<DuccId, IDuccReservation> entry = iterator.next();
							if(entry != null) {
								IDuccReservation reservation = entry.getValue();
								if(reservation != null) {
									NodeIdentity nodeIdentity = reservation.getNodeIdentity();
									put(list, nodeIdentity);
								}
							}
						} 
					}
				}
			}
		}
	}
	
	public static void getJobNodes(List<String> list, DuccWorkMap duccWorkMap) {
		if(duccWorkMap != null) {
			Set<DuccId> jobKeySet = duccWorkMap.getJobKeySet();
			if(jobKeySet != null) {
				Iterator<DuccId> iterator = jobKeySet.iterator();
				while(iterator.hasNext()) {
					DuccId duccId = iterator.next();
					IDuccWork duccWork = duccWorkMap.findDuccWork(duccId);
					IDuccWorkJob dwJob = (IDuccWorkJob) duccWork;
					putJobDriverNode(list, dwJob);
					putJobProcessNodes(list, dwJob);
				}
			}
		}
	}
	
	public static void getReservationNodes(List<String> list, DuccWorkMap duccWorkMap) {
		if(duccWorkMap != null) {
			Set<DuccId> reservationKeySet = duccWorkMap.getReservationKeySet();
			if(reservationKeySet != null) {
				Iterator<DuccId> iterator = reservationKeySet.iterator();
				while(iterator.hasNext()) {
					DuccId duccId = iterator.next();
					IDuccWork duccWork = duccWorkMap.findDuccWork(duccId);
					IDuccWorkReservation dwReservation = (IDuccWorkReservation) duccWork;
					putReservationNodes(list, dwReservation);
				}
			}
		}
	}
		
	public static List<String> getNodes(DuccWorkMap duccWorkMap) {
		ArrayList<String> list = new ArrayList<String>();
		getJobNodes(list,duccWorkMap);
		getReservationNodes(list,duccWorkMap);
		return list;
	}
}
