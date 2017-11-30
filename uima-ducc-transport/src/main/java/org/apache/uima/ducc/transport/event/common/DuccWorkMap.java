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
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.SerializationUtils;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccTypes.DuccType;
import org.apache.uima.ducc.transport.event.common.IDuccWorkService.ServiceDeploymentType;

@SuppressWarnings("rawtypes")
public class DuccWorkMap implements IDuccWorkMap {
	
	/**
	 * please increment this sUID when removing or modifying a field 
	 */
	private static final long serialVersionUID = 2L;
	
	private Map<DuccId,IDuccWork> concurrentWorkMap = new ConcurrentHashMap<DuccId,IDuccWork>();
	
	private AtomicBoolean atomicJobDriverMinimalAllocateRequirementMet = new AtomicBoolean(false);
	
	private AtomicInteger atomicJobCount = new AtomicInteger(0);
	private AtomicInteger atomicServiceCount = new AtomicInteger(0);
	private AtomicInteger atomicReservationCount = new AtomicInteger(0);
	
	private Map<String,DuccId> concurrentJobMap = null;
	private Map<String,DuccId> concurrentServiceMap = null;
	private Map<String,DuccId> concurrentReservationMap = null;
	
	public DuccWorkMap() {
		concurrentJobMap = new ConcurrentHashMap<String,DuccId>();
		concurrentServiceMap = new ConcurrentHashMap<String,DuccId>();
		concurrentReservationMap = new ConcurrentHashMap<String,DuccId>();
	}

	private void init() {
		if(atomicJobDriverMinimalAllocateRequirementMet == null) {
			atomicJobDriverMinimalAllocateRequirementMet = new AtomicBoolean(false);
		}
	}
	
	public boolean isJobDriverMinimalAllocateRequirementMet() {
		init();
		return atomicJobDriverMinimalAllocateRequirementMet.get();
	}
	
	public void setJobDriverMinimalAllocateRequirementMet() {
		init();
		atomicJobDriverMinimalAllocateRequirementMet.set(true);
	}
	
	public void resetJobDriverMinimalAllocateRequirementMet() {
		init();
		atomicJobDriverMinimalAllocateRequirementMet.set(false);
	}
	
	public int getJobCount() {
		return atomicJobCount.get();
	}
	
	public Set<DuccId> getJobKeySet() {
		Set<DuccId> retVal = new HashSet<DuccId>();
		Iterator<DuccId> iterator = keySet().iterator();
		while(iterator.hasNext()) {
			DuccId duccId = iterator.next();
			switch(concurrentWorkMap.get(duccId).getDuccType()) {
			case Job:
				retVal.add(duccId);
				break;
			default:
				break;
			}
		}
		return retVal;
	}
	
	public int getServiceCount() {
		return atomicServiceCount.get();
	}
	
	public Set<DuccId> getServiceKeySet() {
		Set<DuccId> retVal = new HashSet<DuccId>();
		Iterator<DuccId> iterator = keySet().iterator();
		while(iterator.hasNext()) {
			DuccId duccId = iterator.next();
			switch(concurrentWorkMap.get(duccId).getDuccType()) {
			case Service:
				retVal.add(duccId);
				break;
			default:
				break;
			}
		}
		return retVal;
	}
	
	public List<DuccWorkJob> getServicesList(List<String> implementors) {
		ArrayList<DuccWorkJob> servicesList = new ArrayList<DuccWorkJob>();
		if(implementors != null) {
			if(getServiceKeySet().size()> 0) {
				Iterator<DuccId> iterator = null;
				iterator = getServiceKeySet().iterator();
				while(iterator.hasNext()) {
					DuccId serviceId = iterator.next();
					String fid = ""+serviceId.getFriendly();
					if(implementors.contains(fid)) {
						DuccWorkJob service = (DuccWorkJob) findDuccWork(serviceId);
						servicesList.add(service);
					}
				}
			}
		}
		return servicesList;
	}
	
	
	public Map<Long,DuccWorkJob> getServicesMap(List<String> implementors) {
		TreeMap<Long,DuccWorkJob> servicesMap = new TreeMap<Long,DuccWorkJob>();
		if(implementors != null) {
			if(getServiceKeySet().size()> 0) {
				Iterator<DuccId> iterator = null;
				iterator = getServiceKeySet().iterator();
				while(iterator.hasNext()) {
					DuccId serviceId = iterator.next();
					Long lid = serviceId.getFriendly();
					String fid = ""+lid;
					if(implementors.contains(fid)) {
						DuccWorkJob service = (DuccWorkJob) findDuccWork(serviceId);
						servicesMap.put(lid,service);
					}
				}
			}
		}
		return servicesMap;
	}
	
	public Set<DuccId> getManagedReservationKeySet() {
		Set<DuccId> retVal = new HashSet<DuccId>();
		Iterator<DuccId> iterator = keySet().iterator();
		while(iterator.hasNext()) {
			DuccId duccId = iterator.next();
			IDuccWork duccWork = concurrentWorkMap.get(duccId);
			if(duccWork != null) {
				switch(duccWork.getDuccType()) {
				case Service:
					DuccWorkJob service = (DuccWorkJob)duccWork;
					ServiceDeploymentType sdt = service.getServiceDeploymentType();
					if(sdt != null) {
						switch(sdt) {
						case other:
							retVal.add(duccId);
							break;
						default:
							break;
						}
					}
					break;
				default:
					break;
				}
			}
		}
		return retVal;
	}
	
	public int getReservationCount() {
		return atomicReservationCount.get();
	}
	
	public Set<DuccId> getReservationKeySet() {
		Set<DuccId> retVal = new HashSet<DuccId>();
		Iterator<DuccId> iterator = keySet().iterator();
		while(iterator.hasNext()) {
			DuccId duccId = iterator.next();
			switch(concurrentWorkMap.get(duccId).getDuccType()) {
			case Reservation:
				retVal.add(duccId);
				break;
			default:
				break;
			}
		}
		return retVal;
	}
	
	//
	
	public long getMemoryInuseJobs() {
		long size = 0;
		Set<DuccId> keys = getJobKeySet();
		for(DuccId key : keys) {
			IDuccWorkJob job = (IDuccWorkJob)concurrentWorkMap.get(key);
			long bytesPerProcess = job.getSchedulingInfo().getMemorySizeAllocatedInBytes();
			long numberOfProcesses = job.getAliveProcessCount();
			size += bytesPerProcess * numberOfProcesses;
		}
		return size;
	}
	
	public long getMemoryInuseServices() {
		long size = 0;
		Set<DuccId> keys = getServiceKeySet();
		for(DuccId key : keys) {
			IDuccWorkService service = (IDuccWorkService)concurrentWorkMap.get(key);
			long bytesPerProcess = service.getSchedulingInfo().getMemorySizeAllocatedInBytes();
			long numberOfProcesses = 1;
			size += bytesPerProcess * numberOfProcesses;
		}
		return size;
	}
	
	public long getMemoryInuseReservations() {
		long size = 0;
		Set<DuccId> keys = getReservationKeySet();
		for(DuccId key : keys) {
			IDuccWorkReservation reservation = (IDuccWorkReservation)concurrentWorkMap.get(key);
			long bytesPerReservation = reservation.getSchedulingInfo().getMemorySizeAllocatedInBytes();
			long numberOfReservations = 1;
			size += bytesPerReservation * numberOfReservations;
		}
		return size;
	}
	
	public long getMemoryInuse() {
		long retVal = getMemoryInuseJobs()+getMemoryInuseServices()+getMemoryInuseReservations();
		return retVal;
	}
	
	//
	
	public Map<DuccId,IDuccWork> getMap() {
		return concurrentWorkMap;
	}
	
	public static String normalize(String id) {
		String normalizedId = String.valueOf(Integer.parseInt(id));
		return normalizedId;
	}
	
	public void addDuccWork(IDuccWork duccWork) {
		synchronized(this) {
			duccWork.getDuccId();
			DuccId key = duccWork.getDuccId();
			concurrentWorkMap.put(key,duccWork);
			switch(duccWork.getDuccType()) {
			case Job:
				jobAddKey(duccWork.getId(),key);
				break;
			case Service:
				serviceAddKey(duccWork.getId(),key);
				break;
			case Reservation:
				reservationAddKey(duccWork.getId(),key);
				break;
			default:
				break;
			}
			switch(duccWork.getDuccType()) {
			case Job:
				atomicJobCount.incrementAndGet();
				break;
			case Service:
				atomicServiceCount.incrementAndGet();
				break;
			case Reservation:
				atomicReservationCount.incrementAndGet();
				break;
			default: 
				break;
			}
		}
	}
	
	public void removeDuccWork(DuccId duccId) {
		synchronized(this) {
			IDuccWork duccWork = concurrentWorkMap.remove(duccId);
			if(duccWork != null) {
				switch(duccWork.getDuccType()) {
				case Job:
					jobRemoveKey(duccWork.getId());
					break;
				case Service:
					serviceRemoveKey(duccWork.getId());
					break;
				case Reservation:
					reservationRemoveKey(duccWork.getId());
					break;
				default:
					break;
				}
			
                switch(duccWork.getDuccType()) {
                case Job:
                    atomicJobCount.decrementAndGet();
                    break;
                case Service:
                    atomicServiceCount.decrementAndGet();
                    break;
                case Reservation:
                    atomicReservationCount.decrementAndGet();
                    break;
                default: 
                    break;
                }
            }
		}
	}
	
	public IDuccWork findDuccWork(DuccId duccId) {
		synchronized(this) {
			return concurrentWorkMap.get(duccId);
		}
	}

	@Override
	public IDuccWork findDuccWork(String duccId) {
		IDuccWork retVal = null;
		for(Entry<DuccId, IDuccWork> entry : concurrentWorkMap.entrySet()) {
			DuccId id = entry.getKey();
			String sid = ""+id.getFriendly();
			if(sid.equals(duccId)) {
				retVal = entry.getValue();
				break;
			}
		};
		return retVal;
	}
	
	public IDuccWork findDuccWork(DuccType duccType, String id) {
		IDuccWork duccWork = null;
		String key = id;
		DuccId duccId;
		synchronized(this) {
			switch(duccType) {
			case Job:
				duccId = concurrentJobMap.get(normalize(key));
				if(duccId != null) {
					duccWork = concurrentWorkMap.get(duccId);
				}
				break;
			case Service:
				duccId = concurrentServiceMap.get(normalize(key));
				if(duccId != null) {
					duccWork = concurrentWorkMap.get(duccId);
				}
				break;
			case Reservation:
				duccId = concurrentReservationMap.get(normalize(key));
				if(duccId != null) {
					duccWork = concurrentWorkMap.get(duccId);
				}
				break;
			default:
				break;
			}
			return duccWork;
		}
	}
	
	public IDuccWork findDuccWork(DuccType duccType, Long id) {
		IDuccWork duccWork = null;
		String key = normalize(String.valueOf(id));
		DuccId duccId;
		synchronized(this) {
			switch(duccType) {
			case Job:
				duccId = concurrentJobMap.get(normalize(key));
				if(duccId != null) {
					duccWork = concurrentWorkMap.get(duccId);
				}
				break;
			case Service:
				duccId = concurrentServiceMap.get(normalize(key));
				if(duccId != null) {
					duccWork = concurrentWorkMap.get(duccId);
				}
				break;
			case Reservation:
				duccId = concurrentReservationMap.get(normalize(key));
				if(duccId != null) {
					duccWork = concurrentWorkMap.get(duccId);
				}
				break;
			default:
				break;
			}
			return duccWork;
		}
	}
	
	public DuccWorkMap deepCopy() {
		synchronized (this) {
			return (DuccWorkMap)SerializationUtils.clone(this);
		}
	}
	
	/*
	 * *****
	 */

	private void jobAddKey(String id, DuccId duccId) {
		concurrentJobMap.put(normalize(id),duccId);
	}
	
	private void jobRemoveKey(String id) {
		concurrentJobMap.remove(normalize(id));
	}
	
	/*
	 * *****
	 */

	private void serviceAddKey(String id, DuccId duccId) {
		concurrentServiceMap.put(normalize(id),duccId);
	}
	
	private void serviceRemoveKey(String id) {
		concurrentServiceMap.remove(normalize(id));
	}
	
	/*
	 * *****
	 */

	private void reservationAddKey(String id, DuccId duccId) {
		concurrentReservationMap.put(normalize(id),duccId);
	}
	
	private void reservationRemoveKey(String id) {
		concurrentReservationMap.remove(normalize(id));
	}
	
	/*
	 * *****
	 */	
	
	
	public int size() {
		return concurrentWorkMap.size();
	}

	
	public boolean isEmpty() {
		return concurrentWorkMap.isEmpty();
	}

	
	public boolean containsKey(Object key) {
		return concurrentWorkMap.containsKey(key);
	}

	
	public boolean containsValue(Object value) {
		return concurrentWorkMap.containsValue((IDuccWork)value);
	}

	
	public Object get(Object key) {
		return concurrentWorkMap.get((DuccId)key);
	}

	
	public Object put(Object key, Object value) {
		return concurrentWorkMap.put((DuccId)key, (IDuccWork)value);
	}

	
	public Object remove(Object key) {
		return concurrentWorkMap.remove(key);
	}

	@SuppressWarnings("unchecked")
	
	public void putAll(Map m) {
		concurrentWorkMap.putAll(m);
	}

	
	public void clear() {
		 concurrentWorkMap.clear();
	}

	
	public Set<DuccId> keySet() {
		return concurrentWorkMap.keySet();
	}

	
	public Collection<IDuccWork> values() {
		return concurrentWorkMap.values();
	}

	
	public Set entrySet() {
		return concurrentWorkMap.entrySet();
	}

	
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + atomicJobCount.get();
		result = prime * result + ((concurrentJobMap == null) ? 0 : concurrentJobMap.hashCode());
		result = prime * result + atomicServiceCount.get();
		result = prime * result + ((concurrentServiceMap == null) ? 0 : concurrentServiceMap.hashCode());
		result = prime * result + atomicReservationCount.get();
		result = prime * result	+ ((concurrentReservationMap == null) ? 0 : concurrentReservationMap.hashCode());
		result = prime * result + ((concurrentWorkMap == null) ? 0 : concurrentWorkMap.hashCode());
		return result;
	}

	
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DuccWorkMap other = (DuccWorkMap) obj;
		if (atomicJobCount.get() != other.atomicJobCount.get())
			return false;
		if (concurrentJobMap == null) {
			if (other.concurrentJobMap != null)
				return false;
		} else if (!concurrentJobMap.equals(other.concurrentJobMap))
			return false;
		if (atomicServiceCount.get() != other.atomicServiceCount.get())
			return false;
		if (concurrentServiceMap == null) {
			if (other.concurrentServiceMap != null)
				return false;
		} else if (!concurrentServiceMap.equals(other.concurrentServiceMap))
			return false;
		if (atomicReservationCount != other.atomicReservationCount)
			return false;
		if (concurrentReservationMap == null) {
			if (other.concurrentReservationMap != null)
				return false;
		} else if (!concurrentReservationMap.equals(other.concurrentReservationMap))
			return false;
		if (concurrentWorkMap == null) {
			if (other.concurrentWorkMap != null)
				return false;
		} else if (!concurrentWorkMap.equals(other.concurrentWorkMap)) {
			return false;
		}
		return true;
	}
	
}
