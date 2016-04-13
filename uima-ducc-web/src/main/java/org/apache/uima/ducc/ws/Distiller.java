package org.apache.uima.ducc.ws;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.SizeBytes;
import org.apache.uima.ducc.common.SizeBytes.Type;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.OrchestratorStateDuccEvent;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcessMap;
import org.apache.uima.ducc.transport.event.common.IDuccReservation;
import org.apache.uima.ducc.transport.event.common.IDuccReservationMap;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccWorkMap;
import org.apache.uima.ducc.transport.event.common.IDuccWorkReservation;

public class Distiller {

	private static DuccLogger logger = DuccLoggerComponents.getWsLogger(Distiller.class.getName());
	private static DuccId jobid = null;
	
	// map of key = machine name, value = bytes allocated
	private static volatile Map<String,Long> map = new HashMap<String,Long>();
	
	/**
	 * get map of key = machine name, value = bytes allocated
	 */
	public static Map<String,Long> getMap() {
		HashMap<String,Long> retVal = new HashMap<String,Long>();
		retVal.putAll(map);
		return retVal;
	}
	
	/**
	 * for each OR publication that arrives, calculate a new map of <machine name, bytes allocated>
	 */
	public static Map<String,Long> deriveMachineMemoryInUse(OrchestratorStateDuccEvent duccEvent) {
		String location = "getMachineMemoryInUse";
		Map<String,Long> revisedMap = new HashMap<String,Long>();
		try {
			if(duccEvent != null) {
				IDuccWorkMap dwm = duccEvent.getWorkMap();
				if(dwm != null) {
					jobs(revisedMap, dwm);
					reservations(revisedMap, dwm);
					managedReservations(revisedMap, dwm);
					services(revisedMap, dwm);
				}
			}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
		map = revisedMap;
		return map;
	}
	
	// accumulate bytes allocated on each machine for each active job
	private static void jobs(Map<String,Long> map, IDuccWorkMap dwm) {
		String location = "jobs";
		try {
			if(map != null) {
				if(dwm != null) {
					Set<DuccId> keys = dwm.getJobKeySet();
					for(DuccId key : keys) {
						IDuccWork dw = dwm.findDuccWork(key);
						IDuccWorkJob dwj = (IDuccWorkJob) dw;
						if(dwj != null) {
							if(dwj.isOperational()) {
								long bytes = dwj.getSchedulingInfo().getMemorySizeAllocatedInBytes();
								IDuccProcessMap processMap = dwj.getProcessMap();
								if(processMap != null) {
									for(IDuccProcess process : processMap.values()) {
										if(!process.isDeallocated()) {
											NodeIdentity ni = process.getNodeIdentity();
											if(ni != null) {
												String name = ni.getName();
												if(name != null) {
													add(map, name, bytes);
													SizeBytes sb = new SizeBytes(Type.Bytes,bytes);
													String text = location+": "+name+"="+sb.getGBytes();
													logger.trace(location, dw.getDuccId(), text);
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
	}

	// accumulate bytes allocated on each machine for each active reservation
	private static void reservations(Map<String,Long> map, IDuccWorkMap dwm) {
		String location = "reservations";
		try {
			if(map != null) {
				if(dwm != null) {
					Set<DuccId> keys = dwm.getReservationKeySet();
					for(DuccId key : keys) {
						IDuccWork dw = dwm.findDuccWork(key);
						IDuccWorkReservation dwr = (IDuccWorkReservation) dw;
						if(dwr != null) {
							if(dwr.isOperational()) {
								IDuccReservationMap reservationMap = dwr.getReservationMap();
								for(IDuccReservation reservation : reservationMap.values()) {
									NodeIdentity ni = reservation.getNodeIdentity();
									if(ni != null) {
										String name = ni.getName();
										if(name != null) {
											SizeBytes sb = new SizeBytes(SizeBytes.Type.Bytes, dw.getSchedulingInfo().getMemorySizeAllocatedInBytes());
											long bytes = sb.getBytes();
											add(map, name, bytes);
											String text = location+": "+name+"="+sb.getGBytes();
											logger.trace(location, dw.getDuccId(), text);
										}
									}
								}
							}
						}
					}
				}
			}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
	}
	
	// accumulate bytes allocated on each machine for each active managed reservation
	private static void managedReservations(Map<String,Long> map, IDuccWorkMap dwm) {
		// handled by services() below
	}
	
	// accumulate bytes allocated on each machine for each active service instance
	private static void services(Map<String,Long> map, IDuccWorkMap dwm) {
		String location = "services";
		try {
			if(map != null) {
				if(dwm != null) {
					Set<DuccId> keys = dwm.getServiceKeySet();
					for(DuccId key : keys) {
						IDuccWork dw = dwm.findDuccWork(key);
						IDuccWorkJob dws = (IDuccWorkJob) dw;
						if(dws != null) {
							if(dws.isOperational()) {
								long bytes = dws.getSchedulingInfo().getMemorySizeAllocatedInBytes();
								IDuccProcessMap processMap = dws.getProcessMap();
								if(processMap != null) {
									for(IDuccProcess process : processMap.values()) {
										if(!process.isDeallocated()) {
											NodeIdentity ni = process.getNodeIdentity();
											if(ni != null) {
												String name = ni.getName();
												if(name != null) {
													add(map, name, bytes);
													SizeBytes sb = new SizeBytes(Type.Bytes,bytes);
													String text = location+": "+name+"="+sb.getGBytes();
													logger.trace(location, dw.getDuccId(), text);
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
	}
	
	// accumulate bytes allocated on specified machine
	private static void add(Map<String,Long> map, String name, long bytes) {
		Long value = new Long(0);
		if(!map.containsKey(name)) {
			map.put(name, value);
		}
		value = map.get(name)+bytes;
		map.put(name,value);
	}
}
