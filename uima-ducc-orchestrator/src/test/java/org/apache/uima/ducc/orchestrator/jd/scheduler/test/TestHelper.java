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
package org.apache.uima.ducc.orchestrator.jd.scheduler.test;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.config.CommonConfiguration;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.orchestrator.ReservationFactory;
import org.apache.uima.ducc.orchestrator.jd.scheduler.JdHostProperties;
import org.apache.uima.ducc.transport.event.cli.ReservationRequestProperties;
import org.apache.uima.ducc.transport.event.cli.ReservationSpecificationProperties;
import org.apache.uima.ducc.transport.event.common.DuccReservation;
import org.apache.uima.ducc.transport.event.common.IDuccReservation;
import org.apache.uima.ducc.transport.event.common.IDuccReservationMap;
import org.apache.uima.ducc.transport.event.common.IDuccWorkReservation;

public class TestHelper {
	
	private static AtomicInteger jdNumber = new AtomicInteger(0);
	private static AtomicInteger jpNumber = new AtomicInteger(0);
	
	private static AtomicInteger hostNumber = new AtomicInteger(0);
	
	private static String getNodeIp(int i) {
		return "11.22.33."+i;
	}

	private static String getNodeName(int i) {
		return "node"+i;
	}
	private static NodeIdentity getNodeIdentity() {
		NodeIdentity ni = null;
		int i = hostNumber.getAndIncrement();
		try {
			ni = new NodeIdentity(getNodeIp(i), getNodeName(i));
		}
		catch(Exception e) {
		}
		return ni;
	}
	
	public static IDuccWorkReservation getDWR(JdHostProperties jdHostProperties) {
		String user = jdHostProperties.getHostUser();
		String pidAtHost = "n/a";
		String description = jdHostProperties.getHostDescription();
		String schedulingClass = jdHostProperties.getHostClass();
		String memorySize = jdHostProperties.getHostMemorySize();
		return getDWR(user,pidAtHost,description,schedulingClass,memorySize);
	}
	
	public static IDuccWorkReservation getDWR(String user, String pidAtHost, String description, String schedulingClass, String memorySize) {
		IDuccWorkReservation dwr = null;
		ReservationFactory rf = ReservationFactory.getInstance();
		CommonConfiguration cc = null;
		ReservationRequestProperties rrp = new ReservationRequestProperties();
		String key;
		String value;
		//
		key = ReservationSpecificationProperties.key_user;
		value = user;
		rrp.put(key, value);
		//
		key = ReservationSpecificationProperties.key_submitter_pid_at_host;
		value = pidAtHost;
		rrp.put(key, value);
		//
		key = ReservationSpecificationProperties.key_description;
		value = description;
		rrp.put(key, value);
		//
		key = ReservationSpecificationProperties.key_scheduling_class;
		value = schedulingClass;
		rrp.put(key, value);
		//
		key = ReservationSpecificationProperties.key_memory_size;
		value = memorySize;
		rrp.put(key, value);
		//
		dwr = rf.create(cc, rrp);
		//
		assign(dwr);
		//
		return dwr;		
	}
	
	public static DuccId getJobIdentity() {
		DuccId jdId = new DuccId(jdNumber.getAndIncrement());
		long friendly = jdId.getFriendly();
		System.out.println("friendly:"+friendly);
		return jdId;
	}
	
	public static DuccId getProcessIdentity() {
		DuccId jpId = new DuccId(jpNumber.getAndIncrement());
		long friendly = jpId.getFriendly();
		System.out.println("friendly:"+friendly);
		return jpId;
	}
	
	public static void assign(IDuccWorkReservation dwr) {
		IDuccReservationMap rm = dwr.getReservationMap();
		DuccId duccId = new DuccId(hostNumber.get());
		NodeIdentity ni = getNodeIdentity();
		IDuccReservation dr = new DuccReservation(duccId, ni, 1);
		rm.addReservation(dr);
	}
}
