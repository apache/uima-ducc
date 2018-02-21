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
package org.apache.uima.ducc.ws.server;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.common.json.MonitorInfo;
import org.apache.uima.ducc.common.utils.id.DuccId;

public interface IWebMonitor {
	
	public static String job = "job";
	public static String reservation = "reservation";
	public enum MonitorType { 
		Job(job), 
		ManagedReservation(reservation), 
		UnmanagedReservation (reservation);
		private String text = null;
		MonitorType(String text) {
			this.text = text;
		}
		public String getText() {
			return text;
		}
	};
	
	public void register(String host, String port);
	public boolean isAutoCancelEnabled();
	public MonitorInfo renew(MonitorType monitorType, String id);
	public Long getExpiry(MonitorType monitorType, DuccId duccId);
	public ConcurrentHashMap<DuccId,Long> getExpiryMap(MonitorType monitorType);
	public boolean isCanceled(MonitorType monitorType, DuccId duccId);
}
