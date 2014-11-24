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
package org.apache.uima.ducc.orchestrator;

import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.orchestrator.utilities.TrackSync;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.IDuccTypes.DuccType;

/**
 * Wrapper calls to DuccWorkMap to perform synchronization accounting and logging into or.log.
 * Introduced by Jira UIMA-3657.
 */
public class WorkMapHelper {

	public static void addDuccWork(DuccWorkMap workMap, IDuccWork dw, Object object, String methodName) {
		TrackSync ts = TrackSync.await(workMap, object.getClass(), methodName);
		synchronized(workMap) {
			ts.using();
			workMap.addDuccWork(dw);
		}
		ts.ended();
	}
	
	public static void removeDuccWork(DuccWorkMap workMap, IDuccWork dw, Object object, String methodName) {
		TrackSync ts = TrackSync.await(workMap, object.getClass(), methodName);
		DuccId duccId = dw.getDuccId();
		synchronized(workMap) {
			ts.using();
			workMap.removeDuccWork(duccId);
		}
		ts.ended();
	}
	
	public static IDuccWork findDuccWork(DuccWorkMap workMap, String duccId, Object object, String methodName) {
		IDuccWork dw = null;
		TrackSync ts = TrackSync.await(workMap, object.getClass(), methodName);
		synchronized(workMap) {
			ts.using();
			dw = workMap.findDuccWork(duccId);
		}
		ts.ended();
		return dw;
	}
	
	public static IDuccWork findDuccWork(DuccWorkMap workMap, DuccId duccId, Object object, String methodName) {
		IDuccWork dw = null;
		TrackSync ts = TrackSync.await(workMap, object.getClass(), methodName);
		synchronized(workMap) {
			ts.using();
			dw = workMap.findDuccWork(duccId);
		}
		ts.ended();
		return dw;
	}
	
	public static IDuccWork findDuccWork(DuccWorkMap workMap, DuccType duccType, String id, Object object, String methodName) {
		IDuccWork dw = null;
		TrackSync ts = TrackSync.await(workMap, object.getClass(), methodName);
		synchronized(workMap) {
			ts.using();
			dw = workMap.findDuccWork(duccType, id);
		}
		ts.ended();
		return dw;
	}
	
	public static DuccWorkMap deepCopy(DuccWorkMap workMap, Object object, String methodName) {
		DuccWorkMap workMapCopy = null;
		TrackSync ts = TrackSync.await(workMap, object.getClass(), methodName);
		synchronized(workMap) {
			ts.using();
			workMapCopy = workMap.deepCopy();
		}
		ts.ended();
		return workMapCopy;
	}
	
}
