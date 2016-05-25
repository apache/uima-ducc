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
package org.apache.uima.ducc.ws.registry.sort;

import java.util.Properties;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.sm.IService;

public class SortableService extends ServiceAdapter implements Comparable<SortableService> {
	
	private static DuccLogger duccLogger = DuccLoggerComponents.getWsLogger(SortableService.class.getName());
	private static DuccId jobid = null;
	
	public SortableService(Properties svc, Properties meta) {
		super(svc, meta);
	}
	
	private int compareId(SortableService that) {
		int retVal = 0;
		Integer i1 = new Integer(this.getId());
		Integer i2 = new Integer(that.getId());
		retVal = i2.compareTo(i1);
		return retVal;
	}
	
	private int ordinalOf(String state) {
		int retVal = 0;
		if(state.equals(IService.ServiceState.Stopped.name())) {
			retVal = 10;
		}
		else if(state.equals(IService.ServiceState.Available.name())) {
			retVal = 20;
		}
		else if(state.equals(IService.ServiceState.Waiting.name())) {
			retVal = 30;
		}
		else if(state.equals(IService.ServiceState.Stopping.name())) {
			retVal = 40;
		}
		else if(state.equals(IService.ServiceState.Initializing.name())) {
			retVal = 50;
		}
		else if(state.equals(IService.ServiceState.Starting.name())) {
			retVal = 60;
		}
		else if(state.equals(IService.ServiceState.NotAvailable.name())) {
			retVal = 70;
		}
		else if(state.equals(IService.ServiceState.Undefined.name())) {
			retVal = 80;
		}
		return retVal;
	}
	
	private int compareState(SortableService that) {
		int retVal = 0;
		String s1 = this.getState();
		String s2 = that.getState();
		Integer v1 = ordinalOf(s1);
		Integer v2 = ordinalOf(s2);
		retVal = v2.compareTo(v1);
		return retVal;
	}
	
	private int compareAlert(SortableService that) {
		int retVal = 0;
		boolean a1 = this.isAlert();
		boolean a2 = that.isAlert();
		if(a2) {
			if(a1) {
				retVal = 0;
			}
			else {
				retVal = 1;
			}
		}
		else if(a1) {
			if(a2) {
				retVal = 0;
			}
			else {
				retVal = 0-1;
			}
		}
		return retVal;
	}
	
	@Override
	public int compareTo(SortableService object) {
		String location = "compareTo";
		int retVal = 0;
		SortableService that = object;
		retVal = compareAlert(that);
		if(retVal == 0) {
			retVal = this.compareState(that);
		}
		if(retVal == 0) {
			retVal = this.compareId(that);
		}
		String id1 = "id1:"+this.getId();
		String id2 = "id2:"+that.getId();
		String state1 = "state1:"+this.getState();
		String state2 = "state2:"+that.getState();
		String text = id1+" "+id2+" "+state1+" "+state2+" "+"rc:"+retVal;
		duccLogger.trace(location, jobid, text);
		return retVal;
	}
}
