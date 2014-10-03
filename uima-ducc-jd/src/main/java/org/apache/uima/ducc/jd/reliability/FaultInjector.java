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

package org.apache.uima.ducc.jd.reliability;

import java.util.Random;

import org.apache.uima.aae.client.UimaASProcessStatus;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.jd.IJobDriver;
import org.apache.uima.ducc.jd.client.WorkItem;

public class FaultInjector {
	
	private static DuccLogger duccOut = DuccLoggerComponents.getJdOut(FaultInjector.class.getName());
	
	private static FaultInjector instance = new FaultInjector();
	
	public static FaultInjector getInstance() {
		return instance;
	}
	
	private Random random = new Random();
	
	// Warning: setting enabled to true will inject faults by randomly ignoring UIMA-AS client callbacks
	
	private boolean enabled = false;
	
	private enum Type { CallBack1, CallBack2 };
	
	private double pct1 = 0.1;
	private double pct2 = 0.1;

	private FaultInjector() {
		try {
			initialize();
		}
		catch(Exception e) {
		}
	}
	
	private void initialize() {
		DuccPropertiesResolver dpr = DuccPropertiesResolver.getInstance();
		String cb1 = dpr.getProperty("ducc.jd.fault.injector.CallBack1");
		double dv1 = initValue(cb1);
		if(dv1 > 0.0) {
			pct1 = dv1;
			enabled = true;
		}
		String cb2 = dpr.getProperty("ducc.jd.fault.injector.CallBack2");
		double dv2 = initValue(cb2);
		if(dv2 > 0.0) {
			pct2 = dv2;
			enabled = true;
		}
	}
	
	private double initValue(String value) {
		double retVal = -1;
		try {
			if(value != null) {
				double dVal = Double.parseDouble(value);
				if(dVal > 0) {
					if(dVal < 1) {
						retVal = dVal;
					}
				}
			} 
		}
		catch(Exception e) {
		}
		return retVal;
	}
	
	private boolean isFault(DuccId jobid, int seqNo, Type type) {
		String location = "isFault";
		boolean retVal = false;
		try {
			if(enabled) {
				double d1 = random.nextDouble();
				double d0 = d1;
				switch(type) {
				case CallBack1:
					d0 = pct1;
					break;
				case CallBack2:
					d0 = pct2;
					break;
				}
				if(d1 < d0) {
					duccOut.info(location, jobid, "seqNo:"+seqNo+" "+"type:"+type.name());
					retVal = true;
				}
			}
		}
		catch(Exception e) {
			duccOut.error(location, jobid, e);
		}
		return retVal;
	}
	
	private boolean isFault(UimaASProcessStatus status, IJobDriver jobDriver, Type type) {
		String location = "isFault";
		DuccId jobid = null;
		boolean retVal = false;
		try {
			jobid = jobDriver.getJob().getDuccId();
			String casId = ""+status.getCAS().hashCode();
			WorkItem wi = jobDriver.getWorkItem(casId);
			int seqNo = wi.getSeqNo();
			retVal = isFault(jobid, seqNo, type);
		}
		catch(Exception e) {
			duccOut.error(location, jobid, e);
		}
		return retVal;
	}
	
	public boolean isFaultCallBack1(UimaASProcessStatus status, IJobDriver jobDriver) {
		return isFault(status, jobDriver, Type.CallBack1);
	}
	
	public boolean isFaultCallBack2(UimaASProcessStatus status, IJobDriver jobDriver) {
		return isFault(status, jobDriver, Type.CallBack2);
	}
	
}
