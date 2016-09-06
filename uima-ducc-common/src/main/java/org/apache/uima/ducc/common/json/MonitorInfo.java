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

package org.apache.uima.ducc.common.json;

import java.util.ArrayList;
import java.util.List;

public class MonitorInfo {
	public String code = "0";
	public ArrayList<String> stateSequence = new ArrayList<String>();
	public String rationale = "";
	public String total = "0";
	public String done  = "0";
	public String error = "0";
	public String retry = "0";
	public String procs = "0";
	public List<String> remotePids = new ArrayList<String>();
	public List<String> errorLogs = new ArrayList<String>();
	public List<String> nodes = new ArrayList<String>();
	
	private int compareStringNumbers(String sn1, String sn2) {
		int retVal = 0;
		try {
			Integer n1 = Integer.parseInt(sn1);
			Integer n2 = Integer.parseInt(sn2);
			retVal = n1.compareTo(n2);
		}
		catch(Exception e) {
			// oh well...
		}
		return retVal;
	}
	
	public int compareWith(MonitorInfo that) {
		int retVal = 0;
		try {
			if(retVal == 0) {
				retVal = compareStringNumbers(this.total, that.total);
			}
			if(retVal == 0) {
				retVal = compareStringNumbers(this.done, that.done);
			}
			if(retVal == 0) {
				retVal = compareStringNumbers(this.error, that.error);
			}
			if(retVal == 0) {
				retVal = compareStringNumbers(this.retry, that.retry);
			}
		}
		catch(Exception e) {
			// oh well...
		}
		return retVal;
	}
	
	public boolean isRegression(MonitorInfo that) {
		boolean retVal = false;
		if(that != null) {
			if(compareWith(that) < 0) {
				retVal = true;
			}
		}
		return retVal;
	}
}
