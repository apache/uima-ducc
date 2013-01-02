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
package org.apache.uima.ducc.jd;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;

public class ExceptionClassifier {
	
	private static DuccLogger duccOut = DuccLoggerComponents.getJdOut(JobDriver.class.getName());
	
	public enum AnalysisOfCause {
		RemoteTimeout,
		LocalTimeout,
		Interrupted,
		Other
	}
	
	private static boolean contains(String s1, String s2) {
		if(s1 == null) {
			return false;
		}
		if(s2 == null) {
			return false;
		}
		return s1.contains(s2);
	}	
	
	private static AnalysisOfCause getTimeoutType(int level) {
		AnalysisOfCause retVal;
		if(level > 0) {
			retVal = AnalysisOfCause.RemoteTimeout;
		}
		else {
			retVal = AnalysisOfCause.LocalTimeout;
		}
		return retVal;
	}
	
	public static AnalysisOfCause getAnalysisOfCause(Exception e) {
		String location = "getAnalysisOfCause";
		AnalysisOfCause retVal = AnalysisOfCause.Other;
		if(e != null) {
			Throwable nextCause = e.getCause();
			int level = 0;
			while( nextCause != null ) {
				String sCause = nextCause.toString();
				duccOut.trace(location, null, sCause);
				if(contains(sCause,"org.apache.uima.aae.error.UimaASProcessCasTimeout")) {
					retVal = getTimeoutType(level);
					break;
        		}
				else if(contains(sCause,"org.apache.uima.aae.error.UimaASPingTimeout")) {
					retVal = getTimeoutType(level);
					break;
				}
				else if(contains(sCause,"java.lang.InterruptedException")) {
					retVal = AnalysisOfCause.Interrupted;
				}
        		nextCause = nextCause.getCause();
        		level++;
			}
		}
		duccOut.trace(location, null, retVal);
		return retVal;
	}
	
	public static boolean isTimeout(Exception e) {
		String location = "isTimeout";
		boolean retVal = false;
		switch(getAnalysisOfCause(e)) {
		case LocalTimeout:
			retVal = true;
			break;
		}
		duccOut.trace(location, null, retVal);
		return retVal;
	}
	
	public static boolean isInterrupted(Exception e) {
		String location = "isInterrupted";
		boolean retVal = false;
		switch(getAnalysisOfCause(e)) {
		case Interrupted:
			retVal = true;
			break;
		}
		duccOut.trace(location, null, retVal);
		return retVal;
	}
}
