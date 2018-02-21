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

import org.apache.uima.ducc.transport.event.common.IDuccProcess.ReasonForStoppingProcess;
import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;

public class DuccProcessHelper {

	/**
	 * Determine if Process has failed due to User or Framework.
	 * Note that Framework attributed Process failures are not
	 * counted toward the maximum number of failures allowed
	 * before the Job is forcibly terminated.
	 * 
	 * @param process is the IDuccProcess to consider
	 * @return true if User or false if Framework
	 */
	public static boolean isFailedProcess(IDuccProcess process) {
		boolean retVal = false;
		if(process != null) {
			ProcessState processState = process.getProcessState();
			switch(processState) {
			case Failed:
			case Stopped:
			case Killed:
				normalizeReasonForStoppingProcess(process);
				retVal = DuccProcessHelper.isUserFailureReasonForStoppingProcess(process);
			}
		}
		return retVal;
	}

	private static String normalizeReasonForStoppingProcess(IDuccProcess process) {
		String retVal = IDuccProcess.ReasonForStoppingProcess.Unexplained.name();
		if(process != null) {
			String reason = process.getReasonForStoppingProcess();
			if(reason != null) {
				if(reason.trim().length() > 0) {
					retVal = reason;
				}
				else {
					process.setReasonForStoppingProcess(retVal);
				}
			}
			else {
				process.setReasonForStoppingProcess(retVal);
			}
		}
		return retVal;
	}
	
	public static boolean isUserFailureReasonForStoppingProcess(IDuccProcess process) {
		boolean retVal = false;
		if(process != null) {
			String reason = process.getReasonForStoppingProcess();
			if(reason != null) {
				if(reason.equals(ReasonForStoppingProcess.Croaked.name())) {
					retVal = true;
				}
				else if(reason.equals(ReasonForStoppingProcess.ExceededShareSize.name())) {
					retVal = true;
				}
				else if(reason.equals(ReasonForStoppingProcess.ExceededSwapThreshold.name())) {
					retVal = true;
				}
				else if(reason.equals(ReasonForStoppingProcess.ExceededErrorThreshold.name())) {
					retVal = true;
				}
				else if(reason.equals(ReasonForStoppingProcess.Unexplained.name())) {
					retVal = true;
				}
			}
		}
		return retVal;
	}
	
}
