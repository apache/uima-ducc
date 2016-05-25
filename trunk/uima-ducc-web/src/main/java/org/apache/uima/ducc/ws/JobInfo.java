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
package org.apache.uima.ducc.ws;

import org.apache.uima.ducc.transport.event.common.DuccWorkJob;

public class JobInfo implements Comparable<JobInfo> {

	private DuccWorkJob _job;
	
	public JobInfo(DuccWorkJob job) {
		_job = job;
	}

	public DuccWorkJob getJob() {
		return _job;
	}
	
	public boolean isOperational() {
		return _job.isOperational();
	}
	
	
	public int compareTo(JobInfo job) {
		int retVal = 0;
		JobInfo j1 = this;
		JobInfo j2 = job;
		long f1 = j1.getJob().getDuccId().getFriendly();
		long f2 = j2.getJob().getDuccId().getFriendly();
		if(f1 != f2) {
			if(!j1.isOperational() && j2.isOperational()) {
				retVal = 1;
			}
			else if(j1.isOperational() && !j2.isOperational()) {
				retVal = -1;
			}
			else if(f1 > f2) {
				retVal = -1;
			}
			else if(f1 < f2) {
				retVal = 1;
			}
		}
		return retVal;
	}
	
	 
	public boolean equals(Object object) {
		boolean retVal = false;
		try {
			JobInfo i1 = this;
			JobInfo i2 = (JobInfo)object;
			DuccWorkJob j1 = i1.getJob();
			DuccWorkJob j2 = i2.getJob();
			String s1 = j1.getDuccId().toString();
			String s2 = j2.getDuccId().toString();
			retVal = s1.equals(s2);
		}
		catch(Throwable t) {	
		}
		return retVal;
	}
	
	 
	public int hashCode() {
		JobInfo i1 = this;
		DuccWorkJob j1 = i1.getJob();
		String s1 = j1.getDuccId().toString();
		return s1.hashCode();
	}
}
