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

import java.util.Date;
import java.util.TimeZone;

import org.apache.uima.ducc.common.utils.SynchronizedSimpleDateFormat;

public class TimeWindow implements ITimeWindow {

	/**
	 * please increment this sUID when removing or modifying a field 
	 */
	private static final long serialVersionUID = 1L;
	private String timeStart = null;
	private String timeEnd = null;
	
	public TimeWindow() {
	}
	
	
	public String getStart() {
		return timeStart;
	}

	
	public void setStart(String time) {
		this.timeStart = time;
	}
	
	
	public long getStartLong() {
		long retVal = -1;
		try {
			retVal = Long.parseLong(getStart());
		}
		catch(Exception e) {
		}
		return retVal;
	}

	
	public void setStartLong(long time) {
		setStart(String.valueOf(time));
	}

	
	public String getEnd() {
		return timeEnd;
	}

	
	public void setEnd(String time) {
		this.timeEnd = time;
	}
	
	
	public long getEndLong() {
		long retVal = -1;
		try {
			retVal = Long.parseLong(getEnd());
		}
		catch(Exception e) {
		}
		return retVal;
	}

	
	public void setEndLong(long time) {
		setEnd(String.valueOf(time));
	}
	
	
	public String getDiff() {
		return ""+getElapsedMillis();
	}
	
	
	public String getElapsed() {
		String elapsed = "";
		long elapsedTime = Long.valueOf(getDiff());
		SynchronizedSimpleDateFormat dateFormat = new SynchronizedSimpleDateFormat("HH:mm:ss");
		dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		elapsed = dateFormat.format(new Date(elapsedTime));
		return elapsed;
	}
	
	
	public String getElapsed(IDuccWorkJob job) {
		String retVal = null;
		if(isEstimated()) {
			//if(job.isCompleted()) {
				long current = System.currentTimeMillis();
				long elapsed = getElapsedMillis();
				IDuccStandardInfo stdInfo = job.getStandardInfo();
				long t1 = stdInfo.getDateOfCompletionMillis();
				if(t1 == 0) {
					t1 = current;
				}
				long t0 = stdInfo.getDateOfSubmissionMillis();
				if(t0 == 0) {
					t0 = current;
				}
				long tmax = t1-t0;
				if(elapsed > tmax) {
					elapsed = t1 - getStartLong();
				}
				SynchronizedSimpleDateFormat dateFormat = new SynchronizedSimpleDateFormat("HH:mm:ss");
				dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
				retVal = dateFormat.format(new Date(elapsed));
			//}
		}
		else {
			retVal = getElapsed();
		}
		return retVal;
	}
	
	
	public long getElapsedMillis() {
		String t0 = getStart();
		String t1 = getEnd();
		String t = ""+System.currentTimeMillis();
		if(t0 == null) {
			t0 = t;
		}
		if(t1 == null) {
			t1 = t;
		}
		Long l1 = Long.valueOf(t1);
		Long l0 = Long.valueOf(t0);
		Long diff = l1-l0;
		return diff.longValue();
	}

	
	public boolean isEstimated() {
		boolean retVal = false;
		if(getStart() == null) {
			retVal = true;
		}
		else if(getEnd() == null) {
			retVal = true;
		}
		return retVal;
	}

}
