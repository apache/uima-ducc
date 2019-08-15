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
package org.apache.uima.ducc.ws.handlers.utilities;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.uima.ducc.common.utils.TimeStamp;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.DuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.ws.DuccData;
import org.apache.uima.ducc.ws.JobInfo;
import org.apache.uima.ducc.ws.server.DuccCookies;
import org.apache.uima.ducc.ws.server.DuccCookies.DateStyle;
import org.apache.uima.ducc.ws.server.DuccLocalConstants;
import org.apache.uima.ducc.ws.server.DuccLocalCookies;
import org.apache.uima.ducc.ws.xd.IExperiment;

public class HandlersUtilities {
	
	public static IDuccWorkJob getJob(DuccId duccId) {
		IDuccWorkJob retVal = null;
		DuccData duccData = DuccData.getInstance();
		ConcurrentSkipListMap<JobInfo,JobInfo> sortedJobs = duccData.getSortedJobs();
		if(sortedJobs.size()> 0) {
			Iterator<Entry<JobInfo, JobInfo>> iterator = sortedJobs.entrySet().iterator();
			while(iterator.hasNext()) {
				JobInfo jobInfo = iterator.next().getValue();
				DuccWorkJob job = jobInfo.getJob();
				DuccId jobid = job.getDuccId();
				if(jobid.equals(duccId)) {
					retVal = job;
					break;
				}
			}
		}
		return retVal;
	}
	
	public static ArrayList<String> getExperimentsUsers(HttpServletRequest request) {
		String cookie = DuccCookies.getCookie(request,DuccLocalCookies.cookieExperimentsUsers);
		return getUsers(cookie);
	}
	
	public static int getExperimentsMax(HttpServletRequest request) {
		int maxRecords = DuccLocalConstants.defaultRecordsExperiments;
		try {
			String cookie = DuccCookies.getCookie(request,DuccLocalCookies.cookieExperimentsMax);
			int reqRecords = Integer.parseInt(cookie);
			if(reqRecords <= DuccLocalConstants.maximumRecordsExperiments) {
				if(reqRecords > 0) {
					maxRecords = reqRecords;
				}
			}
		}
		catch(Exception e) {
		}
		return maxRecords;
	}
	
	private static ArrayList<String> getUsers(String usersString) {
		ArrayList<String> userRecords = new ArrayList<String>();
		try {
			String[] users = usersString.split("\\s+");
			if(users != null) {
				for(String user : users) {
					user = user.trim();
					if(user.length() > 0) {
						if(!userRecords.contains(user)) {
							userRecords.add(user);
						}
					}
				}
			}
		}
		catch(Exception e) {
		}
		return userRecords;
	}
	
	public static final boolean isListable(HttpServletRequest request, ArrayList<String> users, int maxRecords, int counter, IExperiment handle) {
		boolean list = false;
		DuccCookies.FilterUsersStyle filterUsersStyle = DuccCookies.getFilterUsersStyle(request);
		if(!users.isEmpty()) {
			String user = handle.getUser();
			switch(filterUsersStyle) {
			case IncludePlusActive:
				if(handle.isActive()) {
					list = true;
				}
				else if(users.contains(user)) {
					if(maxRecords > 0) {
						if (counter < maxRecords) {
							list = true;
						}
					}
				}
				break;
			case ExcludePlusActive:
				if(handle.isActive()) {
					list = true;
				}
				else if(!users.contains(user)) {
					if(maxRecords > 0) {
						if (counter < maxRecords) {
							list = true;
						}
					}
				}
				break;
			case Include:
				if(users.contains(user)) {
					if(maxRecords > 0) {
						if (counter < maxRecords) {
							list = true;
						}
					}
				}
				break;
			case Exclude:
				if(!users.contains(user)) {
					if(maxRecords > 0) {
						if (counter < maxRecords) {
							list = true;
						}
					}
				}
				break;
			}	
		}
		else {
			if(handle.isActive()) {
				list = true;
			}
			else if(maxRecords > 0) {
				if (counter < maxRecords) {
					list = true;
				}
			}
		}
		return list;
	}

	public static boolean isIgnorable(Throwable t) {
		boolean retVal = false;
		try {
			String rcm = ExceptionUtils.getMessage(t).trim();
			if(rcm.endsWith("java.io.IOException: Broken pipe")) {
				retVal = true;
			}
		}
		catch(Throwable throwable) {
		}
		return retVal;
	}
	
	private static String inFormat = "yyyy.MM.dd-HH:mm:ss";
	private static SimpleDateFormat inSdf = new SimpleDateFormat(inFormat);

	public static String reFormatDate(DateStyle dateStyle, String inDate) {
		String outDate = inDate;
		try { 
			Date date = inSdf.parse(inDate);
	        long millis = date.getTime();
	        String unformattedDate = TimeStamp.simpleFormat(""+millis);
	        outDate = ResponseHelper.getTimeStamp(dateStyle, unformattedDate);
		}
		catch(Exception e) {
			
		}
		return outDate;
	}

	public static long getMillis(String inDate) {
		long millis = 0;
		try {
			Date date = inSdf.parse(inDate);
	        millis = date.getTime();
		}
		catch(Exception e) {
			
		}
		return millis;
	}
}
