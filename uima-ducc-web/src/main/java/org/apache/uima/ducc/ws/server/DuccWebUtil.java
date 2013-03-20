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

import java.util.ArrayList;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.uima.ducc.transport.event.common.IDuccWork;

public class DuccWebUtil {

	/*
	@Deprecated
	protected String getUserHome(String userName) throws IOException{
	    return new BufferedReader(new InputStreamReader(Runtime.getRuntime().exec(new String[]{"sh", "-c", "echo ~" + userName}).getInputStream())).readLine();
	}
	*/

	public static final void noCache(HttpServletResponse response) {
		response.setHeader("Cache-Control", "no-cache, no-store, must-revalidate"); // HTTP 1.1.
		response.setDateHeader("Expires", 0); // Proxies.
	}
	
	public static final boolean isListable(HttpServletRequest request, ArrayList<String> users, int maxRecords, int counter, IDuccWork dw) {
		boolean list = false;
		DuccCookies.FilterUsersStyle filterUsersStyle = DuccCookies.getFilterUsersStyle(request);
		if(!users.isEmpty()) {
			String jobUser = dw.getStandardInfo().getUser().trim();
			switch(filterUsersStyle) {
			case IncludePlusActive:
				if(!dw.isCompleted()) {
					list = true;
				}
				else if(users.contains(jobUser)) {
					if(maxRecords > 0) {
						if (counter < maxRecords) {
							list = true;
						}
					}
				}
				break;
			case ExcludePlusActive:
				if(!dw.isCompleted()) {
					list = true;
				}
				else if(!users.contains(jobUser)) {
					if(maxRecords > 0) {
						if (counter < maxRecords) {
							list = true;
						}
					}
				}
				break;
			case Include:
				if(users.contains(jobUser)) {
					if(maxRecords > 0) {
						if (counter < maxRecords) {
							list = true;
						}
					}
				}
				break;
			case Exclude:
				if(!users.contains(jobUser)) {
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
			if(!dw.isCompleted()) {
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
}
