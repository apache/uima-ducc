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
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.persistence.services.StateServicesSet;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.ws.registry.IServicesRegistry;
import org.apache.uima.ducc.ws.registry.ServicesRegistryMapPayload;
import org.apache.uima.ducc.ws.registry.sort.IServiceAdapter;

public class DuccWebUtil {

	private static DuccLogger duccLogger = DuccLoggerComponents.getWsLogger(DuccWebUtil.class.getName());
	
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
	
	private static String key_user = IServicesRegistry.user;
	private static String key_state = IServicesRegistry.service_state;
	
	private static String value_NotAvailable = IServicesRegistry.constant_NotAvailable;
	
	public static boolean isAvailable(Properties propertiesMeta) {
		boolean retVal = true;
		try {
			String state = propertiesMeta.getProperty(key_state).trim();
			if(state.equalsIgnoreCase(value_NotAvailable)) {
				retVal = false;
			}
		}
		catch(Exception e) {
			retVal = false;
		}
		return retVal;
	}
	
	public static final boolean isListable(HttpServletRequest request, ArrayList<String> users, int maxRecords, int counter, IServiceAdapter serviceAdapter) {
		Properties propertiesMeta = serviceAdapter.getMeta();
		return isListable(request, users, maxRecords, counter, propertiesMeta);
	}
	
	public static final boolean isListable(HttpServletRequest request, ArrayList<String> users, int maxRecords, int counter, ServicesRegistryMapPayload entry) {
		Properties propertiesMeta = entry.get(IServicesRegistry.meta);
		return isListable(request, users, maxRecords, counter, propertiesMeta);
	}
	
	public static final boolean isListable(HttpServletRequest request, ArrayList<String> users, int maxRecords, int counter, StateServicesSet entry) {
		Properties propertiesMeta = entry.get(IServicesRegistry.meta);
		return isListable(request, users, maxRecords, counter, propertiesMeta);
	}
	
	public static final boolean isListable(HttpServletRequest request, ArrayList<String> users, int maxRecords, int counter, Properties propertiesMeta) {	
		boolean list = false;
		DuccCookies.FilterUsersStyle filterUsersStyle = DuccCookies.getFilterUsersStyle(request);
		if(!users.isEmpty()) {
			String user = propertiesMeta.getProperty(key_user);
			if(user != null) {
				user = user.trim();
				switch(filterUsersStyle) {
				case IncludePlusActive:
					if(isAvailable(propertiesMeta)) {
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
					if(isAvailable(propertiesMeta)) {
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
		}
		else {
			if(isAvailable(propertiesMeta)) {
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
	
	
	public static ArrayList<String> getRemotePids(DuccId duccId, Map<DuccId, IDuccProcess> map) {
		String location = "getRemotePids";
		ArrayList<String> list = new ArrayList<String>();
		if(map != null) {
			if(map.size() > 0) {
				for(Entry<DuccId, IDuccProcess> entry : map.entrySet()) {
					IDuccProcess proc = entry.getValue();
					NodeIdentity nodeIdentity = proc.getNodeIdentity();
					String host = nodeIdentity.getName();
					if(host != null) {
						String pid = proc.getPID();
						if(pid != null) {
							String remotePid = pid+"@"+host;
							list.add(remotePid);
							duccLogger.debug(location, duccId, remotePid);
						}
					}
				}
			}
			else {
				duccLogger.debug(location, duccId, "map is empty");
			}
		}
		else {
			duccLogger.debug(location, duccId, "map is null");
		}
		return list;
	}
}
