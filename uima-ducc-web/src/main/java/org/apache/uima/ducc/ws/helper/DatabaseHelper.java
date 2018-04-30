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
package org.apache.uima.ducc.ws.helper;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

import org.apache.uima.ducc.common.db.DbHelper;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.ws.db.DbQuery;

public class DatabaseHelper extends JmxHelper {
	
	private static DuccLogger logger = DuccLogger.getLogger(DatabaseHelper.class);
	private static DuccId jobid = null;
	
	private static DatabaseHelper instance = new DatabaseHelper();
	
	public static DatabaseHelper getInstance() {
		return instance;
	}

	protected boolean enabled = false;
	protected String[] host_list = null;
	
	private DatabaseHelper() {
		init();
	}
	
	private void init() {
		String location = "init";
		try {
			host_list = DbHelper.getHostList();
			enabled = DbHelper.isDbEnabled();
			if(enabled) {
				String jmx_host = DbHelper.getJxmHostString();
				setJmxHost(jmx_host);
				Integer jmx_port = DbHelper.getJxmPortInteger();
				setJmxPort(jmx_port); 
				jmxConnect();
			}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
	}
	
	public String getHostListString() {
		StringBuffer sb = new StringBuffer();
		for(String host : host_list) {
			sb.append(host);
			sb.append(" ");
		}
		String retVal = sb.toString().trim();
		return retVal;
	}
	
	public boolean isEnabled() {
		return enabled;
	}
	
	public boolean isDisabled() {
		return !enabled;
	}
	
	// Runtime Info //
	
	public boolean isAlive() {
		boolean retVal = DbQuery.getInstance().isUp();
		return retVal;
	}
	
	public Long getStartTime() {
		String location = "getStartTime";
		Long retVal = new Long(0);
		Object o = null;
		MBeanServerConnection mbsc = null;
		try {
			mbsc = getMBSC();
			o = mbsc.getAttribute(new ObjectName("java.lang:type=Runtime"), "StartTime");
			retVal = (Long) o;
		}
		catch(Exception e) {
			try {
				reconnect();
				mbsc = getMBSC();
				o = mbsc.getAttribute(new ObjectName("java.lang:type=Runtime"), "StartTime");
				retVal = (Long) o;
			}
			catch(Exception e2) {
				logger.error(location, jobid, e2);
			}
		}
		return retVal;
	}
	
	@Override
	protected void reconnect() {
		String location = "reconnect";
		init();
		logger.debug(location, jobid, "reconnected");
	}
	
}
