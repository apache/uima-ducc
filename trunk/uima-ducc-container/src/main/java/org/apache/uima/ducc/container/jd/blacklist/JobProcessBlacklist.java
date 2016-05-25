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
package org.apache.uima.ducc.container.jd.blacklist;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.container.common.MessageBuffer;
import org.apache.uima.ducc.container.common.Standardize;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteWorkerProcess;

public class JobProcessBlacklist {

	private static ILogger logger = Logger.getLogger(JobProcessBlacklist.class, IComponent.Id.JD.name());
	
	private static ConcurrentHashMap<IRemoteWorkerProcess,Long> map = new ConcurrentHashMap<IRemoteWorkerProcess,Long>();
	
	private static JobProcessBlacklist instance = new JobProcessBlacklist();
	
	public static JobProcessBlacklist getInstance() {
		return instance;
	}
	
	private static boolean disabled = false;
	
	public void add(IRemoteWorkerProcess rwp) {
		String location = "add";
		if(!disabled) {
			if(rwp != null) {
				if(!map.containsKey(rwp)) {
					Long time = new Long(System.currentTimeMillis());
					map.put(rwp, time);
					MessageBuffer mb = new MessageBuffer();
					mb.append(Standardize.Label.node.get()+rwp.getNodeName());
					mb.append(Standardize.Label.pid.get()+rwp.getPid());
					logger.debug(location, ILogger.null_id, mb.toString());
				}
			}
		}
	}
	
	public void remove(IRemoteWorkerProcess rwp) {
		String location = "remove";
		if(!disabled) {
			if(rwp != null) {
				map.remove(rwp);
				MessageBuffer mb = new MessageBuffer();
				mb.append(Standardize.Label.node.get()+rwp.getNodeName());
				mb.append(Standardize.Label.pid.get()+rwp.getPid());
				logger.debug(location, ILogger.null_id, mb.toString());
			}
		}
	}
	
	public boolean includes(IRemoteWorkerProcess rwp) {
		String location = "includes";
		boolean retVal = false;
		if(!disabled) {
			if(rwp != null) {
				retVal = map.containsKey(rwp);
				MessageBuffer mb = new MessageBuffer();
				mb.append(Standardize.Label.node.get()+rwp.getNodeName());
				mb.append(Standardize.Label.pid.get()+rwp.getPid());
				mb.append(Standardize.Label.size.get()+map.size());
				mb.append(Standardize.Label.value.get()+retVal);
				logger.trace(location, ILogger.null_id, mb.toString());
			}
		}
		return retVal;
	}
	
	public void disable() {
		disabled = true;
	}
}
