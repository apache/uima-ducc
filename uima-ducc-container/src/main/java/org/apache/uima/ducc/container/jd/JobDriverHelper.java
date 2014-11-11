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
package org.apache.uima.ducc.container.jd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.container.common.ContainerLogger;
import org.apache.uima.ducc.container.common.IContainerLogger;
import org.apache.uima.ducc.container.common.IEntityId;
import org.apache.uima.ducc.container.common.MessageBuffer;
import org.apache.uima.ducc.container.common.Standardize;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteWorkerIdentity;
import org.apache.uima.ducc.container.jd.wi.IWorkItem;

public class JobDriverHelper {

	private static IContainerLogger logger = ContainerLogger.getLogger(JobDriverHelper.class, IContainerLogger.Component.JD.name());
	
	private static JobDriverHelper instance = new JobDriverHelper();
	
	public static JobDriverHelper getInstance() {
		return instance;
	}
	
	public HashMap<String,ArrayList<String>> getMapOperating() {
		String location = "getMapOperating";
		HashMap<String,ArrayList<String>> mapOperating = new HashMap<String, ArrayList<String>>();
		JobDriver jd = JobDriver.getInstance();
		ConcurrentHashMap<IRemoteWorkerIdentity, IWorkItem> map = jd.getMap();
		for(Entry<IRemoteWorkerIdentity, IWorkItem> entry : map.entrySet()) {
			IRemoteWorkerIdentity rwi = entry.getKey();
			String node = rwi.getNode();
			String pid = ""+rwi.getPid();
			ArrayList<String> list = null;
			if(!mapOperating.containsKey(node)) {
				list = new ArrayList<String>();
				mapOperating.put(node,list);
			}
			else {
				list = mapOperating.get(node);
			}
			if(!list.contains(pid)) {
				list.add(pid);
				MessageBuffer mb = new MessageBuffer();
				mb.append(Standardize.Label.node.get()+node);
				mb.append(Standardize.Label.pid.get()+pid);
				logger.debug(location, IEntityId.null_id, mb);
			}
		}
		return mapOperating;
	}
}
