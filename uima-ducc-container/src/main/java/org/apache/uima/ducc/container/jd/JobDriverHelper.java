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
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.container.common.ContainerLogger;
import org.apache.uima.ducc.container.common.IContainerLogger;
import org.apache.uima.ducc.container.common.IEntityId;
import org.apache.uima.ducc.container.common.MessageBuffer;
import org.apache.uima.ducc.container.common.Standardize;
import org.apache.uima.ducc.container.jd.mh.iface.IWorkItemInfo;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteWorkerIdentity;
import org.apache.uima.ducc.container.jd.mh.impl.WorkItemInfo;
import org.apache.uima.ducc.container.jd.wi.IWorkItem;

public class JobDriverHelper {

	private static IContainerLogger logger = ContainerLogger.getLogger(JobDriverHelper.class, IContainerLogger.Component.JD.name());
	
	private static JobDriverHelper instance = new JobDriverHelper();
	
	public static JobDriverHelper getInstance() {
		return instance;
	}
	
	public ArrayList<IWorkItemInfo> getActiveWotrkItemInfo() {
		String location = "getActiveWotrkItemInfo";
		ArrayList<IWorkItemInfo> list = new ArrayList<IWorkItemInfo>();
		JobDriver jd = JobDriver.getInstance();
		ConcurrentHashMap<IRemoteWorkerIdentity, IWorkItem> map = jd.getMap();
		for(Entry<IRemoteWorkerIdentity, IWorkItem> entry : map.entrySet()) {
			IRemoteWorkerIdentity rwi = entry.getKey();
			IWorkItem wi = entry.getValue();
			IWorkItemInfo wii = new WorkItemInfo();
			wii.setNodeAddress(rwi.getNodeAddress());
			wii.setNodeName(rwi.getNodeName());
			wii.setPid(rwi.getPid());
			wii.setTid(rwi.getTid());
			//TODO
			wii.setSeqNo(0);
			wii.setOperatingMillis(wi.getMillisOperating());
			list.add(wii);
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.node.get()+wii.getNodeName());
			mb.append(Standardize.Label.pid.get()+wii.getPid());
			mb.append(Standardize.Label.tid.get()+wii.getTid());
			mb.append(Standardize.Label.operatingMillis.get()+wii.getOperatingMillis());
			logger.debug(location, IEntityId.null_id, mb);
		}
		return list;
	}
}
