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
package org.apache.uima.ducc.common.jd.files;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.uima.ducc.common.utils.IOHelper;


@SuppressWarnings("unchecked")
public class WorkItemStateManager {
	
	public static final String work_item_status_ser = "work-item-status.ser";
	
	private ConcurrentSkipListMap<Long,IWorkItemState> map = new ConcurrentSkipListMap<Long,IWorkItemState>();
	private String filename = null;
	
	public WorkItemStateManager(String dirname) {
		init(dirname);
	}
	
	private void init(String dirname) {
		this.filename = IOHelper.marryDir2File(dirname,work_item_status_ser);
	}
	
	public ConcurrentSkipListMap<Long,IWorkItemState> getMap() {
		return map;
	}
	
	public void exportData() {
		try {
			FileOutputStream fos = new FileOutputStream(filename);
			ObjectOutputStream out = new ObjectOutputStream(fos);
			out.writeObject(map);
			out.close();
			return;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void importData() {
		try {
			FileInputStream fis = new FileInputStream(filename);
			ObjectInputStream in = new ObjectInputStream(fis);
			map = (ConcurrentSkipListMap<Long,IWorkItemState>)in.readObject();
			in.close();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void start(int seqNo, String wiId) {
		IWorkItemState wis = new WorkItemState(seqNo);
		Long key = new Long(seqNo);
		map.put(key, wis);
		wis.setWiId(wiId);
		wis.stateStart();
	}
	
	public void queued(String seqNo) {
		Long key = new Long(seqNo);
		queued(key.intValue());
	}
	
	public void queued(int seqNo) {
		Long key = new Long(seqNo);
		IWorkItemState wis = map.get(key);
		wis.stateQueued();
	}
	
	public void operating(String seqNo) {
		Long key = new Long(seqNo);
		operating(key.intValue());
	}
	
	public void operating(int seqNo) {
		Long key = new Long(seqNo);
		IWorkItemState wis = map.get(key);
		wis.stateOperating();
	}
	
	public void ended(int seqNo) {
		Long key = new Long(seqNo);
		IWorkItemState wis = map.get(key);
		wis.stateEnded();
	}
	
	public void error(int seqNo) {
		Long key = new Long(seqNo);
		IWorkItemState wis = map.get(key);
		wis.stateError();
	}
	
	public void retry(int seqNo) {
		Long key = new Long(seqNo);
		IWorkItemState wis = map.get(key);
		wis.stateRetry();
	}
	
	public void location(String seqNo, String node, String pid) {
		Long key = new Long(seqNo);
		location(key.intValue(), node, pid);
	}
	
	public void location(int seqNo, String node, String pid) {
		Long key = new Long(seqNo);
		IWorkItemState wis = map.get(key);
		wis.setNode(node);
		wis.setPid(pid);
	}
}
