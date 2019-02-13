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
package org.apache.uima.ducc.common.jd.files.workitem.legacy;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.uima.ducc.common.jd.files.IPersistenceWorkItemState;
import org.apache.uima.ducc.common.jd.files.IWorkItemState;
import org.apache.uima.ducc.common.utils.IOHelper;

@Deprecated
public class WorkItemStateSerializedObjects implements IPersistenceWorkItemState {
	
	public static final String work_item_status_ser = "work-item-status.ser";
	
	private String filename = null;
	
	public WorkItemStateSerializedObjects(String directory) {
		initialize(directory);
	}
	
	
	public void initialize(String directory) {
		this.filename = IOHelper.marryDir2File(directory,work_item_status_ser);
	}

	
	public void exportData(ConcurrentSkipListMap<Long, IWorkItemState> map) throws IOException {
		FileOutputStream fos = null;
		ObjectOutputStream out = null;
		try {
			fos = new FileOutputStream(filename);
			out = new ObjectOutputStream(fos);
			out.writeObject(map);
			out.close();
		}
		finally {
			if(out != null) {
				out.close();
			}
		}
		
		return;
	}

	@SuppressWarnings("unchecked")
	
	public ConcurrentSkipListMap<Long, IWorkItemState> importData() throws IOException, ClassNotFoundException {
		ConcurrentSkipListMap<Long,IWorkItemState> map = null;
		FileInputStream fis = null;
		ObjectInputStream in = null;
		try {
			fis = new FileInputStream(filename);
			in = new ObjectInputStream(fis);
			map = (ConcurrentSkipListMap<Long,IWorkItemState>)in.readObject();
			in.close();
		}
		finally {
			if(in != null) {
				in.close();
			}
		}
		return map;
	}

}
