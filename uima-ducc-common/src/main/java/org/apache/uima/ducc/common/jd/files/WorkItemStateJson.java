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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.uima.ducc.common.utils.IOHelper;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

@Deprecated
public class WorkItemStateJson implements IPersistenceWorkItemState {
	
	public static final String work_item_status_json = "work-item-status.json";
	
	private String filename = null;
	
	private Gson gson = new Gson();
	
	public WorkItemStateJson(String directory) {
		initialize(directory);
	}
	
	
	public void initialize(String directory) {
		this.filename = IOHelper.marryDir2File(directory,work_item_status_json);
	}

	
	public void exportData(ConcurrentSkipListMap<Long, IWorkItemState> map) throws IOException {
		FileWriter fw = null;
		try {
			String json = gson.toJson(map);
			fw = new FileWriter(filename);
			fw.write(json);
			fw.close();
		}
		finally {
			if(fw != null) {
				fw.close();
			}
		}
	}

	
	public ConcurrentSkipListMap<Long, IWorkItemState> importData() throws IOException, ClassNotFoundException {
		ConcurrentSkipListMap<Long, IWorkItemState> map = null;
		FileReader fr = null;
		BufferedReader br = null;
		try {
			fr = new FileReader(filename);
			br = new BufferedReader(fr);
			Type typeOfMap = new TypeToken<ConcurrentSkipListMap<String,WorkItemState>>() { }.getType();
			map = gson.fromJson(br, typeOfMap);
			br.close();
			fr.close();
		}
		finally {
			if(br != null) {
				br.close();
			}
			if(fr != null) {
				fr.close();
			}
		}
		return map;
	}

}
