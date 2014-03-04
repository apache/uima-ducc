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
package org.apache.uima.ducc.common.jd.files.workitem;

import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.uima.ducc.common.jd.files.IWorkItemState;
import org.apache.uima.ducc.common.utils.IOHelper;
import org.apache.uima.ducc.common.utils.id.DuccId;

import com.google.gson.Gson;

public abstract class WorkItemStateAbstract {

	public static String work_item_state = "work-item-state";
	public static String infoActive = ".active";
	
	public static String typeJson = ".json";
	public static String typeJsonGz = ".json.gz";

	public final String encoding = "UTF-8";
	
	public static String linend = "\n";
	public static boolean append = true;

	protected static Gson gson = new Gson();
	
	protected static DuccId jobid = null;
	
	protected String fnJson = null;
	protected String fnJsonGz = null;
	protected String fnActiveJson = null;
	
	protected ConcurrentSkipListMap<Long, IWorkItemState> activeMap = new ConcurrentSkipListMap<Long, IWorkItemState>();
	
	protected WorkItemStatistics stats = new WorkItemStatistics();
	
	protected void initialize(String directory) {
		fnJson = IOHelper.marryDir2File(directory,work_item_state+typeJson);
		fnJsonGz = IOHelper.marryDir2File(directory,work_item_state+typeJsonGz);
		fnActiveJson = IOHelper.marryDir2File(directory,work_item_state+infoActive+typeJson);
	}
	
}
