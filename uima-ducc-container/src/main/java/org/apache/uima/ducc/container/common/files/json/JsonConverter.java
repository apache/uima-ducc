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
package org.apache.uima.ducc.container.common.files.json;

import java.lang.reflect.Type;
import java.util.concurrent.ConcurrentHashMap;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class JsonConverter {

	private static Gson gson = new Gson();
	
	private static String linend = "\n";
	
	public static String workItemStateToJson(IJsonWorkItemState jsonObject) {
		String json = gson.toJson(jsonObject)+linend;
		return json;
	}
	
	public static IJsonWorkItemState workItemStateFromJson(String jsonString) {
		Type typeOfMap = new TypeToken<JsonWorkItemState>() { }.getType();
		IJsonWorkItemState retVal = gson.fromJson(jsonString, typeOfMap);
		return retVal;
	}
	
	public static String workItemStateMapToJson(ConcurrentHashMap<String,JsonWorkItemState> jsonObject) {
		String json = gson.toJson(jsonObject);
		return json;
	}
	
	public static ConcurrentHashMap<String,JsonWorkItemState> workItemStateMapFromJson(String jsonString) {
		Type typeOfMap = new TypeToken<ConcurrentHashMap<String,JsonWorkItemState>>() { }.getType();
		ConcurrentHashMap<String,JsonWorkItemState> retVal = gson.fromJson(jsonString, typeOfMap);
		return retVal;
	}
}
