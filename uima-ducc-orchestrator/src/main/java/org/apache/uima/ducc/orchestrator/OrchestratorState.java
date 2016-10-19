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
package org.apache.uima.ducc.orchestrator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Type;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class OrchestratorState {

	private static final DuccLogger logger = DuccLoggerComponents.getOrLogger(OrchestratorState.class.getName());
	
	private static OrchestratorCommonArea orchestratorCommonArea = OrchestratorCommonArea.getInstance();
	private static String fileName = orchestratorCommonArea.getStateDirectory()+File.separator+"orchestrator-state.json";
	
	private static Gson gson = new Gson();
	private static OrchestratorState instance = null;
	private static DuccId jobid = null;
	
	private long sequenceNumberState = -1;
	
	public static OrchestratorState getInstance() {
		String location = "getInstance";
		synchronized(OrchestratorState.class) {
			logger.debug(location, jobid, ""+instance);
			if(instance == null) {
				instance = new OrchestratorState();
				instance.initialize();
			}
		}
		return instance;
	}
	
	private void initialize() {
		String location = "initialize";
		logger.debug(location, jobid, ""+instance);
		importState();
	}
	
	public long getNextSequenceNumberState() {
		String location = "getNextSequenceNumberState";
		synchronized(this) {
			sequenceNumberState++;
			exportState();
			logger.debug(location, jobid, ""+sequenceNumberState);
			return sequenceNumberState;
		}
	}
	
	public void setNextSequenceNumberState(long value) {
		String location = "setNextSequenceNumberState";
		synchronized(this) {
			sequenceNumberState = value;
			exportState();
			logger.debug(location, jobid, ""+sequenceNumberState);
		}
	}
	
	private void copy(OrchestratorState importedState) {
		String location = "copy";
		if(importedState != null) {
			setNextSequenceNumberState(importedState.getNextSequenceNumberState());
		}
		else {
			logger.warn(location, jobid, "no previous state found");
		}
	}
	
	private void importState() {
		String location = "importState";
		try {
			importer();
		}
		catch(Exception e) {
			logger.warn(location, jobid, e);
		}
	}
	
	private void importer() throws IOException {
		String location = "importer";
		FileReader fr = null;
		BufferedReader br = null;
		try {
			logger.debug(location, jobid, fileName);
			fr = new FileReader(fileName);
			br = new BufferedReader(fr);
			Type typeOfMap = new TypeToken<OrchestratorState>() { }.getType();
			OrchestratorState importedState = gson.fromJson(br, typeOfMap);
			br.close();
			fr.close();
			copy(importedState);
		}
		finally {
			if(br != null) {
				br.close();
			}
			if(fr != null) {
				fr.close();
			}
		}
	}
	
	private void exportState() {
		String location = "exportState";
		try {
			exporter();
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
	}
	
	private void exporter() throws IOException {
		String location = "exporter";
		FileWriter fw = null;
		try {
			logger.debug(location, jobid, fileName);
			String json = gson.toJson(this);
			fw = new FileWriter(fileName);
			fw.write(json);
			fw.close();
		}
		finally {
			if(fw != null) {
				fw.close();
			}
		}
	}
	
}
