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
package org.apache.uima.ducc.orchestrator.state;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.orchestrator.OrchestratorCommonArea;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class OrchestratorStateFile implements IOrchestratorState {

	private static DuccLogger logger = DuccLogger.getLogger(OrchestratorStateFile.class);
	private static DuccId jobid = null;
	
	private static OrchestratorCommonArea orchestratorCommonArea = OrchestratorCommonArea.getInstance();
	private static String jsonFileName = orchestratorCommonArea.getStateDirectory()+File.separator+"orchestrator-state.json";
	private static String propertiesFileName = orchestratorCommonArea.getStateDirectory()+File.separator+"orchestrator.properties";
	
	private static Gson gson = new Gson();
	
	private static OrchestratorStateFile instance = null;
	
	private AtomicLong seqNoPublication = new AtomicLong(-1);
	
	private static final String constSeqNo = "seqno";
	
	public static IOrchestratorState getInstance() {
		String location = "getInstance";
		synchronized(OrchestratorStateFile.class) {
			logger.debug(location, jobid, ""+instance);
			if(instance == null) {
				instance = new OrchestratorStateFile();
				instance.initialize();
			}
		}
		return instance;
	}
	
	private void initialize() {
		String location = "initialize";
		logger.debug(location, jobid, ""+instance);
		jsonImportState();
	}
	
	// OR publication seqNo
	
	private long getNextSequenceNumberState() {
		String location = "getNextSequenceNumberState";
		synchronized(this) {
			long value = seqNoPublication.incrementAndGet();
			jsonExportState();
			logger.debug(location, jobid, ""+seqNoPublication);
			return value;
		}
	}
	
	@Override
	public long getNextPublicationSequenceNumber() {
		return getNextSequenceNumberState();
	}
	
	private void setNextSequenceNumberState(long value) {
		String location = "setNextSequenceNumberState";
		synchronized(this) {
			seqNoPublication.set(value);
			jsonExportState();
			logger.debug(location, jobid, ""+value);
		}
	}
	
	@Override
	public void setNextPublicationSequenceNumber(long seqNo) {
		setNextSequenceNumberState(seqNo);
	}
	
	private void setNextSequenceNumberStateIfGreater(NodeIdentity nodeIdentity, long value) {
		String location = "setNextSequenceNumberStateIfGreater";
		synchronized(this) {
			String node = "?";
			if(nodeIdentity != null) {
				node = nodeIdentity.getName();
			}
			long currentValue = seqNoPublication.get();
			if(value > currentValue) {
				setNextSequenceNumberState(value);
				logger.warn(location, jobid, "agent:"+node+" "+"value:"+value+" "+"or:"+currentValue);
			}
			else {
				logger.trace(location, jobid, "agent:"+node+" "+"value:"+value+" "+"or:"+currentValue);
			}
		}
	}
	
	@Override
	public void setNextPublicationSequenceNumberIfGreater(long value, NodeIdentity nodeIdentity) {
		setNextSequenceNumberStateIfGreater(nodeIdentity, value);
	}
	
	// json
	
	private void jsonCopy(OrchestratorStateJson importedState) {
		String location = "jsonCopy";
		if(importedState != null) {
			setNextSequenceNumberState(importedState.getSequenceNumberState());
		}
		else {
			logger.warn(location, jobid, "no previous state found");
		}
	}
	
	private void jsonImportState() {
		String location = "jsonImportState";
		try {
			jsonImporter();
		}
		catch(Exception e) {
			logger.warn(location, jobid, e);
		}
	}
	
	private void jsonImporter() throws IOException {
		String location = "jsonImporter";
		FileReader fr = null;
		BufferedReader br = null;
		try {
			logger.debug(location, jobid, jsonFileName);
			fr = new FileReader(jsonFileName);
			br = new BufferedReader(fr);
			Type typeOfMap = new TypeToken<OrchestratorStateJson>() { }.getType();
			OrchestratorStateJson importedState = gson.fromJson(br, typeOfMap);
			br.close();
			fr.close();
			jsonCopy(importedState);
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
	
	private void jsonExportState() {
		String location = "jsonExportState";
		try {
			jsonExporter();
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
	}
	
	private void jsonExporter() throws IOException {
		String location = "jsonExporter";
		FileWriter fw = null;
		try {
			logger.debug(location, jobid, jsonFileName);
			OrchestratorStateJson orchestratorStateJson = new OrchestratorStateJson(seqNoPublication.get());
			String json = gson.toJson(orchestratorStateJson);
			fw = new FileWriter(jsonFileName);
			fw.write(json);
			fw.close();
		}
		finally {
			if(fw != null) {
				fw.close();
			}
		}
	}

	// DuccWork seqNo
	
	@Override
	public long getDuccWorkSequenceNumber() {
		String location = "getDuccWorkSequenceNumber";
		long retVal = 0;
		try {
			Properties properties = propertiesLoad();
			String key = constSeqNo;
			String value = properties.getProperty(key);
			retVal = Long.parseLong(value);
			logger.debug(location, jobid, key, value);
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
		return retVal;
	}
	
	@Override
	public void setDuccWorkSequenceNumber(long seqNo) {
		String location = "setDuccWorkSequenceNumber";
		try {
			Properties properties = propertiesLoad();
			String key = constSeqNo;
			String value = ""+seqNo;
			properties.setProperty(key,value);
			propertiesStore(properties);
			logger.debug(location, jobid, key, value);
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
	}
	
	@Override
	public long getNextDuccWorkSequenceNumber() {
		String location = "getNextDuccWorkSequenceNumber";
		long retVal = 0;
		synchronized(this) {
			try {
				long prev = getDuccWorkSequenceNumber();
				long next = prev+1;
				setDuccWorkSequenceNumber(next);
				retVal = next;
				logger.debug(location, jobid, retVal);
			}
			catch(Exception e) {
				logger.error(location, jobid, e);
			}
		}
		return retVal;
	}




	@Override
	public void setDuccWorkSequenceNumberIfGreater(long seqNo) {
		String location = "setDuccWorkSequenceNumberIfGreaterr";
		synchronized(this) {
			try {
				long prev = getDuccWorkSequenceNumber();
				if(seqNo > prev) {
					setDuccWorkSequenceNumber(seqNo);
					logger.debug(location, jobid, seqNo);
				}
			}
			catch(Exception e) {
				logger.error(location, jobid, e);
			}
		}
	}
	
	// properties
	
	private Properties propertiesLoad() {
		String location = "propertiesLoad";
		Properties properties = new Properties();
		try {
			FileInputStream fis = new FileInputStream(propertiesFileName);
			properties.load(fis);
			fis.close();
		}
		catch(Exception e) {
			logger.warn(location, jobid, e);
		}
		return properties;
	}
	
	private void propertiesStore(Properties properties) {
		String location = "propertiesStore";
		try {
			FileOutputStream fos = new FileOutputStream(propertiesFileName);
			properties.store(fos, null);
			fos.close();
		}
		catch(Exception e) {
			logger.warn(location, jobid, e);
		}
	}
	
}
