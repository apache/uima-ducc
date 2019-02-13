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
import java.io.FileReader;
import java.lang.reflect.Type;
import java.util.Properties;

import org.apache.uima.ducc.common.db.DbHelper;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.orchestrator.OrchestratorCommonArea;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class OrchestratorStateDbConversion {

	private static DuccLogger logger = DuccLogger.getLogger(OrchestratorStateDbConversion.class);
	private static DuccId jobid = null;
	
	private static OrchestratorCommonArea orchestratorCommonArea = OrchestratorCommonArea.getInstance();
	private static String jsonFileName = orchestratorCommonArea.getStateDirectory()+File.separator+"orchestrator-state.json";
	private static String propertiesFileName = orchestratorCommonArea.getStateDirectory()+File.separator+"orchestrator.properties";
	
	private static final String constSeqNo = "seqno";
	
	private static Gson gson = new Gson();
	
	private static IOrchestratorState orchestratorStateDb = OrchestratorStateDb.getInstance();
	
	public static void convert() {
		if(isDbEnabled()) {
			jsonConvert();
			propertiesConvert();
		}
	}
	
	private static void jsonConvert() {
		String location = "jsonConvert";
		try {
			if(jsonFileExists()) {
				long seqNo = getPublishSeqNoFromFile();
				if(seqNo > 0) {
					setPublishSeqNoInDb(seqNo);
				}
				jsonEraseFile();
			}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
	}
	
	private static void propertiesConvert() {
		String location = "propertiesConvert";
		try {
			if(propertiesFileExists()) {
				long seqNo = getDuccWorkSeqNoFromFile();
				if(seqNo > 0) {
					setDuccWorkSeqNoInDb(seqNo);
				}
				propertiesEraseFile();
			}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
	}

	private static boolean isDbDisabled() {
		String location = "isDbDisabled";
    	boolean retVal = DbHelper.isDbDisabled();
    	logger.info(location, jobid, retVal);
    	return retVal;
    }
	
	private static boolean isDbEnabled() {
		return !isDbDisabled();
	}
	
	// json
	
	private static boolean jsonFileExists() {
		String location = "jsonFileExists";
		File file = new File(jsonFileName);
		boolean retVal = file.exists();
		logger.info(location, jobid, jsonFileName, retVal);
		return retVal;
	}
	
	private static boolean jsonEraseFile() {
		String location = "jsonEraseFile";
		File file = new File(jsonFileName);
		boolean retVal = file.delete();
		logger.info(location, jobid, jsonFileName, retVal);
		return retVal;
	}
	
	private static long getPublishSeqNoFromFile() throws Exception {
		String location = "getPublishSeqNoFromFile";
		long retVal = -1;
		FileReader fr = null;
		BufferedReader br = null;
		try {
			fr = new FileReader(jsonFileName);
			br = new BufferedReader(fr);
			Type typeOfMap = new TypeToken<OrchestratorStateJson>() { }.getType();
			OrchestratorStateJson importedState = gson.fromJson(br, typeOfMap);
			retVal = importedState.getSequenceNumberState();
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
		logger.info(location, jobid, retVal);
		return retVal;
	}
	
	private static void setPublishSeqNoInDb(long value) {
		String location = "setPublishSeqNoInDb";
		orchestratorStateDb.setNextPublicationSequenceNumber(value);
		logger.info(location, jobid, value);
	}
	
	// properties
	
	private static boolean propertiesFileExists() {
		String location = "propertiesFileExists";
		File file = new File(propertiesFileName);
		boolean retVal = file.exists();
		logger.info(location, jobid, propertiesFileName, retVal);
		return retVal;
	}
	
	private static boolean propertiesEraseFile() {
		String location = "propertiesEraseFile";
		File file = new File(propertiesFileName);
		boolean retVal = file.delete();
		logger.info(location, jobid, propertiesFileName, retVal);
		return retVal;
	}
	
	private static long getDuccWorkSeqNoFromFile() throws Exception {
		String location = "getDuccWorkSeqNoFromFile";
		long retVal = 0;
		try {
			Properties properties = new Properties();
			FileInputStream fis = new FileInputStream(propertiesFileName);
			properties.load(fis);
			fis.close();
			String key = constSeqNo;
			String value = properties.getProperty(key);
			retVal = Long.parseLong(value);
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
		logger.info(location, jobid, retVal);
		return retVal;
	}
	
	private static void setDuccWorkSeqNoInDb(long value) {
		String location = "setDuccWorkSeqNoInDb";
		orchestratorStateDb.setDuccWorkSequenceNumber(value);
		logger.info(location, jobid, value);
	}
}
