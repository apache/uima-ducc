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
package org.apache.uima.ducc.container.jd.wi.perf.files;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.uima.ducc.common.utils.AlienFile;
import org.apache.uima.ducc.common.utils.IOHelper;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class JobPerformanceIndividualJsonGz implements IPersistenceJobPerformanceIndividual {
	
	public static final String suffix_json_gz = ".json.gz";
	
	private String filename = null;
	
	private Gson gson = new Gson();
	
	public JobPerformanceIndividualJsonGz(String directory, String wiNo) {
		initialize(directory,wiNo);
	}
	
	public void initialize(String directory, String wiNo) {
		String fn = wiNo+suffix_json_gz;
		this.filename = IOHelper.marryDir2File(directory,fn);
	}

	public final String encoding = "UTF-8";

	public void exportData(JobPerformanceIndividualData data) throws IOException {
		BufferedWriter writer = null;
		OutputStreamWriter osr = null;
		GZIPOutputStream gos = null;
		FileOutputStream fos = null;
		File file = null;
		try {
			String json = gson.toJson(data);
			file = new File(filename);
			fos = new FileOutputStream(file);
			gos = new GZIPOutputStream(fos);
			osr = new OutputStreamWriter(gos, encoding);
	        writer = new BufferedWriter(osr);
	        writer.write(json);
		}
		finally {           
	        if(writer != null) {
	        	writer.close();
	        }
	    }
	}
	
	private int getSize() throws IOException {
		int size = 0;
		BufferedReader reader = null;
		InputStreamReader isr = null;
		GZIPInputStream gis = null;
		FileInputStream fis = null;
		File file = null;
		int rc = 0;
		try {
			file = new File(filename);
			fis = new FileInputStream(file);
			gis = new GZIPInputStream(fis);
			isr = new InputStreamReader(gis, encoding);
	        reader = new BufferedReader(isr);
	        while(rc >= 0) {
				rc = reader.read();
		        size++;
			}
		}
		finally {           
	        if(reader != null) {
	        	reader.close();
	        }
	    }
		return size-1;
	}
	

	public JobPerformanceIndividualData importData() throws IOException, ClassNotFoundException {
		JobPerformanceIndividualData data = new JobPerformanceIndividualData();
		BufferedReader reader = null;
		InputStreamReader isr = null;
		GZIPInputStream gis = null;
		FileInputStream fis = null;
		File file = null;
		char[] cbuf = new char[getSize()];
		try {
			file = new File(filename);
			fis = new FileInputStream(file);
			gis = new GZIPInputStream(fis);
			isr = new InputStreamReader(gis, encoding);
	        reader = new BufferedReader(isr);
	        reader.read(cbuf);
	        String json = new String(cbuf);
	        Type typeOfMap = new TypeToken<JobPerformanceIndividualData>() { }.getType();
			data = gson.fromJson(json, typeOfMap);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		finally {           
	        if(reader != null) {
	        	reader.close();
	        }
	    }
		return data;
	}
	
	public JobPerformanceIndividualData importData(String userid) throws IOException, ClassNotFoundException {
		JobPerformanceIndividualData data = new JobPerformanceIndividualData();
		if(userid == null) {
			data = importData();
		}
		else {
			try {
				AlienFile alienFile = new AlienFile(userid, filename);
				String json = alienFile.getString();
				Type typeOfMap = new TypeToken<JobPerformanceIndividualData>() { }.getType();
				data = gson.fromJson(json, typeOfMap);
			}
			catch(Throwable t) {
				// TODO
			}
		}
		return data;
	}
	
}
