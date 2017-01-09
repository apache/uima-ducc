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

public class JobPerformanceSummaryJsonGz implements IPersistenceJobPerformanceSummary {
	
	public static final String job_performance_summary_json_gz = "job-performance-summary.json.gz";
	
	private String filename = null;
	
	private Gson gson = new Gson();
	
	public JobPerformanceSummaryJsonGz(String directory) {
		initialize(directory);
	}
	
	public void initialize(String directory) {
		this.filename = IOHelper.marryDir2File(directory,job_performance_summary_json_gz);
	}

	public final String encoding = "UTF-8";

	public void exportData(JobPerformanceSummaryData data) throws IOException {
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
	

	public JobPerformanceSummaryData importData() throws IOException, ClassNotFoundException {
		JobPerformanceSummaryData data = new JobPerformanceSummaryData();
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
	        Type typeOfMap = new TypeToken<JobPerformanceSummaryData>() { }.getType();
			data = gson.fromJson(json, typeOfMap);
		}
		catch(Exception e) {
			//e.printStackTrace();
			System.out.println("importData: error importing perf data "+e);      // Should use logger?
		}
		finally {           
	        if(reader != null) {
	        	reader.close();
	        }
	    }
		return data;
	}
	
	public JobPerformanceSummaryData importData(String userid) throws IOException, ClassNotFoundException {
	    String json;
	    JobPerformanceSummaryData data = new JobPerformanceSummaryData();
		try {
		    File f = new File(filename);
		    if (f.canRead()) {
		        json = readGzipFile(filename);
		    } else {
				AlienFile alienFile = new AlienFile(userid, filename);
				json = alienFile.getString();
		    }
		    Type typeOfMap = new TypeToken<JobPerformanceSummaryData>() { }.getType();
		    data = gson.fromJson(json, typeOfMap);
		}
		catch(Throwable t) {
		    System.out.println("importData: error importing perf data for "+userid+" "+t);   // Should use logger?
		    return null;
		}
		return data;
	}
	
	/*
	 * Unzip a text file into a string
	 * (Could be public?  or made part of AlienFile that would skip ducc_ling if file is readable?)
	 */
    private String readGzipFile(String filename) throws IOException {
        BufferedReader br = null;
        try {
            // Wrap the file-stream in a GZIP-stream in a reader in a buffered-reader
            br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(filename)), encoding));
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
                sb.append("\n");
            }
            return sb.toString();
        } finally {
            if (br != null) {
                br.close();
            }
        }
    }
}
