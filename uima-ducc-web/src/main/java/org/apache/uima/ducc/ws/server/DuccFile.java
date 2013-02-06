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

package org.apache.uima.ducc.ws.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.uima.ducc.common.IDuccEnv;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;

public class DuccFile {
	
	public static Properties getProperties(IDuccWorkJob job) throws IOException {
		String directory = job.getUserLogsDir()+job.getDuccId().getFriendly()+File.separator;
		String name = "job-specification.properties";
		Properties properties = DuccFile.getProperties(directory, name);
		return properties;
	}
	
	public static Properties getProperties(String directory, String name) throws IOException {
		return getProperties(directory+name);
	}
	
	public static Properties getProperties(String path) throws IOException {
		FileInputStream fis = null;
		try {
			File file = new File(path);
			fis = new FileInputStream(file);
			Properties properties = new Properties();
			properties.load(fis);
			fis.close();
			return properties;
		}
		catch(IOException e) {
			if(fis != null) {
				fis.close();
			}
			throw e;
		}
	}
}
