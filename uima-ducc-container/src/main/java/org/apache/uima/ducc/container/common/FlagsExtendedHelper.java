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
package org.apache.uima.ducc.container.common;

import java.io.File;

import org.apache.uima.ducc.common.container.FlagsHelper;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;

public class FlagsExtendedHelper extends FlagsHelper {

	private static Logger logger = Logger.getLogger(FlagsExtendedHelper.class, IComponent.Id.JD.name());
	
	private static FlagsExtendedHelper instance = new FlagsExtendedHelper();
	
	public static FlagsExtendedHelper getInstance() {
		return instance;
	}
	
	public String getUserClasspath() {
		String location = "getUserClasspath";
		String retVal = super.getUserClasspath();
		if(retVal != null) {
			String[] list = retVal.split(":");
			if(list != null) {
				if(list.length > 0) {
					logger.debug(location, ILogger.null_id, FlagsHelper.Name.UserClasspath.pname());
					int index = 0;
					for(String item : list) {
						String text = "["+index+"]"+" "+item;
						logger.debug(location, ILogger.null_id, text);
						index++;
					}
				}
			}
		}
		return retVal;
	}
	
	public String getLogDirectory() {
		StringBuffer sb = new StringBuffer();
		String jobDirectory = getJobDirectory();
		if(jobDirectory != null) {
			sb.append(jobDirectory);
			if(!jobDirectory.endsWith(File.separator)) {
				sb.append(File.separator);
			}
		}
		String jobId = getJobId();
		if(jobId != null) {
			sb.append(jobId);
			sb.append(File.separator);
		}
		return sb.toString();
	}
}
