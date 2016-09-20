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
package org.apache.uima.ducc.ws.utils.commands;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;

/**
 * Issue /usr/bin/id <userid> and return results
 */
public class CmdId {
	
	private static String command = "/usr/bin/id";
	
	private DuccLogger logger = null;
	private DuccId jobid = null;

	// constructors
	
	public CmdId() {
		init(true);			// use logger, by default
	}
	
	public CmdId(boolean useLogger) {
		init(useLogger);	// use logger or not, by choice
	}
	
	// set use of logger or console
	
	private void init(boolean useLogger) {
		if(useLogger) {
			logger = DuccLoggerComponents.getWsLogger(CmdId.class.getName());
		}
	}
	
	// use logger or console?
	
	private boolean isUseLogger() {
		return logger != null;
	}
	
	// run id command and return result
	
	public String runnit(String[] args) {
		String location = "runCommand";
		String retVal = null;
		try {
			List<String> commandList = new ArrayList<String>();
			commandList.add(command);
			String[] commandArray = commandList.toArray(new String[0]);
			ProcessBuilder pb = new ProcessBuilder( commandArray );
			Process p = pb.start();
			//p.waitFor();
			InputStream pOut = p.getInputStream();
			InputStreamReader isr = new InputStreamReader(pOut);
			BufferedReader br = new BufferedReader(isr);
	        String line;
	        StringBuffer sb = new StringBuffer();
	        while ((line = br.readLine()) != null) {
	        	sb.append(line);
	        	debug(location, line);
	        }
	        retVal = sb.toString();
		}
		catch(Exception e) {
			error(location, e);
		}
		return retVal;
	}
	
	// log result to logger or console when debugging
	
	private void debug(String location, String s) {
		if(isUseLogger()) {
			logger.debug(location, jobid, s);
		}
		else {
			System.out.println(s);
		}
	}
	
	// log exception to logger or console
	
	private void error(String location, Exception e) {
		if(isUseLogger()) {
			logger.error(location, jobid, e);
		}
		else {
			e.printStackTrace();
		}
	}
	
	// process command line invocation
	
	public static void main(String[] args) {
		CmdId id = new CmdId(false);
		id.runnit(args);
	}

}
