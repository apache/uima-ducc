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
package org.apache.uima.ducc.ws.xd;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.StringReader;

import org.apache.uima.ducc.common.utils.AlienFile;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.ws.log.WsLog;

public class ExperimentsRegistryUtilities {
	
  // NOTE - this variable used to hold the class name before WsLog was simplified
	private static DuccLogger cName = DuccLogger.getLogger(ExperimentsRegistryUtilities.class);
	
	private static String stateFileName = "Experiment.state";
	
	public static String upOne(String directory) {
		String retVal = directory;
		if(directory != null) {
		  retVal = new File(directory).getParent();
		}
		return retVal;
	}
	
	public static String getStateFilePath(String directory) {
		String retVal = new File(directory, stateFileName).getAbsolutePath();
		return retVal;
	}
	
	private static void closer(Closeable object) {
		String mName = "closer";
		try {
			object.close();
		}
		catch(Exception e) {
			WsLog.debug(cName, mName, e);
		}
	}
	
	public static long getFileDate(IExperiment experiment) {
		String mName = "getFileDate";
		WsLog.enter(cName, mName);
		String user = experiment.getUser();
		String filename = getStateFilePath(experiment.getDirectory());
		long retVal = getFileDate(user, filename);
		return retVal;
	}
	
	public static long getFileDate(String user, String filename) {
		String mName = "getFileDate";
		WsLog.enter(cName, mName);
		long retVal = getDomesticFileDate(user, filename);
		if(retVal == 0) {
			retVal = getAlienFileDate(user, filename);
		}
		WsLog.exit(cName, mName);
		return retVal;
	}
	
	private static long getAlienFileDate(String user, String filename) {
		String mName = "getAlienFileDate";
		WsLog.enter(cName, mName);
		long retVal = 0;
		AlienFile alienFile = new AlienFile(user, filename);
		// NOTE - should not need the "--" ... or this could be moved to AlienFile
        String[] lines = alienFile.getResult(false, "--", "/bin/ls", "-l", "--time-style=+%s", filename );
        // Should have 1 line with secs-since-epoch in 6th token
        if (lines.length == 1) {
            String[] toks = lines[0].split("\\s+");
            if (toks.length >= 6) {
                retVal = Long.valueOf(toks[5]) * 1000;
            }
        }
		WsLog.exit(cName, mName);
		return retVal;
	}
	
	private static long getDomesticFileDate(String user, String filename) {
		String mName = "getDomesticFileDate";
		WsLog.enter(cName, mName);
		long retVal =0;
		try {
			File file = new File(filename);
			retVal = file.lastModified();
		}
		catch(Exception e) {
			WsLog.trace(cName, mName, e);
		}
		WsLog.exit(cName, mName);
		return retVal;
	}
	
	/*
	 * Returns null if file is missing or can't be read
	 */
	public static String getFileContents(String user, String filename) {
		String mName = "getFileContents";
		WsLog.enter(cName, mName);
		boolean canRead = ((new File(filename)).canRead());
		String retVal = canRead ? getDomesticFileContents(user, filename) : getAlienFileContents(user, filename);
		WsLog.exit(cName, mName);
		return retVal;
	}
	
	private static String getAlienFileContents(String user, String filename) {
		String mName = "getAlienFileContents";
		WsLog.enter(cName, mName);
		String retVal = null;
		try {
			AlienFile alienFile = new AlienFile(user, filename);
			retVal = alienFile.getString();
		}
		catch(Throwable t) {
			WsLog.trace(cName, mName, t);
		}
		
		WsLog.exit(cName, mName);
		return retVal;
	}
	
	private static String getDomesticFileContents(String user, String filename) {
		String mName = "getDomesticFileContents";
		WsLog.enter(cName, mName);
		String retVal = null;
		FileReader fr = null;
		BufferedReader br = null;
		StringReader sr = null;
		try {
			fr = new FileReader(filename);
			br = new BufferedReader(fr);
			StringBuffer sb = new StringBuffer();
			String line = br.readLine();
			while(line != null) {
				sb.append(line);
				line = br.readLine();
			}
			retVal = sb.toString();
		}
		catch(Exception e) {
			WsLog.debug(cName, mName, e);
		}
		finally {
			if(br != null) {
				closer(br);
			}
			if(fr != null) {
				closer(fr);
			}
			if(sr != null) {
				closer(sr);
			}
		}
		WsLog.exit(cName, mName);
		return retVal;
	}

}
