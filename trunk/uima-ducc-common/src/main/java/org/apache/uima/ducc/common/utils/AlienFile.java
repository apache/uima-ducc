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
package org.apache.uima.ducc.common.utils;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;

public class AlienFile extends AlienAbstract {
	
	private static DuccLogger duccLogger = DuccLoggerComponents.getWsLogger(AlienFile.class.getName());
	
	private static String ducc_ling = 
			Utils.resolvePlaceholderIfExists(
					System.getProperty("ducc.agent.launcher.ducc_spawn_path"),System.getProperties());
	
	private String command_cat = "/bin/cat";
	private String file_name;
	
	private File devNull = new File("/dev/null");

	private int exitValue;
	
	protected void set_command_cat(String value) {
		command_cat = value;
	}
	
	protected String get_command_cat() {
		return command_cat;
	}
	
	protected void set_file_name(String value) {
		file_name = value;
	}
	
	protected String get_file_name() {
		return file_name;
	}
	
	public AlienFile(String user, String file_name) {
		set_user(user);
		set_file_name(file_name);
	}
	
	private String[] getCommand() {
		String[] command_ducc_ling_yes = { ducc_ling, q_parameter, u_parameter, user, command_cat, file_name};
		String[] command_ducc_ling_no  = { command_cat, file_name};
		String[] command = command_ducc_ling_yes;
		if(ducc_ling == null) {
			command = command_ducc_ling_no;
		}
		return command;
	}
	
	/*
	 * Read file contents into a string
	 * Returns null if file is missing or cannot be read
	 */
	private String reader() throws Exception {
	    String location = "reader";
		BufferedReader br = null;
		InputStreamReader isr = null;
		Process p = null;
		StringBuilder sb = new StringBuilder();
		String[] command = getCommand();
		echo(command);
		ProcessBuilder pb = new ProcessBuilder( command );
		pb = pb.redirectError(devNull);
		try{
			p = pb.start();
			InputStream pOut = p.getInputStream();
			if (FileHelper.isGzFileType(file_name)) {
			    GZIPInputStream gis = new GZIPInputStream(pOut);
			    isr = new InputStreamReader(gis, encoding);
			}
			else {
				isr = new InputStreamReader(pOut);
			}
	        br = new BufferedReader(isr);

	        String line;
	        while ((line = br.readLine()) != null) {
	        	sb.append(line);
	        	sb.append("\n");
	        }
	        int rc = p.waitFor();   // 0 => success
	        return rc==0 ? sb.toString() : null;
		}
		catch(Exception e) {
		    if (p != null) {
		        int rc = p.waitFor();
		        if (rc != 0) {
		            return null;                // cat failed, presumably permission denied
		        }
		    }
		    // Process start failed or some exception while unzipping?
		    duccLogger.error(location, duccId, e);
			throw e;
		}
		finally {
			closer(br);
			closer(isr);
		}
	}
	
	/**
	 * return null if file is unreadable or does not exist
	 */
	public String getString() throws Exception {
		String data = reader();
		if(data != null) {
			data = data.trim();
		}
		return data;
	}
	
	public InputStreamReader getInputStreamReader() throws Exception {
		InputStreamReader isr = null;
		try {
			String[] command = getCommand();
			echo(command);
			ProcessBuilder pb = new ProcessBuilder( command );
			pb = pb.redirectError(devNull);
			Process p = pb.start();
			InputStream pOut = p.getInputStream();
			if(FileHelper.isGzFileType(file_name)) {
				GZIPInputStream gis = new GZIPInputStream(pOut);
				isr = new InputStreamReader(gis, encoding);
			}
			else {
				isr = new InputStreamReader(pOut);
			}
		} 
		catch(Exception e) {
			throw e;
		}
		return isr;
	}

	public DataInputStream getDataInputStream() throws Exception {
		DataInputStream dis = null;
		try {
			String[] command = getCommand();
			echo(command);
			ProcessBuilder pb = new ProcessBuilder( command );
			pb = pb.redirectError(devNull);
			Process p = pb.start();
			InputStream pOut = p.getInputStream();
			dis= new DataInputStream(pOut);
		} 
		catch(Exception e) {
			throw e;
		}
		return dis;
	}
	
	/*
	 * Execute a command and return an array of result lines
	 * (not used but may be useful e.g. in getting the date of a file)
	 */
	public String[] getResult(boolean includeStderr, String... command) {
		String location = "getLines";
		ArrayList<String> lines = new ArrayList<String>();
		try {
			String[] prefix = { ducc_ling, "-q", "-u", user};
			ArrayList<String> fullCmd = new ArrayList<String>();
			fullCmd.addAll(Arrays.asList(prefix));
			fullCmd.addAll(Arrays.asList(command));
			ProcessBuilder pb = new ProcessBuilder( fullCmd );
			if (includeStderr) {
				pb.redirectErrorStream(true);
			} else {
				pb.redirectError(new File("/dev/null"));
			}
			Process process = pb.start();
			InputStream is = process.getInputStream();
	        InputStreamReader isr = new InputStreamReader(is);
	        BufferedReader br = new BufferedReader(isr);
	        String line;
	        while ((line = br.readLine()) != null) {
	           lines.add(line);
	        }
	        exitValue = process.waitFor();
		}
		catch(Exception e) {
			exitValue = -1;
			duccLogger.error(location, duccId, e);
		}
		return lines.toArray(new String[lines.size()]);
	}
	
	public int getRc() {
		return exitValue;
	}
	
	public static void main(String[] args) throws Exception {
		AlienFile alienFile;
		String arg_user = args[0];
		String arg_file = args[1];
		alienFile = new AlienFile(arg_user, arg_file);
		String data = alienFile.getString();
		System.out.println(data);
	}
}
