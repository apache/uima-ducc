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
package org.apache.uima.ducc.ws.utils.alien;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.uima.ducc.common.utils.AlienAbstract;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;

public class AlienDirectory extends AlienAbstract {
	
	private static DuccLogger logger = DuccLoggerComponents.getWsLogger(AlienDirectory.class.getName());
	private static DuccId jobid = null;
	
	private static String newline = "\n";
	private static String whitespace = "\\s+";
	
	private static String dashdash = "--";
	
	private static String command_ls = "/bin/ls";
	private static String command_ls_flag1 = "-atl";
	private static String command_ls_flag1R = "-atlR";
	private static String command_ls_flag2 = "--time-style=long-iso";
	
	private static int indexPermissions = 0;
	private static int indexUser = 2;
	private static int indexGroup = 3;
	private static int indexLength = 4;
	private static int indexDate = 5;
	private static int indexTime = 6;
	private static int indexName = 7;
	
	protected boolean recursive = false;
	private String directory_name;
	
	protected void setRecursive(boolean value) {
		recursive = value;
	}
	
	protected boolean isRecursive() {
		return recursive;
	}
	
	protected String get_command_ls() {
		return command_ls;
	}
	
	protected String get_command_ls_flag1() {
		String retVal = command_ls_flag1;
		if(isRecursive()) {
			retVal = command_ls_flag1R;
		}
		return retVal;
	}
	
	protected String get_command_ls_flag2() {
		return command_ls_flag2;
	}
	
	protected void set_directory_name(String value) {
		directory_name = value;
	}
	
	protected String get_directory_name() {
		return directory_name;
	}
	
	public AlienDirectory(EffectiveUser user, String directory_name, String ducc_ling) {
		set_user(user.get());
		set_directory_name(directory_name);
		set_ducc_ling(ducc_ling);
	}
	
	public AlienDirectory(EffectiveUser user, String directory_name, String ducc_ling, boolean recursive) {
		set_user(user.get());
		set_directory_name(directory_name);
		set_ducc_ling(ducc_ling);
		setRecursive(recursive);
	}
	
	private String[] getCommand() {
		String[] command_ducc_ling_yes = { ducc_ling, q_parameter, u_parameter, user, dashdash, get_command_ls(), get_command_ls_flag1(), get_command_ls_flag2(), get_directory_name() };
		String[] command_ducc_ling_no  = { get_command_ls(), get_command_ls_flag1(), get_command_ls_flag2(), get_directory_name() };
		String[] command = command_ducc_ling_yes;
		if(ducc_ling == null) {
			command = command_ducc_ling_no;
		}
		return command;
	}

	private String reader() throws Exception {
		String data = null;
		BufferedReader br = null;
		InputStreamReader isr = null;
		try {
			String[] command = getCommand();
			echo(command);
			ProcessBuilder pb = new ProcessBuilder( command );
			Process p = pb.start();
			//p.waitFor();
			InputStream pOut = p.getInputStream();
			isr = new InputStreamReader(pOut);
	        br = new BufferedReader(isr);
	        StringBuilder sb = new StringBuilder();
	        String line;
	        boolean first = true;
	        while ((line = br.readLine()) != null) {
	        	if(first) {
	        		first = false;
	        	}
	        	else {
	        		sb.append(line);
	        		sb.append("\n");
	        	}
	        }
	        data = sb.toString();
			return data;
		}
		catch(Exception e) {
			e.printStackTrace();
			throw e;
		}
		finally {
			closer(br);
			closer(isr);
		}
	}
	
	private String getString() throws Exception {
		String methodName = "getString";
		String data = reader();
		logger.trace(methodName, duccId, "\n"+data);
		return data;
	}

	private void put(Map<String, FileInfo> map, String key, FileInfo value) {
		String location = "put";
		map.put(key, value);
		logger.trace(location, jobid, key);
	}
	
	private TreeMap<String, FileInfo> transform(String[] lines) {
		String location = "transform";
		TreeMap<String, FileInfo> map = new TreeMap<String, FileInfo>();
		if(lines != null) {
			String type = "p";
			String parent = get_directory_name();
			logger.debug(location, jobid, type+" "+0+" "+parent);
			for(String line : lines) {
				String[] elements = line.split(whitespace);
				type = "?";
				int count = elements.length;
				if(count == indexName+1) {
					if(elements[indexPermissions].startsWith("d")) {
						type = "d";
						logger.trace(location, jobid, type+" "+count+" "+line);
					}
					else {
						type = "f";
						logger.debug(location, jobid, type+" "+count+" "+line);
						String reldir = parent.replace(get_directory_name(), "");
						if(reldir.startsWith(File.separator)) {
							reldir = reldir.replace(File.separator, ".");
						}
						String permissions = elements[indexPermissions];
						String user = elements[indexUser];
						String group = elements[indexGroup];
						long length = Long.parseLong(elements[indexLength]);
						String date = elements[indexDate];
						String time = elements[indexTime];
						String name = (parent+File.separator+elements[indexName]).replace(File.separator+File.separator, File.separator);
						FileInfo fi = new FileInfo(reldir, permissions, user, group, length, date, time, name);
						put(map, name, fi);
					}
				}
				else if(count == indexPermissions+1) {
					if(elements[indexPermissions].endsWith(":")) {
						type = "p";
						logger.debug(location, jobid, type+" "+count+" "+line);
						parent = elements[indexPermissions].replace(":", "");
					}
					else {
						type = "o";
						logger.trace(location, jobid, type+" "+count+" "+line);
					}
				}
				else {
					logger.trace(location, jobid, type+" "+count+" "+line);
				}
			}
		}
		return map;
	}
	
	public TreeMap<String, FileInfo> getMap() {
		String location = "getMap";
		long start = System.currentTimeMillis();
		TreeMap<String, FileInfo> map = new TreeMap<String, FileInfo>();
		String result = null;
		try {
			result = getString();
		}
		catch(Exception e) {
		}
		if(result != null) {
			String[] lines = result.split(newline);
			map = transform(lines);
		}
		long end = System.currentTimeMillis();
		logger.debug(location, jobid, "elapsed="+(end-start));
		return map;
	}
	
	protected InputStreamReader getInputStreamReader() throws Exception {
		InputStreamReader isr = null;
		try {
			String[] command = getCommand();
			echo(command);
			ProcessBuilder pb = new ProcessBuilder( command );
			Process p = pb.start();
			//p.waitFor();
			InputStream pOut = p.getInputStream();
			isr = new InputStreamReader(pOut);
		} 
		catch(Exception e) {
			throw e;
		}
		return isr;
	}

	protected DataInputStream getDataInputStream() throws Exception {
		DataInputStream dis = null;
		try {
			String[] command = getCommand();
			echo(command);
			ProcessBuilder pb = new ProcessBuilder( command );
			Process p = pb.start();
			//p.waitFor();
			InputStream pOut = p.getInputStream();
			dis= new DataInputStream(pOut);
		} 
		catch(Exception e) {
			throw e;
		}
		return dis;
	}
	
	public static void main(String[] args) throws Exception {
		String location = "main";
		logger.trace(location, jobid, "start");
		AlienDirectory alienDirectory;
		String arg_user = args[0];
		logger.trace(location, jobid, "user: "+arg_user);
		String arg_directory = args[1];
		logger.trace(location, jobid, "directory: "+arg_directory);
		EffectiveUser eu = EffectiveUser.create(arg_user);
		if(args.length < 3) {
			alienDirectory = new AlienDirectory(eu, arg_directory, null);
		}
		else {
			String arg_ducc_ling = args[2];
			alienDirectory = new AlienDirectory(eu, arg_directory, arg_ducc_ling);
		}
		Map<String, FileInfo> map = alienDirectory.getMap();
		for(Entry<String, FileInfo> entry : map.entrySet()) {
			FileInfo fi = entry.getValue();
			String text = ""
						+fi.getPermissions()
						+" "
						+fi.getName()
						+" "
						+fi.getUser()
						+" "
						+fi.getGroup()
						+" "
						+fi.getLength()
					    ;
			logger.info(location, jobid, text);
		}
		logger.trace(location, jobid, "end");
	}
}
