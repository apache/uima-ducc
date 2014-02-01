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
import java.io.InputStream;
import java.io.InputStreamReader;

public class AlienDirectory extends AlienAbstract {
	
	private static DuccLogger duccLogger = DuccLoggerComponents.getWsLogger(AlienFile.class.getName());
	
	private String command_ls = "/bin/ls";
	private String command_ls_flags = "-atl";
	private String directory_name;
	
	protected void set_command_ls(String value) {
		command_ls = value;
	}
	
	protected String get_command_ls() {
		return command_ls;
	}
	
	protected void set_command_ls_flags(String value) {
		command_ls_flags = value;
	}
	
	protected String get_command_ls_flags() {
		return command_ls_flags;
	}
	
	protected void set_directory_name(String value) {
		directory_name = value;
	}
	
	protected String get_directory_name() {
		return directory_name;
	}
	
	public AlienDirectory(String user, String directory_name, String ducc_ling) {
		set_user(user);
		set_directory_name(directory_name);
		set_ducc_ling(ducc_ling);
	}
	
	private String[] getCommand() {
		String[] command_ducc_ling_yes = { ducc_ling, q_parameter, u_parameter, user, command_ls, command_ls_flags, directory_name};
		String[] command_ducc_ling_no  = { command_ls, command_ls_flags, directory_name};
		String[] command = command_ducc_ling_yes;
		if(ducc_ling == null) {
			command = command_ducc_ling_no;
		}
		return command;
	}

	private String reader() throws Throwable {
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
		catch(Throwable t) {
			t.printStackTrace();
			throw t;
		}
		finally {
			closer(br);
			closer(isr);
		}
	}
	
	public String getString() throws Throwable {
		String methodName = "getString";
		String data = reader();
		duccLogger.debug(methodName, duccId, data);
		return data;
	}
	
	public InputStreamReader getInputStreamReader() throws Throwable {
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
		catch(Throwable t) {
			throw t;
		}
		return isr;
	}

	public DataInputStream getDataInputStream() throws Throwable {
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
		catch(Throwable t) {
			throw t;
		}
		return dis;
	}
	
	public static void main(String[] args) throws Throwable {
		AlienDirectory alienDirectory;
		String arg_user = args[0];
		String arg_directory = args[1];
		if(args.length < 3) {
			alienDirectory = new AlienDirectory(arg_user, arg_directory, null);
		}
		else {
			String arg_ducc_ling = args[2];
			alienDirectory = new AlienDirectory(arg_user, arg_directory, arg_ducc_ling);
		}
		String data = alienDirectory.getString();
		System.out.println(data);
	}
}
